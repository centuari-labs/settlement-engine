# CLAUDE.md — Settlement Engine

## Stack

Node.js · TypeScript (strict) · Viem · ioredis (Streams + Consumer Groups) · PostgreSQL (raw pg) · Zod · Jest 29 · pnpm

## Commands

```bash
pnpm run dev                # ts-node-dev --respawn --transpile-only
pnpm run build              # tsc -p tsconfig.json
pnpm run start              # node dist/index.js
pnpm run test               # jest (all tests, 4GB heap)
pnpm run test:unit          # unit only (ignores integration)
pnpm run test:integration   # integration only
pnpm run test:coverage      # with coverage report
```

## Architecture

```
src/
├── index.ts                          # Entry point: startup, shutdown, consumer group init
├── config.ts                         # Zod-validated config (17 env vars with defaults)
├── logger.ts                         # Structured pino logger
├── schemas/
│   └── match.ts                      # Match Zod schema + Redis stream constants
├── redis/
│   ├── client.ts                     # Singleton Redis client
│   └── settlementMatchConsumer.ts    # Stream reading, entry parsing, pending recovery
├── settlement/
│   ├── abi.ts                        # Re-exports SETTLEMENT_CONTRACT_ABI from src/abi/Settlement.json (synced)
│   ├── batchAccumulator.ts           # Hybrid batching (size + time triggers)
│   ├── batchProcessor.ts             # Main polling loop with backoff
│   ├── processBatch.ts               # Orchestrates: filter → settle → persist → ack
│   ├── smartContract.ts              # Viem clients, multicall, error mapping, event parsing
│   ├── nonceManager.ts               # Transaction nonce management
│   ├── database/
│   │   ├── index.ts                       # Re-exports
│   │   ├── connection.ts                  # Pool, transactions, retry, error classification
│   │   ├── apply-settlement.ts            # applyOnChainEffect-stamped position writes + collateral-flag cleanup + lock-release writeback
│   │   ├── lock-release.ts                # matches.settlement_status writeback + user_balance.in_orders decrement (Phase 1A)
│   │   ├── pending-collateral-flags.ts    # Eager DELETE from receipt CollateralFlagSet logs
│   │   └── order-failure.ts               # Batch-failure handling
│   ├── helpers.ts                    # UUID ↔ bytes32 conversion, hash-based IDs, backoff utility
│   └── eventAbis.ts                  # Contract event definitions
└── tests/
    ├── setup.ts                     # Global test setup
    └── helpers/                     # Test config, fixtures, Redis/DB/contract mocks
```

### Processing Pipeline

```
Redis Stream (settlement:matches)
  → Consumer Group read (XREADGROUP)
  → Zod validate
  → BatchAccumulator (size OR time trigger)
  → Filter already-settled (multicall isSettled check)
  → settleMatches() on-chain tx
  → Parse events (BondTokenCreated, LendPositionCreated, BorrowPositionCreated)
  → Persist to PostgreSQL (batch insert with FK ordering)
  → ACK + XDEL Redis entries
```

### Settlement Writeback / Lock Release (Phase 1A — order-lock lifecycle)

After `Settlement.settle()` confirms on-chain, [`applySettlementResult`](src/settlement/database/apply-settlement.ts) runs three writebacks against indexer-v3's Postgres in this order:

1. Per-event `applyOnChainEffect` for lend/borrow position rows (idempotency via `applied_by_tx_hash` / `applied_by_log_index` stamps).
2. `clearPendingCollateralFlagsFromReceipt` — DELETEs `pending_collateral_flags` rows matching the receipt's `CollateralFlagSet` events (P3 collateral-flag cleanup).
3. `writebackSettledMatches` ([lock-release.ts](src/settlement/database/lock-release.ts)) — for each settled match, a fresh `withTransaction` flips `matches.settlement_status PENDING → SETTLED` and decrements `user_balance.in_orders` (keyed by BYTEA `user_address` + loan-token `asset`) for both lender and borrower by the exact decomposition the matching-engine db-writer added at match time:

   - lender: `matchedAmount + lenderSettlementFeeAmount + lenderTradeFee`
   - borrower: `borrowerSettlementFeeAmount + borrowerTradeFee`
   - trade-fee split: `borrowerTradeFee = takerFeeAmount` if `borrowerIsTaker`, else `makerFeeAmount`. Lender pays the opposite.

**Idempotency.** The conditional `UPDATE matches SET settlement_status = 'SETTLED' … WHERE id = ? AND settlement_status = 'PENDING'` returns 0 rows on retry, so the per-side `in_orders` decrements only fire on the first transition. `GREATEST(in_orders − decrement, 0)` is a belt-and-suspenders guard against any underflow from manual SQL repair.

**Per-match transactions, not the `applyOnChainEffect` closure.** The writeback runs **after** the per-event `applyOnChainEffect` calls and in its **own** per-match `withTransaction`, not inside an `applyOnChainEffect` mutation closure. Partial failure on one match's writeback doesn't block writeback for the rest of the batch — the on-chain settlement is already final at this point, so retries are safe; stuck `PENDING` rows are picked up by the separate reconciliation job (see followups).

**Match-time counterpart** lives in [matching-engine/src/services/db/postgres-db-client.ts:251-271](../matching-engine/src/services/db/postgres-db-client.ts). The decrement decomposition mirrors that `user_balance.in_orders` increment exactly; any drift between the two sites breaks lock-release accounting.

For the planned reconciliation job that sweeps stuck `PENDING` rows, see [../dev-docs/architecture-html/launches/hub-only.html](../dev-docs/architecture-html/launches/hub-only.html) Track C2 (deep reference in [archive/order-lock-lifecycle-followups.md](../smart-contract-revamp/docs/archive/order-lock-lifecycle-followups.md)).

### Batch Strategy

Two triggers — whichever fires first:
1. **Size**: queue reaches `SETTLEMENT_BATCH_SIZE` (default: 10)
2. **Time**: `SETTLEMENT_BATCH_INTERVAL_MS` elapsed with >=1 match (default: 5000ms)

Backpressure: `maxCapacity = batchSize * 5`. Deduplication via `seenIds` Set.

## Code Standards

### Naming

| Element | Convention | Example |
|---------|-----------|---------|
| Files | camelCase | `batchAccumulator.ts`, `smartContract.ts`, `processBatch.ts` |
| Classes | PascalCase | `BatchAccumulator`, `BatchProcessor` |
| Functions | camelCase | `settleBatch`, `consumeMatches`, `ensureConsumerGroup` |
| Interfaces | PascalCase | `MatchWithMeta`, `SettlementResult`, `BatchProcessingError` |
| Config keys | SCREAMING_SNAKE_CASE (env) | `SETTLEMENT_BATCH_SIZE`, `REDIS_URL` |
| Constants | SCREAMING_SNAKE_CASE | `STREAM_KEY`, `CONSUMER_GROUP`, `MAX_BATCH` |

### Clean Code Rules

1. **Single responsibility per file** — `batchAccumulator.ts` only accumulates, `batchProcessor.ts` only orchestrates the poll loop, `processBatch.ts` only handles the settle→persist→ack flow.
2. **Retryable vs non-retryable errors** — always distinguish. Use `BatchProcessingError` with `retryable: boolean`. Retryable: network timeouts, transient contract errors. Non-retryable: already settled, invalid data, insufficient funds.
3. **Exponential backoff** — `delay = min(baseMs * 2^(failures-1), maxBackoffMs)`. Apply to both smart contract retries and database retries.
4. **Client caching** — Viem public/wallet clients are cached by `chainId|rpcUrl` key. Never create new HTTP transports per call.
5. **Multicall for batch checks** — use `publicClient.multicall()` to batch `isSettled` checks. One RPC call, not N.
6. **FK-ordered persistence** — insert in dependency order: matches → settlement_batches → settlement_items → (bond tokens, positions). Respect foreign key constraints.
7. **Idempotent operations** — use upsert patterns. A match arriving twice must not create duplicate rows.
8. **Graceful shutdown** — handle SIGTERM/SIGINT. Stop polling, finish current batch, close connections.
9. **Pending entry recovery** — on startup, reclaim pending entries from dead consumers via XCLAIM. Run reclaim on a separate timer during runtime.
10. **No floating promises** — every async operation must be awaited or explicitly fire-and-forget with error logging.
11. **File size limit** — source files should be <500 lines. Split larger files into focused modules.
12. **Layer separation** — database layer must not make RPC/blockchain calls. Smart contract interactions belong in `smartContract.ts`.
13. **Structured logging** — use the logger from `src/logger.ts`, not raw `console.log/warn/error`.

### Database Patterns

- Raw `pg.Pool` — no ORM. Singleton initialized on first use.
- Transaction-based operations for atomicity.
- Retry with exponential backoff (max 3 attempts) for transient errors (serialization, deadlock, connection).
- Fail fast on constraint violations — don't retry non-transient errors.

### ID Conversions

- Match UUID → `bytes32`: `keccak256(abi.encode(uuid))`
- `bytes32` → UUID: First 32 hex chars formatted as UUID
- Position ID: `SHA1(marketId-wallet).slice(0,32)` formatted as UUID

### Testing Rules

- Unit tests mock `smartContract` module globally in setup
- Integration tests require real Redis + PostgreSQL (set `REDIS_TEST_URL`, `DATABASE_URL`)
- Use `createIsolatedTestEnvironment()` for per-test Redis clients with unique stream names
- Use `waitForCondition()` for async assertions — never fixed `setTimeout` delays
- Test factories: `createMatch()`, `createMatchBatch()`, `createTestConfig()`
- Cleanup all test streams in `afterEach`
- Coverage threshold: 80% minimum enforced in jest.config.js (branches at 70% due to hard-to-exercise error paths)
- All recovery code paths (XCLAIM, event recovery) must have dedicated tests

### Formatting

Prettier (if configured). TypeScript strict mode. 2-space indent.
