# CLAUDE.md — Settlement Engine

## Stack

Node.js · TypeScript (strict) · Viem · ioredis (Streams + Consumer Groups) · PostgreSQL (raw pg) · Zod · Jest 29 · npm

## Commands

```bash
npm run dev                # ts-node-dev --respawn --transpile-only
npm run build              # tsc -p tsconfig.json
npm run start              # node dist/index.js
npm run test               # jest (all tests, 4GB heap)
npm run test:unit          # unit only (ignores integration)
npm run test:integration   # integration only
npm run test:coverage      # with coverage report
```

## Architecture

```
src/
├── index.ts                          # Entry point: startup, shutdown, consumer group init
├── config.ts                         # Zod-validated config (17 env vars with defaults)
├── schemas/
│   └── match.ts                      # Match Zod schema + Redis stream constants
├── redis/
│   ├── client.ts                     # Singleton Redis client
│   └── settlementMatchConsumer.ts    # Stream reading, entry parsing, pending recovery
├── settlement/
│   ├── batchAccumulator.ts           # Hybrid batching (size + time triggers)
│   ├── batchProcessor.ts            # Main polling loop with backoff
│   ├── processBatch.ts              # Orchestrates: filter → settle → persist → ack
│   ├── smartContract.ts             # Viem clients, multicall, error mapping, event parsing
│   ├── database.ts                  # PostgreSQL operations with retry logic
│   ├── helpers.ts                   # UUID ↔ bytes32 conversion, hash-based IDs
│   └── eventAbis.ts                 # Contract event definitions
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

### Formatting

Prettier (if configured). TypeScript strict mode. 2-space indent.
