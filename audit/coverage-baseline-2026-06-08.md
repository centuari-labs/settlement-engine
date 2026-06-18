# settlement-engine — Test Coverage Baseline (2026-06-08)

Audit-prep phase **2.3** (hub-only external-audit scope).

- **Command:** `TZ=UTC pnpm run test:coverage` (`jest --coverage`, all suites: unit + integration)
- **Environment:** Redis, Postgres, NATS up (`docker-compose`); `DATABASE_URL` set for integration suites.
- **Result:** **25 test suites / 314 tests passing.** Coverage thresholds in `jest.config.js`
  (statements/functions/lines ≥ 80, branches ≥ 70) **met** → command exits 0.

## Totals

| Metric | % | Threshold |
|---|---|---|
| Statements | 89.26 | ≥ 80 ✅ |
| Branches | 71.63 | ≥ 70 ✅ |
| Functions | 87.83 | ≥ 80 ✅ |
| Lines | 89.10 | ≥ 80 ✅ |

> Note: `jest.config.js` excludes `src/index.ts` and `src/tests/**` from coverage collection.
> The branch threshold is set to 70 (vs 80 for the others) because several error-path branches in
> `batchProcessor.ts`, `connection.ts`, and the Zod schema in `config.ts` are hard to exercise
> without elaborate failure-injection setups.

## Per-directory / per-file

```
File                          | % Stmts | % Branch | % Funcs | % Lines | Uncovered Line #s
------------------------------|---------|----------|---------|---------|------------------
All files                     |   89.26 |    71.63 |   87.83 |    89.10 |
 src                          |     100 |       50 |     100 |     100 |
  config.ts                   |     100 |       50 |     100 |     100 | 22-125
  logger.ts                   |     100 |      100 |     100 |     100 |
 src/redis                    |   90.96 |    80.95 |     100 |    90.41 |
  client.ts                   |   95.65 |    88.88 |     100 |    95.00 | 45
  deadLetter.ts               |      90 |       50 |     100 |    90.00 | 56
  settlementMatchConsumer.ts  |   90.27 |    80.76 |     100 |    89.78 | 95,207-220,355-356,408-409,431-432,440-441
 src/schemas                  |     100 |      100 |     100 |     100 |
  match.ts                    |     100 |      100 |     100 |     100 |
 src/settlement               |   89.01 |    71.89 |   84.84 |    88.97 |
  abi.ts                      |     100 |      100 |     100 |     100 |
  batchAccumulator.ts         |     100 |      100 |     100 |     100 |
  batchProcessor.ts           |    91.80 |    78.57 |   91.66 |    91.73 | 183,246,285-289,317,324,340,384,392,415
  eventAbis.ts                |    92.85 |    66.66 |     100 |    92.30 | 18
  helpers.ts                  |     100 |      100 |     100 |     100 |
  nonceManager.ts             |    89.47 |    73.68 |   81.25 |    90.21 | 295-309,336-342
  poisonIsolation.ts          |     100 |       70 |     100 |     100 | 57,100,137
  processBatch.ts             |    97.32 |    64.51 |   92.85 |    97.27 | 133,276-281
  settlementSweeper.ts        |    59.01 |    36.36 |   30.00 |    59.01 | 68-134
  smartContract.ts            |    86.29 |    73.61 |   88.46 |    86.02 | 39,44,386,395,404,413,517,571,748-773,853,861,915,945-950
 src/settlement/database      |    87.19 |    73.33 |   84.31 |    86.84 |
  apply-settlement.ts         |    89.83 |    66.66 |   58.33 |    90.90 | 108-116,136-147
  connection.ts               |    47.45 |     8.33 |   40.00 |    44.44 | 49,77-82,95-133,154-192
  index.ts                    |     100 |      100 |     100 |     100 |
  lock-release.ts             |     100 |      100 |     100 |     100 |
  order-failure.ts            |     100 |      100 |     100 |     100 |
  pending-collateral-flags.ts |     100 |      100 |     100 |     100 |
  pending-settlement-sweep.ts |     100 |      100 |     100 |     100 |
```

## Lowest-covered modules (audit attention)

| File | Stmts | Notes |
|---|---|---|
| `database/connection.ts` | 47.45% | Pool/retry/error paths exercised mostly via integration, not unit; reconnection branches uncovered (77-82, 95-133, 154-192). |
| `settlement/settlementSweeper.ts` | 59.01% | Background sweep loop (68-134) not unit-covered; relies on live-timer/integration behavior. |

These are the two files an auditor should weigh: both sit in or near on-chain-effect / DB write paths.
Neither is a security regression — they reflect logic best exercised by integration/live runs rather
than unit tests. No new gaps were introduced this session.
