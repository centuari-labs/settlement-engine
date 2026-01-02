## Settlement Engine

The Settlement Engine is an off-chain backend service that listens to Redis streams
for matched lend/borrow orders produced by the Matching Engine and coordinates
on-chain settlements and database updates.

At this stage, the service:

- Connects to Redis using `ioredis`.
- Ensures a consumer group exists for the `settlement:matches` stream.
- Listens for new match messages.
- Validates incoming messages with a Zod `matchSchema`.
- Logs validated matches via a placeholder batch processor.

### Project structure

- `src/index.ts`: Service entrypoint and lifecycle management.
- `src/config.ts`: Environment configuration and validation.
- `src/schemas/match.ts`: Zod schema and types for match messages and Redis constants.
- `src/redis/client.ts`: Redis client singleton.
- `src/redis/settlementMatchConsumer.ts`: Redis stream consumer for settlement matches.
- `src/settlement/processBatch.ts`: Placeholder for future batch settlement logic.

### Configuration

The service uses environment variables (loaded via `dotenv`) with sensible defaults:

- `REDIS_URL` (default: `redis://localhost:6379`)
- `REDIS_STREAM_SETTLEMENT_MATCHES` (default: `settlement:matches`)
- `REDIS_CONSUMER_GROUP` (default: `settlement-engine`)
- `REDIS_CONSUMER_NAME` (default: `settlement-engine-1`)
- `REDIS_READ_BLOCK_MS` (default: `5000`)
- `REDIS_READ_COUNT` (default: `10`)

You can create a `.env` file in the project root and set these values as needed.

### Scripts

- `npm run dev`: Run the settlement engine in watch mode using `ts-node-dev`.
- `npm run build`: Compile TypeScript to JavaScript in `dist/`.
- `npm run start`: Run the compiled service from `dist/index.js`.

### Running locally

1. Ensure Redis is running and accessible (for example on `localhost:6379`).
2. Install dependencies:

```bash
npm install
```

3. Start the service in development mode:

```bash
npm run dev
```

The service will create the consumer group for `settlement:matches` if it does not
already exist and begin consuming messages. Initially, it logs each validated match;
later you can extend `processSettlementBatch` to call smart contracts and update your
database in batches.

### Testing

The project includes both unit tests and integration tests:

- **Unit tests** (`*.test.ts`): Use mocks and don't require external dependencies
- **Integration tests** (`*.integration.test.ts`): Use a real Redis instance to test actual stream operations

#### Running Tests

```bash
# Run all tests
pnpm run test

# Run only unit tests (no Redis required)
pnpm run test:unit

# Run only integration tests (requires Redis)
pnpm run test:integration
```

#### Integration Test Requirements

Integration tests require a Redis server to be running. By default, tests connect to `redis://localhost:6379`. You can override this by setting the `REDIS_TEST_URL` environment variable:

```bash
REDIS_TEST_URL=redis://localhost:6380 pnpm run test:integration
```

Integration tests:
- Use real Redis stream operations (`xadd`, `xreadgroup`, `xack`, `xdel`, `xtrim`, `xgroup`)
- Create isolated test streams with unique names (timestamped)
- Clean up test data after each test
- Verify that data can be submitted to Redis streams and consumed correctly


