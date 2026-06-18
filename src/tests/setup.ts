/**
 * Test setup and teardown utilities.
 * This file can be extended with global test setup/teardown hooks if needed.
 */

// Load environment variables for tests. .env.contracts is loaded FIRST so
// its keys win over .env (dotenv only sets unset keys by default — first-wins
// gives priority to the auto-generated file synced from
// smart-contract-revamp/bin/sync-to-services.sh).
import dotenv from 'dotenv';
import path from 'path';

dotenv.config({ path: path.resolve(__dirname, '..', '..', '.env.contracts') });
dotenv.config();

// The shared @centuari-labs/on-chain-effects package is published as ESM,
// which CJS ts-jest cannot load directly. Mock it globally so every suite
// gets a callable no-op; individual suites can override via jest.mock() of
// the same module id.
jest.mock('@centuari-labs/on-chain-effects', () => ({
  applyOnChainEffect: jest.fn(async () => ({ applied: true })),
  applyLendPositionCreatedMutation: jest.fn(async () => 1),
  applyBorrowPositionCreatedMutation: jest.fn(async () => 1),
  isAlreadyStamped: jest.fn(async () => false),
  // Real impl: pending-collateral-flags.test.ts loads the real repo under this
  // global mock and asserts the exact BYTEA bytes hexToBytea produces.
  hexToBytea: (hex: string) => Buffer.from(hex.replace(/^0x/, ''), 'hex'),
}));

// Automatically mock smart contract module for all tests
// This ensures unit and integration tests don't make real blockchain calls
jest.mock('../settlement/smartContract');

// Poison-match isolation (Track C8) now runs unconditionally in processBatch.
// Mock it globally so suites that don't exercise it (e.g. processBatch.test.ts)
// see a clean dry-run (no poison) and behave as before. Suites that test the
// real module (poisonIsolation.test.ts) unmock it; processBatch.poison.test.ts
// overrides the per-test behavior.
jest.mock('../settlement/poisonIsolation');

// Global test timeout can be adjusted here if needed
// Individual tests can override with jest.setTimeout()

beforeEach(() => {
  // Clear all mocks before each test
  jest.clearAllMocks();

  // Default: filterAlreadySettledMatches returns all matches as unsettled
  // (no matches already settled on-chain). Tests can override if needed.
  // Guard: skip if smartContract was unmocked (e.g. in smartContract.test.ts).
  const smartContract = require('../settlement/smartContract');
  if (typeof smartContract.filterAlreadySettledMatches.mockImplementation === 'function') {
    smartContract.filterAlreadySettledMatches.mockImplementation(
      async (matches: readonly { id: string; stream: string; payload: unknown }[]) => ({
        unsettled: [...matches],
        alreadySettled: [],
      }),
    );
  }

  // Default: poison-isolation dry-run is clean (returns null → settle all).
  // Guard: skip if the module was unmocked (poisonIsolation.test.ts).
  const poisonIsolation = require('../settlement/poisonIsolation');
  if (typeof poisonIsolation.simulateSettleBatch.mockImplementation === 'function') {
    poisonIsolation.simulateSettleBatch.mockResolvedValue(null);
  }
});

