/**
 * Test setup and teardown utilities.
 * This file can be extended with global test setup/teardown hooks if needed.
 */

// Load environment variables from .env file for tests
import 'dotenv/config';

// The shared @centuari-labs/on-chain-effects package is published as ESM,
// which CJS ts-jest cannot load directly. Mock it globally so every suite
// gets a callable no-op; individual suites can override via jest.mock() of
// the same module id.
jest.mock('@centuari-labs/on-chain-effects', () => ({
  applyOnChainEffect: jest.fn(async () => ({ applied: true })),
}));

// Automatically mock smart contract module for all tests
// This ensures unit and integration tests don't make real blockchain calls
jest.mock('../settlement/smartContract');

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
});

