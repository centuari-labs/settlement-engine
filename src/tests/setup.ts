/**
 * Test setup and teardown utilities.
 * This file can be extended with global test setup/teardown hooks if needed.
 */

// Load environment variables from .env file for tests
import 'dotenv/config';

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
  const smartContract = require('../settlement/smartContract');
  smartContract.filterAlreadySettledMatches.mockImplementation(
    async (matches: readonly { id: string; stream: string; payload: unknown }[]) => ({
      unsettled: [...matches],
      alreadySettled: [],
    }),
  );
});

