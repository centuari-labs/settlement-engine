/**
 * Test setup and teardown utilities.
 * This file can be extended with global test setup/teardown hooks if needed.
 */

// Automatically mock smart contract module for all tests
// This ensures unit and integration tests don't make real blockchain calls
jest.mock('../settlement/smartContract');

// Global test timeout can be adjusted here if needed
// Individual tests can override with jest.setTimeout()

beforeEach(() => {
  // Clear all mocks before each test
  jest.clearAllMocks();
});

