import type { SettlementResult, SettlementError, SettleBatchOptions } from '../../settlement/smartContract';
import type { Match } from '../../schemas/match';

/**
 * Mock implementation of settleBatch that returns a successful result.
 *
 * @param options - Options for the settlement batch call.
 * @returns Promise resolving to a successful settlement result.
 */
const createSuccessfulSettlementResult = (
  options: SettleBatchOptions,
): SettlementResult => {
  const randomHex = (len: number): `0x${string}` =>
    `0x${Array.from({ length: len }, () =>
      Math.floor(Math.random() * 16).toString(16),
    ).join('')}` as `0x${string}`;
  return {
    transactionHash: randomHex(64),
    blockHash: randomHex(64),
    blockNumber: Math.floor(Math.random() * 1000000),
    gasUsed: options.matches.length * 50000,
    timestamp: Date.now(),
    settledMatchIds: options.matches.map((m) => m.matchId),
    bondTokenEvents: [],
    lendPositionEvents: [],
    borrowPositionEvents: [],
  };
};

/**
 * Creates a settlement error with the specified properties.
 *
 * @param message - Error message.
 * @param code - Optional error code.
 * @param retryable - Whether the error is retryable.
 * @param matchIds - Array of match IDs that failed.
 * @returns SettlementError object.
 */
export const createSettlementError = (
  message: string,
  code?: string,
  retryable = true,
  matchIds: readonly string[] = [],
): SettlementError => {
  return {
    message,
    code,
    retryable,
    failedMatchIds: matchIds,
  };
};

/**
 * Sets up the mock settleBatch function to return a successful result.
 * This is the default behavior for most tests.
 *
 * @param mockSettleBatch - The mocked settleBatch function from jest.
 */
export const setupMockSettleBatch = (
  mockSettleBatch: jest.MockedFunction<
    (options: SettleBatchOptions) => Promise<SettlementResult>
  >,
): void => {
  mockSettleBatch.mockImplementation(async (options: SettleBatchOptions) => {
    return createSuccessfulSettlementResult(options);
  });
};

/**
 * Sets up the mock settleBatch function to throw a settlement error.
 *
 * @param mockSettleBatch - The mocked settleBatch function from jest.
 * @param error - The settlement error to throw.
 */
export const setupMockSettleBatchError = (
  mockSettleBatch: jest.MockedFunction<
    (options: SettleBatchOptions) => Promise<SettlementResult>
  >,
  error: SettlementError,
): void => {
  mockSettleBatch.mockImplementation(async () => {
    throw error;
  });
};

/**
 * Sets up the mock settleBatch function to throw a network error.
 *
 * @param mockSettleBatch - The mocked settleBatch function from jest.
 * @param matchIds - Array of match IDs that failed.
 */
export const setupMockSettleBatchNetworkError = (
  mockSettleBatch: jest.MockedFunction<
    (options: SettleBatchOptions) => Promise<SettlementResult>
  >,
  matchIds: readonly string[] = [],
): void => {
  const error = createSettlementError(
    'Network error: Connection timeout',
    'NETWORK_ERROR',
    true,
    matchIds,
  );
  setupMockSettleBatchError(mockSettleBatch, error);
};

/**
 * Sets up the mock settleBatch function to throw a contract error.
 *
 * @param mockSettleBatch - The mocked settleBatch function from jest.
 * @param code - Error code (e.g., 'ALREADY_SETTLED', 'CONTRACT_PAUSED').
 * @param retryable - Whether the error is retryable.
 * @param matchIds - Array of match IDs that failed.
 */
export const setupMockSettleBatchContractError = (
  mockSettleBatch: jest.MockedFunction<
    (options: SettleBatchOptions) => Promise<SettlementResult>
  >,
  code: string,
  retryable = false,
  matchIds: readonly string[] = [],
): void => {
  const errorMessages: Record<string, string> = {
    ALREADY_SETTLED: 'Match already settled',
    CONTRACT_PAUSED: 'Contract is paused',
    EMPTY_BATCH: 'Cannot settle empty batch',
    INVALID_MATCH_DATA: 'Invalid match data',
    TRANSACTION_REVERTED: 'Transaction reverted',
  };

  const message = errorMessages[code] || 'Contract error';
  const error = createSettlementError(message, code, retryable, matchIds);
  setupMockSettleBatchError(mockSettleBatch, error);
};

/**
 * Resets the mock settleBatch function to default successful behavior.
 *
 * @param mockSettleBatch - The mocked settleBatch function from jest.
 */
export const resetMockSettleBatch = (
  mockSettleBatch: jest.MockedFunction<
    (options: SettleBatchOptions) => Promise<SettlementResult>
  >,
): void => {
  mockSettleBatch.mockReset();
  setupMockSettleBatch(mockSettleBatch);
};

/**
 * Gets the mocked settleBatch function from the smartContract module.
 * This helper assumes the module is already mocked via jest.mock().
 *
 * @returns The mocked settleBatch function.
 */
export const getMockSettleBatch = (): jest.MockedFunction<
  (options: SettleBatchOptions) => Promise<SettlementResult>
> => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { settleBatch } = require('../../settlement/smartContract');
  return settleBatch as jest.MockedFunction<
    (options: SettleBatchOptions) => Promise<SettlementResult>
  >;
};
