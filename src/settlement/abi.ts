/**
 * Contract ABI definitions for the settlement engine.
 */

/**
 * Contract ABI for the settlement contract.
 * Includes function definitions, view functions, and error definitions
 * so viem can decode reverts properly.
 */
export const SETTLEMENT_CONTRACT_ABI = [
  {
    type: 'function',
    name: 'settleMatches',
    inputs: [
      {
        name: 'matches',
        type: 'tuple[]',
        components: [
          { name: 'matchId', type: 'bytes32' },
          { name: 'marketId', type: 'bytes32' },
          { name: 'lendOrderId', type: 'bytes32' },
          { name: 'borrowOrderId', type: 'bytes32' },
          { name: 'lender', type: 'address' },
          { name: 'borrower', type: 'address' },
          { name: 'matchedAmount', type: 'uint256' },
          { name: 'rate', type: 'uint256' },
          { name: 'loanToken', type: 'address' },
          { name: 'maturity', type: 'uint256' },
          { name: 'timestamp', type: 'uint256' },
          { name: 'borrowerIsTaker', type: 'bool' },
          { name: 'lenderSettlementFee', type: 'uint256' },
          { name: 'borrowerSettlementFee', type: 'uint256' },
          { name: 'makerFeeAmount', type: 'uint256' },
          { name: 'takerFeeAmount', type: 'uint256' },
        ],
      },
    ],
    outputs: [],
    stateMutability: 'nonpayable',
  },
  {
    type: 'function',
    name: 'isSettled',
    inputs: [{ name: 'matchId', type: 'bytes32' }],
    outputs: [{ type: 'bool' }],
    stateMutability: 'view',
  },
  // Settlement errors
  { type: 'error', name: 'AlreadySettled', inputs: [{ name: 'matchId', type: 'bytes32' }] },
  { type: 'error', name: 'Unauthorized', inputs: [] },
  { type: 'error', name: 'InvalidMatchData', inputs: [] },
  { type: 'error', name: 'ZeroAddress', inputs: [] },
  { type: 'error', name: 'ContractPaused', inputs: [] },
  { type: 'error', name: 'EmptyBatch', inputs: [] },
  // Centuari errors
  { type: 'error', name: 'InvalidAmount', inputs: [] },
  { type: 'error', name: 'InvalidMaturity', inputs: [] },
  { type: 'error', name: 'BondTokenNotFound', inputs: [] },
  // Treasury errors
  { type: 'error', name: 'InsufficientFunds', inputs: [] },
  // OpenZeppelin errors
  { type: 'error', name: 'EnforcedPause', inputs: [] },
  { type: 'error', name: 'ReentrancyGuardReentrantCall', inputs: [] },
  {
    type: 'error',
    name: 'AccessControlUnauthorizedAccount',
    inputs: [
      { name: 'account', type: 'address' },
      { name: 'neededRole', type: 'bytes32' },
    ],
  },
] as const;

export const erc20MetadataAbi = [
  { type: 'function', name: 'name', inputs: [], outputs: [{ type: 'string' }], stateMutability: 'view' },
  { type: 'function', name: 'symbol', inputs: [], outputs: [{ type: 'string' }], stateMutability: 'view' },
] as const;
