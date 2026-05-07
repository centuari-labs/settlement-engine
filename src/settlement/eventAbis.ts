/**
 * Event ABI definitions for parsing settlement transaction receipt logs.
 * Sourced from Centuari and CentuariBondERC20Factory contracts.
 */

export const BOND_TOKEN_CREATED_EVENT = {
  type: 'event',
  name: 'BondTokenCreated',
  inputs: [
    { name: 'marketId', type: 'bytes32', indexed: true, internalType: 'bytes32' },
    { name: 'bondToken', type: 'address', indexed: true, internalType: 'address' },
    { name: 'loanToken', type: 'address', indexed: true, internalType: 'address' },
    { name: 'maturity', type: 'uint256', indexed: false, internalType: 'uint256' },
    { name: 'name', type: 'string', indexed: false, internalType: 'string' },
    { name: 'symbol', type: 'string', indexed: false, internalType: 'string' },
  ],
  anonymous: false,
} as const;

export const LEND_POSITION_CREATED_EVENT = {
  type: 'event',
  name: 'LendPositionCreated',
  inputs: [
    { name: 'marketId', type: 'bytes32', indexed: true, internalType: 'bytes32' },
    { name: 'lender', type: 'address', indexed: true, internalType: 'address' },
    { name: 'bondToken', type: 'address', indexed: true, internalType: 'address' },
    { name: 'cbtAmount', type: 'uint256', indexed: false, internalType: 'uint256' },
    { name: 'principal', type: 'uint256', indexed: false, internalType: 'uint256' },
    { name: 'rate', type: 'uint256', indexed: false, internalType: 'uint256' },
  ],
  anonymous: false,
} as const;

export const BORROW_POSITION_CREATED_EVENT = {
  type: 'event',
  name: 'BorrowPositionCreated',
  inputs: [
    { name: 'marketId', type: 'bytes32', indexed: true, internalType: 'bytes32' },
    { name: 'borrower', type: 'address', indexed: true, internalType: 'address' },
    { name: 'principal', type: 'uint256', indexed: false, internalType: 'uint256' },
    { name: 'debt', type: 'uint256', indexed: false, internalType: 'uint256' },
    { name: 'rate', type: 'uint256', indexed: false, internalType: 'uint256' },
  ],
  anonymous: false,
} as const;

/**
 * Emitted by BalanceLedger.markCollateral / unmarkCollateral. The 5-param
 * shape is the canonical Phase 1 event after the P1b-explicit refactor
 * (writer + user + asset + used + flaggedAt). Settlement-engine parses these
 * from the receipt of its own `Settlement.settleMatches` tx and DELETEs the
 * matching `pending_collateral_flags` rows (Phase 3 eager queue cleanup).
 */
export const COLLATERAL_FLAG_SET_EVENT = {
  type: 'event',
  name: 'CollateralFlagSet',
  inputs: [
    { name: 'writer', type: 'address', indexed: true, internalType: 'address' },
    { name: 'user', type: 'address', indexed: true, internalType: 'address' },
    { name: 'asset', type: 'address', indexed: true, internalType: 'address' },
    { name: 'used', type: 'bool', indexed: false, internalType: 'bool' },
    { name: 'flaggedAt', type: 'uint64', indexed: false, internalType: 'uint64' },
  ],
  anonymous: false,
} as const;

export const SETTLEMENT_EVENT_ABIS = [
  BOND_TOKEN_CREATED_EVENT,
  LEND_POSITION_CREATED_EVENT,
  BORROW_POSITION_CREATED_EVENT,
  COLLATERAL_FLAG_SET_EVENT,
] as const;
