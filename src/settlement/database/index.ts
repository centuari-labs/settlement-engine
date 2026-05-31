export {
  getPool,
  withTransaction,
  mapPostgresErrorToDatabaseError,
  executeWithRetry,
} from './connection';

export type { DatabaseError } from './connection';

export { applySettlementResult } from './apply-settlement';

export {
  readForBorrowers as readPendingCollateralFlagsForBorrowers,
  clearForEvent as clearPendingCollateralFlagForEvent,
} from './pending-collateral-flags';

export {
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
} from './order-failure';

export {
  applyMatchSettlementWriteback,
  writebackSettledMatches,
} from './lock-release';

export {
  findStuckPendingMatches,
  remediateUnsettledMatch,
  SWEEPER_FAILURE_REASON,
} from './pending-settlement-sweep';

export type { StuckMatch } from './pending-settlement-sweep';
