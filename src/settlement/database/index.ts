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
