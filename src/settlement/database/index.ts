export {
  getPool,
  withTransaction,
  mapPostgresErrorToDatabaseError,
  executeWithRetry,
  settlementBatchStatusSchema,
} from './connection';

export type {
  DatabaseError,
  PersistSettlementResultsOptions,
  SettlementBatchStatus,
  SettlementBatch,
  SettlementItem,
  RawSettlementEvents,
} from './connection';

export {
  persistSettlementResults,
  processSettlementEvents,
} from './persistence';

export {
  findUnprocessedSettlementBatches,
  retryEventProcessing,
  unlockFailedMatches,
  recordFailedMatches,
  restoreOrdersForFailedMatches,
} from './recovery';
