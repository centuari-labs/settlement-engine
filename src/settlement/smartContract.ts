import { createHash } from 'crypto';
import {
  createWalletClient,
  createPublicClient,
  http,
  type Address,
  type Hash,
  keccak256,
  toBytes,
  type Chain,
  defineChain,
  decodeEventLog,
  type PublicClient,
  type TransactionReceipt,
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { loadConfig, type AppConfig } from '../config';
import type { Match } from '../schemas/match';
import type { MatchWithMeta } from '../redis/settlementMatchConsumer';
import {
  BOND_TOKEN_CREATED_EVENT,
  LEND_POSITION_CREATED_EVENT,
  BORROW_POSITION_CREATED_EVENT,
} from './eventAbis';
import { SETTLEMENT_CONTRACT_ABI, erc20MetadataAbi } from './abi';
import type { NonceManager } from './nonceManager';
import { logger } from '../logger';

/**
 * Parsed BondTokenCreated event from settlement tx receipt.
 */
export interface ParsedBondToken {
  readonly marketId: string;
  readonly bondToken: string;
  readonly loanToken: string;
  readonly maturity: bigint;
  readonly name: string;
  readonly symbol: string;
  readonly logIndex: number;
}

/**
 * Parsed LendPositionCreated event from settlement tx receipt.
 */
export interface ParsedLendPosition {
  readonly marketId: string;
  readonly lender: string;
  readonly bondToken: string;
  readonly cbtAmount: bigint;
  readonly principal: bigint;
  readonly rate: bigint;
  readonly logIndex: number;
}

/**
 * Parsed BorrowPositionCreated event from settlement tx receipt.
 */
export interface ParsedBorrowPosition {
  readonly marketId: string;
  readonly borrower: string;
  readonly principal: bigint;
  readonly debt: bigint;
  readonly rate: bigint;
  readonly logIndex: number;
}

/**
 * Result of a smart contract settlement batch call.
 */
export interface SettlementResult {
  /**
   * Transaction hash of the settlement transaction.
   */
  readonly transactionHash: string;
  /**
   * Block hash of the block where the transaction was mined. Used as part of
   * the idempotency stamp written alongside eager DB mutations via
   * applyOnChainEffect.
   */
  readonly blockHash: Hash;
  /**
   * Block number where the transaction was mined.
   */
  readonly blockNumber: number;
  /**
   * Gas used for the transaction.
   */
  readonly gasUsed: number;
  /**
   * Timestamp when the settlement was executed.
   */
  readonly timestamp: number;
  /**
   * Array of match IDs that were successfully settled.
   */
  readonly settledMatchIds: readonly string[];
  /**
   * Parsed BondTokenCreated events from the settlement tx receipt.
   */
  readonly bondTokenEvents: readonly ParsedBondToken[];
  /**
   * Parsed LendPositionCreated events from the settlement tx receipt.
   */
  readonly lendPositionEvents: readonly ParsedLendPosition[];
  /**
   * Parsed BorrowPositionCreated events from the settlement tx receipt.
   */
  readonly borrowPositionEvents: readonly ParsedBorrowPosition[];
  /**
   * Raw mined transaction receipt. Held here so downstream DB writers (via
   * `applyOnChainEffect`) don't re-fetch it per event — settlement batches
   * can emit N+M+K logs, and re-running `waitForTransactionReceipt` for each
   * would be wasteful.
   */
  readonly receipt: TransactionReceipt;
}

/**
 * Error information for failed settlement attempts.
 */
export interface SettlementError {
  /**
   * Error message describing the failure.
   */
  readonly message: string;
  /**
   * Error code if available.
   */
  readonly code?: string;
  /**
   * Whether the error is retryable (transient).
   */
  readonly retryable: boolean;
  /**
   * Array of match IDs that failed to settle.
   */
  readonly failedMatchIds: readonly string[];
}

/**
 * Options for calling the smart contract settlement function.
 */
export interface SettleBatchOptions {
  /**
   * Array of matches to settle in a single batch.
   */
  readonly matches: readonly Match[];
  /**
   * Maximum number of retries for transient errors.
   */
  readonly maxRetries?: number;
  /**
   * Initial retry delay in milliseconds (exponential backoff).
   */
  readonly retryDelayMs?: number;
  /**
   * Application configuration. If not provided, will be loaded from environment.
   */
  readonly config?: AppConfig;
  /**
   * Nonce manager for explicit nonce sequencing.
   * When provided, acquires and manages nonces for each transaction.
   */
  readonly nonceManager?: NonceManager;
  /**
   * Per-borrower lookup of pending collateral flag assets read from
   * `pending_collateral_flags` at settle time (Phase 3 queue-driven encoder).
   * Each match's `borrower` is keyed by lowercase address into this map; the
   * resolved array is encoded into `MatchData.collateralAssets` so
   * `Centuari.settleMatch` flags exactly the assets the user has currently
   * queued. Absent map / absent borrower entries default to `[]` — the
   * settle call still goes through, just with no on-chain flag mutation.
   * The match payload's plumbing-only `borrowerCollateralAssets` (P2) is
   * NOT used here — settlement reads the queue directly so the user's
   * latest unflag wins over a stale order-time snapshot.
   */
  readonly collateralAssetsByBorrower?: ReadonlyMap<string, readonly Address[]>;
}

/**
 * Error selector hex values for fallback matching when viem cannot decode.
 * These match the first 4 bytes of keccak256(error_signature).
 */
const ERROR_SELECTORS = {
  AlreadySettled: '0xb196a44a',
  ContractPaused: '0xab35696f',
  EmptyBatch: '0xc2e5347d',
  InvalidMatchData: '0x388cfcc2',
  Unauthorized: '0x82b42900',
  InvalidAmount: '0x2c5211c6',
  InvalidMaturity: '0xc7a682c8',
  InsufficientFunds: '0x356680b7',
  BondTokenNotFound: '0xca42fe63',
  ZeroAddress: '0xd92e233d',
  EnforcedPause: '0xd93c0665',
  ReentrancyGuardReentrantCall: '0x3ee5aeb5',
  AccessControlUnauthorizedAccount: '0xe2517d3f',
} as const;

/**
 * Converts a UUID string to a bytes32 value using keccak256 hash.
 *
 * @param uuid - UUID string to convert.
 * @returns bytes32 hash of the UUID.
 */
const uuidToBytes32 = (uuid: string): Hash => {
  const uuidBytes = toBytes(uuid);
  return keccak256(uuidBytes);
};

/**
 * Converts a UUID string directly to a bytes32 value by stripping dashes
 * and zero-padding to 64 hex chars. This is NOT keccak-hashed, so
 * bytes32ToUuid(uuidToBytes32Direct(uuid)) === uuid.
 *
 * Used only for marketId to ensure the on-chain bytes32 round-trips
 * back to the original backend UUID.
 *
 * @param uuid - UUID string to convert.
 * @returns bytes32 zero-padded hex of the UUID.
 */
const uuidToBytes32Direct = (uuid: string): Hash => {
  const hex = uuid.replace(/-/g, '');
  return `0x${hex.padEnd(64, '0')}` as Hash;
};

/**
 * Transforms a Match object to the contract's MatchData struct format.
 *
 * @param match - Match object to transform.
 * @param collateralAssetsByBorrower - Optional per-borrower queue lookup
 *   from Phase 3 (`pending_collateral_flags`). The key is the lowercased
 *   borrower address. Missing entries resolve to `[]` so settle still goes
 *   through with no on-chain flag mutation for that borrower.
 * @returns MatchData struct in the format expected by the contract.
 */
const transformMatchToContractFormat = (
  match: Match,
  collateralAssetsByBorrower?: ReadonlyMap<string, readonly Address[]>,
) => {
  const borrower = match.borrowerWallet as Address;
  const queued = collateralAssetsByBorrower?.get(borrower.toLowerCase());
  // Defensive de-dupe + lowercase: the repo already returns
  // case-normalized + de-duped arrays, but this guards against future
  // callers passing the map directly without going through the repo.
  const collateralAssets = queued
    ? Array.from(new Set(queued.map((a) => a.toLowerCase()))) as Address[]
    : [];

  return {
    matchId: uuidToBytes32(match.matchId),
    marketId: uuidToBytes32Direct(match.marketId),
    lendOrderId: uuidToBytes32(match.lendOrderId),
    borrowOrderId: uuidToBytes32(match.borrowOrderId),
    lender: match.lenderWallet as Address,
    borrower,
    matchedAmount: BigInt(match.matchedAmount),
    rate: BigInt(match.rate),
    loanToken: match.loanToken as Address,
    maturity: BigInt(match.maturity),
    timestamp: BigInt(match.timestamp),
    borrowerIsTaker: match.borrowerIsTaker,
    lenderSettlementFee: BigInt(match.lenderSettlementFeeAmount),
    borrowerSettlementFee: BigInt(match.borrowerSettlementFeeAmount),
    makerFeeAmount: BigInt(match.makerFeeAmount),
    takerFeeAmount: BigInt(match.takerFeeAmount),
    collateralAssets,
  };
};

/**
 * Maps contract error to SettlementError with appropriate retryability.
 * Matches both decoded error names and hex selectors (defense-in-depth when viem cannot decode).
 *
 * @param error - Error object from contract call.
 * @param matchIds - Array of match IDs that failed.
 * @returns SettlementError with appropriate retryability flag.
 */
const mapContractError = (
  error: unknown,
  matchIds: readonly string[],
): SettlementError => {
  const errorMessage = error instanceof Error ? error.message : String(error);

  const hasError = (name: keyof typeof ERROR_SELECTORS) =>
    errorMessage.includes(name) || errorMessage.includes(ERROR_SELECTORS[name]);

  // Non-retryable: already settled
  if (hasError('AlreadySettled')) {
    return {
      message: 'Match already settled',
      code: 'ALREADY_SETTLED',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Retryable: contract paused (operator can unpause)
  if (hasError('ContractPaused') || hasError('EnforcedPause')) {
    return {
      message: 'Contract is paused',
      code: 'CONTRACT_PAUSED',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  // Non-retryable: empty batch
  if (hasError('EmptyBatch')) {
    return {
      message: 'Cannot settle empty batch',
      code: 'EMPTY_BATCH',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Non-retryable: invalid match data
  if (hasError('InvalidMatchData')) {
    return {
      message: 'Invalid match data',
      code: 'INVALID_MATCH_DATA',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Non-retryable: insufficient funds (lender has not deposited enough)
  if (hasError('InsufficientFunds')) {
    return {
      message: 'Insufficient funds in Treasury',
      code: 'INSUFFICIENT_FUNDS',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Non-retryable: maturity in the past
  if (hasError('InvalidMaturity')) {
    return {
      message: 'Invalid maturity (expired)',
      code: 'INVALID_MATURITY',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Non-retryable: unauthorized, invalid amount, etc.
  if (
    hasError('Unauthorized') ||
    hasError('InvalidAmount') ||
    hasError('ZeroAddress') ||
    hasError('BondTokenNotFound') ||
    hasError('AccessControlUnauthorizedAccount')
  ) {
    return {
      message: errorMessage,
      code: 'CONTRACT_ERROR',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Retryable: reentrancy (transient)
  if (hasError('ReentrancyGuardReentrantCall')) {
    return {
      message: 'Reentrancy guard triggered',
      code: 'REENTRANCY',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  // Nonce-related errors
  if (
    errorMessage.includes('nonce too low') ||
    errorMessage.includes('nonce has already been used')
  ) {
    return {
      message: `Nonce too low: ${errorMessage}`,
      code: 'NONCE_TOO_LOW',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  if (errorMessage.includes('replacement transaction underpriced')) {
    return {
      message: `Replacement underpriced: ${errorMessage}`,
      code: 'REPLACEMENT_UNDERPRICED',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  if (errorMessage.includes('already known')) {
    return {
      message: `Transaction already known: ${errorMessage}`,
      code: 'TX_ALREADY_KNOWN',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  if (errorMessage.includes('insufficient funds for gas')) {
    return {
      message: `Insufficient funds for gas: ${errorMessage}`,
      code: 'INSUFFICIENT_GAS',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  // Network/RPC errors are retryable
  if (
    errorMessage.includes('network') ||
    errorMessage.includes('timeout') ||
    errorMessage.includes('ECONNREFUSED') ||
    errorMessage.includes('fetch')
  ) {
    return {
      message: `Network error: ${errorMessage}`,
      code: 'NETWORK_ERROR',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  // Default: assume retryable for transaction failures
  return {
    message: errorMessage,
    code: 'UNKNOWN_ERROR',
    retryable: true,
    failedMatchIds: matchIds,
  };
};

/**
 * Creates a custom chain configuration based on chain ID.
 *
 * @param chainId - Chain ID to create configuration for.
 * @returns Chain configuration.
 */
const createChainFromId = (chainId: number): Chain => {
  return defineChain({
    id: chainId,
    name: `Chain ${chainId}`,
    nativeCurrency: {
      name: 'Ether',
      symbol: 'ETH',
      decimals: 18,
    },
    rpcUrls: {
      default: {
        http: [],
      },
    },
    contracts: {
      multicall3: {
        address: '0xcA11bde05977b3631167028862bE2a173976CA11',
      },
    },
  });
};

/**
 * Cached viem clients keyed by "<chainId>|<rpcUrl>" to avoid creating new
 * HTTP transports on every poll cycle (which leaks memory).
 */
let cachedPublicClient: ReturnType<typeof createPublicClient> | null = null;
let cachedClientKey: string | null = null;

export const getPublicClient = (config: AppConfig) => {
  const key = `${config.ethereumChainId}|${config.ethereumRpcUrl}`;
  if (cachedPublicClient && cachedClientKey === key) {
    return cachedPublicClient;
  }
  const chain = createChainFromId(config.ethereumChainId);
  cachedPublicClient = createPublicClient({
    chain,
    transport: http(config.ethereumRpcUrl),
  });
  cachedClientKey = key;
  return cachedPublicClient;
};

let cachedWalletClient: ReturnType<typeof createWalletClient> | null = null;
let cachedWalletKey: string | null = null;

const getWalletClient = (config: AppConfig) => {
  const keyHash = createHash('sha256').update(config.settlementPrivateKey).digest('hex');
  const key = `${config.ethereumChainId}|${config.ethereumRpcUrl}|${keyHash}`;
  if (cachedWalletClient && cachedWalletKey === key) {
    return cachedWalletClient;
  }
  const chain = createChainFromId(config.ethereumChainId);
  const privateKey = config.settlementPrivateKey.startsWith('0x')
    ? (config.settlementPrivateKey as `0x${string}`)
    : (`0x${config.settlementPrivateKey}` as `0x${string}`);
  const account = privateKeyToAccount(privateKey);
  cachedWalletClient = createWalletClient({
    account,
    chain,
    transport: http(config.ethereumRpcUrl),
  });
  cachedWalletKey = key;
  return cachedWalletClient;
};

/**
 * Parse settlement transaction receipt logs to extract BondTokenCreated,
 * LendPositionCreated, and BorrowPositionCreated events.
 */
type ReceiptLog = {
  readonly topics: readonly `0x${string}`[];
  readonly data: `0x${string}`;
  readonly logIndex: number | null;
};

const parseReceiptLogs = (logs: readonly ReceiptLog[]): {
  bondTokenEvents: ParsedBondToken[];
  lendPositionEvents: ParsedLendPosition[];
  borrowPositionEvents: ParsedBorrowPosition[];
} => {
  const bondTokenEvents: ParsedBondToken[] = [];
  const lendPositionEvents: ParsedLendPosition[] = [];
  const borrowPositionEvents: ParsedBorrowPosition[] = [];

  for (const log of logs) {
    // logIndex is null on pending logs; settlement logs come from mined
    // receipts, so this is defensive — skip unidentifiable logs rather than
    // stamping a row with a null index.
    if (log.logIndex === null) {
      continue;
    }
    const logIndex = log.logIndex;

    try {
      const decoded = decodeEventLog({
        abi: [BOND_TOKEN_CREATED_EVENT],
        data: log.data,
        topics: log.topics as [`0x${string}`, ...`0x${string}`[]],
      });
      if (decoded.eventName === 'BondTokenCreated') {
        // Narrow `decoded.args` once: SETTLEMENT_CONTRACT_ABI / event ABIs are
        // now JSON-imported (synced from smart-contract-revamp) and viem can't
        // infer literal field types from a wide AbiEvent.
        const args = decoded.args as unknown as {
          marketId: string;
          bondToken: string;
          loanToken: string;
          maturity: bigint;
          name: string;
          symbol: string;
        };
        bondTokenEvents.push({
          marketId: args.marketId,
          bondToken: args.bondToken.toLowerCase(),
          loanToken: args.loanToken.toLowerCase(),
          maturity: args.maturity,
          name: args.name,
          symbol: args.symbol,
          logIndex,
        });
      }
    } catch {
      // Not a BondTokenCreated event, try next
    }

    try {
      const decoded = decodeEventLog({
        abi: [LEND_POSITION_CREATED_EVENT],
        data: log.data,
        topics: log.topics as [`0x${string}`, ...`0x${string}`[]],
      });
      if (decoded.eventName === 'LendPositionCreated') {
        const args = decoded.args as unknown as {
          marketId: string;
          lender: string;
          bondToken: string;
          cbtAmount: bigint;
          principal: bigint;
          rate: bigint;
        };
        lendPositionEvents.push({
          marketId: args.marketId,
          lender: args.lender.toLowerCase(),
          bondToken: args.bondToken.toLowerCase(),
          cbtAmount: args.cbtAmount,
          principal: args.principal,
          rate: args.rate,
          logIndex,
        });
      }
    } catch {
      // Not a LendPositionCreated event, try next
    }

    try {
      const decoded = decodeEventLog({
        abi: [BORROW_POSITION_CREATED_EVENT],
        data: log.data,
        topics: log.topics as [`0x${string}`, ...`0x${string}`[]],
      });
      if (decoded.eventName === 'BorrowPositionCreated') {
        const args = decoded.args as unknown as {
          marketId: string;
          borrower: string;
          principal: bigint;
          debt: bigint;
          rate: bigint;
        };
        borrowPositionEvents.push({
          marketId: args.marketId,
          borrower: args.borrower.toLowerCase(),
          principal: args.principal,
          debt: args.debt,
          rate: args.rate,
          logIndex,
        });
      }
    } catch {
      // Not a BorrowPositionCreated event
    }
  }

  return { bondTokenEvents, lendPositionEvents, borrowPositionEvents };
};

/**
 * Result of filtering already-settled matches.
 */
export interface FilterAlreadySettledResult {
  /** Matches that have not been settled yet (safe to submit) */
  readonly unsettled: readonly MatchWithMeta[];
  /** Matches that were already settled on-chain (should be ACKed from Redis) */
  readonly alreadySettled: readonly MatchWithMeta[];
}

/**
 * Filter out matches that have already been settled on-chain.
 * Uses multicall to batch all isSettled checks into a single RPC request.
 *
 * @param matches - Matches to check.
 * @param config - Application configuration. If not provided, will be loaded from environment.
 * @returns Object with unsettled and alreadySettled arrays.
 */
export const filterAlreadySettledMatches = async (
  matches: readonly MatchWithMeta[],
  config: AppConfig = loadConfig(),
): Promise<FilterAlreadySettledResult> => {
  if (matches.length === 0) {
    return { unsettled: [], alreadySettled: [] };
  }

  const publicClient = getPublicClient(config);
  const contractAddress = config.settlementContractAddress as Address;

  const results = await publicClient.multicall({
    contracts: matches.map((match) => ({
      address: contractAddress,
      abi: SETTLEMENT_CONTRACT_ABI,
      functionName: 'isSettled' as const,
      args: [keccak256(toBytes(match.payload.matchId))] as const,
    })),
  });

  const unsettled: MatchWithMeta[] = [];
  const alreadySettled: MatchWithMeta[] = [];

  for (let i = 0; i < matches.length; i++) {
    const result = results[i];
    if (result.status === 'success' && result.result === true) {
      alreadySettled.push(matches[i]);
    } else {
      unsettled.push(matches[i]);
    }
  }

  if (alreadySettled.length > 0) {
    logger.info(
      {
        component: 'smart-contract',
        count: alreadySettled.length,
        matchIds: alreadySettled.map((m) => m.payload.matchId),
      },
      'Filtered out already-settled matches',
    );
  }

  return { unsettled, alreadySettled };
};

/**
 * Check on-chain settled status for a list of match IDs (Track C2 sweeper).
 *
 * Unlike {@link filterAlreadySettledMatches}, this takes bare match-ID strings
 * (the stuck-PENDING sweeper only has IDs, not full payloads) and returns a
 * `Map<matchId, isSettled>`. Match IDs whose multicall read FAILED are OMITTED
 * from the map (treated as "unknown" by the caller, which skips them that
 * round rather than risk unlocking a possibly-settled match).
 *
 * @param matchIds - Match UUIDs to check.
 * @param config - App configuration (defaults to loaded env config).
 * @returns Map of matchId -> settled boolean (failed reads omitted).
 */
export const checkMatchesSettledOnChain = async (
  matchIds: readonly string[],
  config: AppConfig = loadConfig(),
): Promise<Map<string, boolean>> => {
  const settled = new Map<string, boolean>();
  if (matchIds.length === 0) {
    return settled;
  }

  const publicClient = getPublicClient(config);
  const contractAddress = config.settlementContractAddress as Address;

  const results = await publicClient.multicall({
    contracts: matchIds.map((matchId) => ({
      address: contractAddress,
      abi: SETTLEMENT_CONTRACT_ABI,
      functionName: 'isSettled' as const,
      args: [keccak256(toBytes(matchId))] as const,
    })),
  });

  for (let i = 0; i < matchIds.length; i++) {
    const result = results[i];
    if (result.status === 'success') {
      settled.set(matchIds[i], result.result === true);
    }
    // status === 'failure' -> omit (unknown); caller skips this round.
  }

  return settled;
};

/**
 * Call the smart contract to settle a batch of matches.
 *
 * @param options - Options for the settlement batch call.
 * @returns Promise resolving to the settlement result.
 * @throws SettlementError if the settlement fails.
 */
export const settleBatch = async (
  options: SettleBatchOptions,
): Promise<SettlementResult> => {
  const {
    matches,
    maxRetries = 3,
    retryDelayMs = 1000,
    config = loadConfig(),
    nonceManager,
    collateralAssetsByBorrower,
  } = options;

  if (matches.length === 0) {
    const error: SettlementError = {
      message: 'Cannot settle empty batch',
      code: 'EMPTY_BATCH',
      retryable: false,
      failedMatchIds: [],
    };
    throw error;
  }

  const publicClient = getPublicClient(config);
  const walletClient = getWalletClient(config);
  const account = walletClient.account!;

  // Transform matches to contract format. Per-borrower `collateralAssets`
  // are filled from the upstream `pending_collateral_flags` lookup (Phase 3
  // queue-driven encoder); missing entries default to `[]`.
  const contractMatches = matches.map((m) =>
    transformMatchToContractFormat(m, collateralAssetsByBorrower),
  );

  logger.info(
    {
      component: 'smart-contract',
      matchCount: matches.length,
      matchIds: matches.map((m) => m.matchId),
      maxRetries,
      retryDelayMs,
      settlementContractAddress: config.settlementContractAddress,
      ethereumChainId: config.ethereumChainId,
      ethereumRpcUrl: config.ethereumRpcUrl,
    },
    'Settling batch',
  );

  try {
    // Acquire nonce if nonce manager is provided
    const nonce = nonceManager
      ? await nonceManager.acquireNonce()
      : undefined;

    // Call the settleMatches function
    const chain = createChainFromId(config.ethereumChainId);
    const hash = await walletClient.writeContract({
      address: config.settlementContractAddress as Address,
      abi: SETTLEMENT_CONTRACT_ABI,
      functionName: 'settleMatches',
      args: [contractMatches],
      account,
      chain,
      nonce,
    });

    // Record pending tx for crash recovery
    if (nonceManager) {
      await nonceManager.confirmNonce(hash);
    }

    // Wait for transaction receipt
    const receipt = await publicClient.waitForTransactionReceipt({ hash });

    // Tx confirmed on-chain — release nonce lock
    if (nonceManager) {
      await nonceManager.onTxConfirmed();
    }

    if (!receipt.status || receipt.status === 'reverted') {
      const error: SettlementError = {
        message: 'Transaction reverted',
        code: 'TRANSACTION_REVERTED',
        retryable: true,
        failedMatchIds: matches.map((m) => m.matchId),
      };
      throw error;
    }

    // Extract block number and gas used
    const blockNumber = Number(receipt.blockNumber);
    const gasUsed = Number(receipt.gasUsed);

    // Get block timestamp
    const block = await publicClient.getBlock({ blockNumber: receipt.blockNumber });
    const timestamp = Number(block.timestamp) * 1000; // Convert to milliseconds

    const { bondTokenEvents, lendPositionEvents, borrowPositionEvents } =
      parseReceiptLogs(receipt.logs);

    logger.info(
      {
        component: 'smart-contract',
        transactionHash: receipt.transactionHash,
        blockNumber,
        gasUsed,
        timestamp,
        matchIds: matches.map((m) => m.matchId),
        bondTokenEvents: bondTokenEvents.length,
        lendPositionEvents: lendPositionEvents.length,
        borrowPositionEvents: borrowPositionEvents.length,
      },
      'Settlement transaction mined',
    );

    return {
      transactionHash: receipt.transactionHash,
      blockHash: receipt.blockHash,
      blockNumber,
      gasUsed,
      timestamp,
      settledMatchIds: matches.map((m) => m.matchId),
      bondTokenEvents,
      lendPositionEvents,
      borrowPositionEvents,
      receipt,
    };
  } catch (error) {
    // Handle nonce failure before mapping the error
    if (nonceManager) {
      await nonceManager.handleFailure(error);
    }

    const settlementError = mapContractError(
      error,
      matches.map((m) => m.matchId),
    );

    logger.error(
      {
        component: 'smart-contract',
        message: settlementError.message,
        code: settlementError.code,
        retryable: settlementError.retryable,
        failedMatchIds: settlementError.failedMatchIds,
      },
      'Settlement failed',
    );

    throw settlementError;
  }
};

/**
 * Fetch ERC20 token name and symbol from on-chain contract.
 */
export async function fetchErc20Metadata(
  config: AppConfig,
  tokenAddress: string,
): Promise<{ name: string; symbol: string }> {
  const client = getPublicClient(config);
  const [name, symbol] = await Promise.all([
    client.readContract({ address: tokenAddress as Address, abi: erc20MetadataAbi, functionName: 'name' }),
    client.readContract({ address: tokenAddress as Address, abi: erc20MetadataAbi, functionName: 'symbol' }),
  ]);
  return { name, symbol };
}

