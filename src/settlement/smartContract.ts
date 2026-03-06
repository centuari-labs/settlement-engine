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
} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { loadConfig, type AppConfig } from '../config';
import type { Match } from '../schemas/match';

/**
 * Result of a smart contract settlement batch call.
 */
export interface SettlementResult {
  /**
   * Transaction hash of the settlement transaction.
   */
  readonly transactionHash: string;
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
}

/**
 * Contract ABI for the settlement contract.
 */
const SETTLEMENT_CONTRACT_ABI = [
  {
    type: 'function',
    name: 'settleMatches',
    inputs: [
      {
        name: 'matches',
        type: 'tuple[]',
        components: [
          { name: 'matchId', type: 'bytes32' },
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
] as const;

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
 * Transforms a Match object to the contract's MatchData struct format.
 *
 * @param match - Match object to transform.
 * @returns MatchData struct in the format expected by the contract.
 */
const transformMatchToContractFormat = (match: Match) => {
  return {
    matchId: uuidToBytes32(match.matchId),
    lendOrderId: uuidToBytes32(match.lendOrderId),
    borrowOrderId: uuidToBytes32(match.borrowOrderId),
    lender: match.lenderWallet as Address,
    borrower: match.borrowerWallet as Address,
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
  };
};

/**
 * Maps contract error to SettlementError with appropriate retryability.
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

  // Check for specific contract errors
  if (errorMessage.includes('AlreadySettled')) {
    return {
      message: 'Match already settled',
      code: 'ALREADY_SETTLED',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  if (errorMessage.includes('ContractPaused')) {
    return {
      message: 'Contract is paused',
      code: 'CONTRACT_PAUSED',
      retryable: true,
      failedMatchIds: matchIds,
    };
  }

  if (errorMessage.includes('EmptyBatch')) {
    return {
      message: 'Cannot settle empty batch',
      code: 'EMPTY_BATCH',
      retryable: false,
      failedMatchIds: matchIds,
    };
  }

  if (errorMessage.includes('InvalidMatchData')) {
    return {
      message: 'Invalid match data',
      code: 'INVALID_MATCH_DATA',
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
  // For common chains, we can use predefined chains, but for flexibility,
  // we'll create a custom chain. This works for most EVM-compatible chains.
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
  });
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

  // Normalize private key (remove 0x prefix if present, then add it back for viem)
  const privateKey = config.settlementPrivateKey.startsWith('0x')
    ? (config.settlementPrivateKey as `0x${string}`)
    : (`0x${config.settlementPrivateKey}` as `0x${string}`);

  // Create account from private key
  const account = privateKeyToAccount(privateKey);

  // Create chain configuration
  const chain = createChainFromId(config.ethereumChainId);

  // Create clients
  const publicClient = createPublicClient({
    chain,
    transport: http(config.ethereumRpcUrl),
  });

  const walletClient = createWalletClient({
    account,
    chain,
    transport: http(config.ethereumRpcUrl),
  });

  // Transform matches to contract format
  const contractMatches = matches.map(transformMatchToContractFormat);

  // eslint-disable-next-line no-console
  console.log(
    `[smart-contract] Settling batch of ${matches.length} matches`,
    matches.map((m) => m.matchId),
  );

  try {
    // Call the settleMatches function
    const hash = await walletClient.writeContract({
      address: config.settlementContractAddress as Address,
      abi: SETTLEMENT_CONTRACT_ABI,
      functionName: 'settleMatches',
      args: [contractMatches],
      account,
    });

    // Wait for transaction receipt
    const receipt = await publicClient.waitForTransactionReceipt({ hash });

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

    return {
      transactionHash: receipt.transactionHash,
      blockNumber,
      gasUsed,
      timestamp,
      settledMatchIds: matches.map((m) => m.matchId),
    };
  } catch (error) {
    const settlementError = mapContractError(
      error,
      matches.map((m) => m.matchId),
    );
    throw settlementError;
  }
};

