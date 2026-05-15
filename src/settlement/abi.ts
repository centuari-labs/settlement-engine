/**
 * Contract ABI definitions for the settlement engine.
 *
 * SETTLEMENT_CONTRACT_ABI is the full ABI synced from smart-contract-revamp
 * via bin/sync-to-services.sh. Do not hand-edit src/abi/Settlement.json —
 * regenerate by re-running the sync script.
 */

import type { Abi } from 'viem';
import Settlement from '../abi/Settlement.json';

export const SETTLEMENT_CONTRACT_ABI = Settlement as Abi;

export const erc20MetadataAbi = [
  { type: 'function', name: 'name', inputs: [], outputs: [{ type: 'string' }], stateMutability: 'view' },
  { type: 'function', name: 'symbol', inputs: [], outputs: [{ type: 'string' }], stateMutability: 'view' },
] as const;
