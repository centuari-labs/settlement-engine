/**
 * Event ABI definitions for parsing settlement transaction receipt logs.
 * Sourced from Centuari, CentuariBondERC20Factory, and BalanceLedger
 * contracts. Full ABIs are synced from smart-contract-revamp via
 * bin/sync-to-services.sh — see src/abi/*.json.
 */

import type { AbiEvent } from 'viem';
import BalanceLedger from '../abi/BalanceLedger.json';
import Centuari from '../abi/Centuari.json';
import BondFactory from '../abi/CentuariBondERC20Factory.json';

type AbiEntry = { readonly type: string; readonly name?: string };

const findEvent = (abi: readonly AbiEntry[], name: string): AbiEvent => {
  const event = abi.find((e) => e.type === 'event' && e.name === name);
  if (!event) {
    throw new Error(
      `Event "${name}" not found in synced ABI — sync-to-services.sh may be out of date.`,
    );
  }
  return event as unknown as AbiEvent;
};

export const BOND_TOKEN_CREATED_EVENT = findEvent(BondFactory, 'BondTokenCreated');
export const LEND_POSITION_CREATED_EVENT = findEvent(Centuari, 'LendPositionCreated');
export const BORROW_POSITION_CREATED_EVENT = findEvent(Centuari, 'BorrowPositionCreated');
export const COLLATERAL_FLAG_SET_EVENT = findEvent(BalanceLedger, 'CollateralFlagSet');

export const SETTLEMENT_EVENT_ABIS = [
  BOND_TOKEN_CREATED_EVENT,
  LEND_POSITION_CREATED_EVENT,
  BORROW_POSITION_CREATED_EVENT,
  COLLATERAL_FLAG_SET_EVENT,
] as const;
