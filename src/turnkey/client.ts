import { Turnkey } from '@turnkey/sdk-server';
import { createAccountWithAddress } from '@turnkey/viem';
import type { LocalAccount } from 'viem';
import type { AppConfig } from '../config';

let cachedClient: InstanceType<typeof Turnkey> | null = null;
let cachedClientKey: string | null = null;
let cachedAccount: LocalAccount | null = null;
let cachedAccountKey: string | null = null;

export const getTurnkeyClient = (config: AppConfig): InstanceType<typeof Turnkey> => {
  const key = `${config.turnkeyOrganizationId}|${config.turnkeyApiPublicKey}`;
  if (cachedClient && cachedClientKey === key) {
    return cachedClient;
  }
  cachedClient = new Turnkey({
    apiBaseUrl: 'https://api.turnkey.com',
    apiPublicKey: config.turnkeyApiPublicKey,
    apiPrivateKey: config.turnkeyApiPrivateKey,
    defaultOrganizationId: config.turnkeyOrganizationId,
  });
  cachedClientKey = key;
  return cachedClient;
};

export const getSettlementAccount = (config: AppConfig): LocalAccount => {
  const key = `${config.turnkeyOrganizationId}|${config.walletAddress}`;
  if (cachedAccount && cachedAccountKey === key) {
    return cachedAccount;
  }
  const turnkey = getTurnkeyClient(config);
  // Cast required: @turnkey/viem types reference sdk-server v4; we use v5.
  // The runtime API is compatible — TurnkeyApiClient has all required methods.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  cachedAccount = createAccountWithAddress({
    client: turnkey.apiClient() as any,
    organizationId: config.turnkeyOrganizationId,
    signWith: config.walletAddress,
    ethereumAddress: config.walletAddress,
  });
  cachedAccountKey = key;
  return cachedAccount;
};
