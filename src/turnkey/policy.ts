import type { AppConfig } from '../config';
import { logger } from '../logger';
import { getTurnkeyClient } from './client';

const buildExpectedCondition = (contractAddress: string): string =>
  `eth.tx.to == '${contractAddress.toLowerCase()}'`;

export const ensureTurnkeyPolicy = async (config: AppConfig): Promise<void> => {
  const expectedCondition = buildExpectedCondition(config.settlementContractAddress);

  let apiClient: ReturnType<ReturnType<typeof getTurnkeyClient>['apiClient']>;
  try {
    const turnkey = getTurnkeyClient(config);
    apiClient = turnkey.apiClient();
  } catch (err) {
    logger.error({ component: 'turnkey-policy', err }, 'Turnkey policy verification failed');
    throw new Error('Turnkey policy verification failed');
  }

  let policies: Array<{ condition?: string }>;
  try {
    const response = await apiClient.getPolicies();
    policies = (response as { policies?: Array<{ condition?: string }> }).policies ?? [];
  } catch (err) {
    logger.error({ component: 'turnkey-policy', err }, 'Turnkey policy verification failed');
    throw new Error('Turnkey policy verification failed');
  }

  const exists = policies.some(
    (p) => (p.condition ?? '').trim().toLowerCase() === expectedCondition,
  );

  if (exists) {
    logger.info(
      { component: 'turnkey-policy', condition: expectedCondition },
      'Turnkey policy verified',
    );
    return;
  }

  try {
    await apiClient.createPolicy({
      policyName: `settlement-engine-allow-${config.settlementContractAddress.toLowerCase()}`,
      effect: 'EFFECT_ALLOW',
      condition: expectedCondition,
      notes: 'Auto-created by settlement-engine startup',
    });
    logger.info(
      { component: 'turnkey-policy', condition: expectedCondition },
      `Created Turnkey policy for contract ${config.settlementContractAddress}`,
    );
  } catch (err) {
    logger.error({ component: 'turnkey-policy', err }, 'Turnkey policy verification failed');
    throw new Error('Turnkey policy verification failed');
  }
};
