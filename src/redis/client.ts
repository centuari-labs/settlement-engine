import Redis from 'ioredis';
import type { AppConfig } from '../config';

let redisClient: Redis | null = null;

/**
 * Create or return a singleton Redis client instance.
 *
 * @param config - Application configuration.
 */
export const getRedisClient = (config: AppConfig): Redis => {
  if (!redisClient) {
    redisClient = new Redis(config.redisUrl);
  }
  return redisClient;
};

/**
 * Close the Redis client if it has been created.
 */
export const closeRedisClient = async (): Promise<void> => {
  if (redisClient) {
    await redisClient.quit();
    redisClient = null;
  }
};


