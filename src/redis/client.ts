import Redis from 'ioredis';
import type { AppConfig } from '../config';

let redisClient: Redis | null = null;

/**
 * Check if a Redis client is connected and ready to use.
 *
 * @param client - The Redis client to check.
 * @returns True if the client is connected, false otherwise.
 */
const isClientConnected = (client: Redis): boolean => {
  return client.status === 'ready' || client.status === 'connecting';
};

/**
 * Create or return a singleton Redis client instance.
 * If the existing client is closed, a new one will be created.
 *
 * @param config - Application configuration.
 * @returns A Redis client instance.
 */
export const getRedisClient = (config: AppConfig): Redis => {
  if (!redisClient || !isClientConnected(redisClient)) {
    if (redisClient) {
      // Client exists but is closed, clean it up
      redisClient.disconnect();
      redisClient = null;
    }
    redisClient = new Redis(config.redisUrl);
  }
  return redisClient;
};

/**
 * Close the Redis client if it has been created.
 * This sets the singleton to null so a new client will be created on next access.
 */
export const closeRedisClient = async (): Promise<void> => {
  if (redisClient) {
    try {
      if (isClientConnected(redisClient)) {
        await redisClient.quit();
      } else {
        redisClient.disconnect();
      }
    } catch {
      // Ignore errors during cleanup
    } finally {
      redisClient = null;
    }
  }
};

/**
 * Create a new isolated Redis client instance.
 * Unlike getRedisClient, this creates a new instance each time and does not use the singleton.
 * Useful for testing to avoid cross-test contamination.
 *
 * @param config - Application configuration.
 * @returns A new Redis client instance.
 */
export const createIsolatedRedisClient = (config: AppConfig): Redis => {
  return new Redis(config.redisUrl);
};


