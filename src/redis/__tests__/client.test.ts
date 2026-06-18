import { createIsolatedTestConfig } from '../../tests/helpers/testConfig';
import type { AppConfig } from '../../config';

/**
 * Unit tests for Redis client singleton lifecycle.
 * Uses jest.resetModules() to clear the module-level singleton between tests.
 *
 * @requires Redis server running (default: localhost:6379, or set REDIS_TEST_URL)
 */
describe('Redis client', () => {
  let config: AppConfig;

  beforeEach(() => {
    jest.resetModules();
    config = createIsolatedTestConfig();
  });

  afterEach(async () => {
    // Clean up any active clients
    try {
      const { closeRedisClient } = require('../client');
      await closeRedisClient();
    } catch {
      // Ignore
    }
  });

  describe('getRedisClient', () => {
    it('should return the same instance on subsequent calls', () => {
      const { getRedisClient } = require('../client');
      const client1 = getRedisClient(config);
      const client2 = getRedisClient(config);

      expect(client1).toBe(client2);

      client1.disconnect();
    });

    it('should create a new instance after the previous one is closed', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');
      const client1 = getRedisClient(config);

      await closeRedisClient();

      const client2 = getRedisClient(config);

      expect(client2).not.toBe(client1);

      client2.disconnect();
    });

    it('should create a new instance when client status is not connected', async () => {
      const { getRedisClient } = require('../client');
      const client1 = getRedisClient(config);

      // Force disconnect and wait for status to settle
      client1.disconnect();
      await new Promise((resolve) => {
        client1.once('end', resolve);
        // Fallback timeout in case 'end' already fired
        setTimeout(resolve, 200);
      });

      const client2 = getRedisClient(config);
      expect(client2).not.toBe(client1);

      client2.disconnect();
    });
  });

  describe('closeRedisClient', () => {
    it('should quit a connected client', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');
      const client = getRedisClient(config);

      // Wait for ready
      await new Promise<void>((resolve, reject) => {
        client.on('ready', resolve);
        client.on('error', reject);
        // If already ready
        if (client.status === 'ready') resolve();
      });

      await closeRedisClient();

      // Singleton should be null — next call creates new instance
      const client2 = getRedisClient(config);
      expect(client2).not.toBe(client);

      client2.disconnect();
    });

    it('should be a no-op when no client exists', async () => {
      const { closeRedisClient } = require('../client');
      // Should not throw
      await expect(closeRedisClient()).resolves.not.toThrow();
    });

    it('should disconnect a non-connected client', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');
      const client = getRedisClient(config);

      // Force to a non-connected state
      client.disconnect();

      // closeRedisClient should handle this without throwing
      await expect(closeRedisClient()).resolves.not.toThrow();
    });
  });

  describe('createIsolatedRedisClient', () => {
    it('should return a new instance each call', () => {
      const { createIsolatedRedisClient } = require('../client');
      const client1 = createIsolatedRedisClient(config);
      const client2 = createIsolatedRedisClient(config);

      expect(client1).not.toBe(client2);

      client1.disconnect();
      client2.disconnect();
    });

    it('should not affect the singleton', () => {
      const { getRedisClient, createIsolatedRedisClient } = require('../client');
      const singleton = getRedisClient(config);
      const isolated = createIsolatedRedisClient(config);

      expect(isolated).not.toBe(singleton);

      // Singleton should still be the same
      const singleton2 = getRedisClient(config);
      expect(singleton2).toBe(singleton);

      singleton.disconnect();
      isolated.disconnect();
    });
  });
});
