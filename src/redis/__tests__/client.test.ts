/**
 * Unit tests for Redis client module.
 * Mocks ioredis to avoid requiring a live Redis connection.
 */

// Mock ioredis before importing the module
jest.mock('ioredis', () => {
  const mockQuit = jest.fn().mockResolvedValue('OK');
  const mockDisconnect = jest.fn();

  const MockRedis = jest.fn().mockImplementation(() => ({
    status: 'ready',
    quit: mockQuit,
    disconnect: mockDisconnect,
  }));

  // Expose mocks for test assertions
  (MockRedis as any).__mockQuit = mockQuit;
  (MockRedis as any).__mockDisconnect = mockDisconnect;

  return {
    __esModule: true,
    default: MockRedis,
  };
});

import { createTestConfig } from '../../tests/helpers/testConfig';

describe('Redis client', () => {
  const config = createTestConfig();

  beforeEach(() => {
    jest.clearAllMocks();
    // Reset modules to clear the singleton between tests
    jest.resetModules();

    // Re-mock ioredis after resetModules
    jest.mock('ioredis', () => {
      const mockQuit = jest.fn().mockResolvedValue('OK');
      const mockDisconnect = jest.fn();

      const MockRedis = jest.fn().mockImplementation(() => ({
        status: 'ready',
        quit: mockQuit,
        disconnect: mockDisconnect,
      }));

      (MockRedis as any).__mockQuit = mockQuit;
      (MockRedis as any).__mockDisconnect = mockDisconnect;

      return {
        __esModule: true,
        default: MockRedis,
      };
    });
  });

  describe('getRedisClient', () => {
    it('should create a new Redis client', () => {
      const { getRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      const client = getRedisClient(config);

      expect(Redis).toHaveBeenCalledWith(config.redisUrl);
      expect(client).toBeDefined();
    });

    it('should return the same client on subsequent calls (singleton)', () => {
      const { getRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      const client1 = getRedisClient(config);
      const client2 = getRedisClient(config);

      expect(client1).toBe(client2);
      expect(Redis).toHaveBeenCalledTimes(1);
    });

    it('should create a new client if existing client is closed', () => {
      const { getRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      const client1 = getRedisClient(config);
      // Simulate closed connection
      client1.status = 'end';

      const client2 = getRedisClient(config);

      expect(client2).not.toBe(client1);
      expect(Redis).toHaveBeenCalledTimes(2);
    });

    it('should return existing client if status is connecting', () => {
      const { getRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      const client1 = getRedisClient(config);
      client1.status = 'connecting';

      const client2 = getRedisClient(config);

      expect(client1).toBe(client2);
      expect(Redis).toHaveBeenCalledTimes(1);
    });
  });

  describe('closeRedisClient', () => {
    it('should quit the client when connected', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');

      const client = getRedisClient(config);

      await closeRedisClient();

      expect(client.quit).toHaveBeenCalled();
    });

    it('should disconnect if client is not connected', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');

      const client = getRedisClient(config);
      client.status = 'end';

      await closeRedisClient();

      expect(client.disconnect).toHaveBeenCalled();
    });

    it('should be a no-op if no client exists', async () => {
      const { closeRedisClient } = require('../client');

      // Should not throw
      await expect(closeRedisClient()).resolves.not.toThrow();
    });

    it('should set singleton to null after close', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      getRedisClient(config);
      await closeRedisClient();

      // Next call should create a new client
      getRedisClient(config);
      expect(Redis).toHaveBeenCalledTimes(2);
    });

    it('should handle quit errors gracefully', async () => {
      const { getRedisClient, closeRedisClient } = require('../client');

      const client = getRedisClient(config);
      client.quit.mockRejectedValue(new Error('quit failed'));

      // Should not throw
      await expect(closeRedisClient()).resolves.not.toThrow();
    });
  });

  describe('createIsolatedRedisClient', () => {
    it('should create a new Redis client each time', () => {
      const { createIsolatedRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      const client1 = createIsolatedRedisClient(config);
      const client2 = createIsolatedRedisClient(config);

      // Each call should create a new instance
      expect(Redis).toHaveBeenCalledTimes(2);
      expect(client1).not.toBe(client2);
    });

    it('should not affect the singleton client', () => {
      const { getRedisClient, createIsolatedRedisClient } = require('../client');
      const Redis = require('ioredis').default;

      const singleton = getRedisClient(config);
      const isolated = createIsolatedRedisClient(config);

      // Singleton should still be the same
      const singleton2 = getRedisClient(config);
      expect(singleton).toBe(singleton2);

      // Isolated should be different
      expect(isolated).not.toBe(singleton);
    });
  });
});
