/**
 * Unit tests for redis/client.ts.
 *
 * Mocks ioredis to test singleton behaviour, reconnection, and cleanup
 * without requiring a live Redis server.
 */

// Store instances for inspection
const mockInstances: Array<{
  status: string;
  quit: jest.Mock;
  disconnect: jest.Mock;
}> = [];

jest.mock('ioredis', () => {
  return jest.fn().mockImplementation(() => {
    const instance = {
      status: 'ready',
      quit: jest.fn().mockResolvedValue('OK'),
      disconnect: jest.fn(),
    };
    mockInstances.push(instance);
    return instance;
  });
});

import { createTestConfig } from '../../tests/helpers/testConfig';

// Use requireActual to get the real client module (not the global auto-mock if any)
const clientModule = jest.requireActual('../client') as typeof import('../client');

describe('redis/client', () => {
  const config = createTestConfig();

  beforeEach(() => {
    jest.clearAllMocks();
    mockInstances.length = 0;
  });

  afterEach(async () => {
    // Clean up singleton between tests
    await clientModule.closeRedisClient();
    mockInstances.length = 0;
  });

  describe('getRedisClient', () => {
    it('should create a new Redis client on first call', () => {
      const client = clientModule.getRedisClient(config);
      expect(client).toBeDefined();
      expect(mockInstances.length).toBe(1);
    });

    it('should return the same instance on subsequent calls', () => {
      const client1 = clientModule.getRedisClient(config);
      const client2 = clientModule.getRedisClient(config);
      expect(client1).toBe(client2);
      expect(mockInstances.length).toBe(1);
    });

    it('should create a new client if existing client is disconnected', () => {
      const client1 = clientModule.getRedisClient(config);
      // Simulate client being closed
      (client1 as unknown as { status: string }).status = 'end';

      const client2 = clientModule.getRedisClient(config);
      expect(client2).not.toBe(client1);
      expect(mockInstances.length).toBe(2);
      // Old client should have been disconnected
      expect(mockInstances[0].disconnect).toHaveBeenCalled();
    });

    it('should keep existing client if status is connecting', () => {
      const client1 = clientModule.getRedisClient(config);
      (client1 as unknown as { status: string }).status = 'connecting';

      const client2 = clientModule.getRedisClient(config);
      expect(client2).toBe(client1);
      expect(mockInstances.length).toBe(1);
    });
  });

  describe('closeRedisClient', () => {
    it('should quit a connected client', async () => {
      const client = clientModule.getRedisClient(config);
      await clientModule.closeRedisClient();
      expect((client as unknown as { quit: jest.Mock }).quit).toHaveBeenCalled();
    });

    it('should disconnect a non-ready client', async () => {
      const client = clientModule.getRedisClient(config);
      (client as unknown as { status: string }).status = 'end';

      await clientModule.closeRedisClient();
      expect(
        (client as unknown as { disconnect: jest.Mock }).disconnect,
      ).toHaveBeenCalled();
    });

    it('should be safe to call multiple times', async () => {
      clientModule.getRedisClient(config);
      await clientModule.closeRedisClient();
      await clientModule.closeRedisClient(); // Should not throw
    });

    it('should be safe when no client exists', async () => {
      await clientModule.closeRedisClient(); // no client created yet
    });

    it('should suppress errors during quit', async () => {
      const client = clientModule.getRedisClient(config);
      (client as unknown as { quit: jest.Mock }).quit.mockRejectedValue(
        new Error('quit error'),
      );

      // Should not throw
      await expect(clientModule.closeRedisClient()).resolves.toBeUndefined();
    });
  });

  describe('createIsolatedRedisClient', () => {
    it('should create a new client instance each time', () => {
      const client1 = clientModule.createIsolatedRedisClient(config);
      const client2 = clientModule.createIsolatedRedisClient(config);
      expect(client1).not.toBe(client2);
      expect(mockInstances.length).toBe(2);
    });

    it('should not affect the singleton', () => {
      const singleton = clientModule.getRedisClient(config);
      const isolated = clientModule.createIsolatedRedisClient(config);
      expect(isolated).not.toBe(singleton);

      // Singleton should still work
      const sameSingleton = clientModule.getRedisClient(config);
      expect(sameSingleton).toBe(singleton);
    });
  });
});
