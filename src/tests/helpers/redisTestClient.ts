import type Redis from 'ioredis';

/**
 * Cleans up all streams and keys used in tests.
 * This helps ensure test isolation.
 *
 * @param redis - The Redis client.
 * @param streams - Array of stream names to clean up.
 */
export const cleanupTestStreams = async (
  redis: Redis,
  streams: readonly string[],
): Promise<void> => {
  try {
    for (const stream of streams) {
      await redis.del(stream);
      // Try to get consumer groups and clean them up
      try {
        const groups = await redis.xinfo('GROUPS', stream);
        if (Array.isArray(groups)) {
          // Groups info is returned as nested arrays, extract group names
          for (const groupInfo of groups) {
            if (Array.isArray(groupInfo)) {
              // Find the 'name' field in the group info array
              for (let i = 0; i < groupInfo.length; i += 2) {
                if (groupInfo[i] === 'name' && groupInfo[i + 1]) {
                  const groupName = groupInfo[i + 1] as string;
                  // Delete consumer group
                  try {
                    await redis.xgroup('DESTROY', stream, groupName);
                  } catch {
                    // Ignore errors when destroying groups
                  }
                }
              }
            }
          }
        }
      } catch {
        // Ignore errors if stream doesn't exist or has no groups
      }
    }
  } catch (error) {
    // Ignore cleanup errors
  }
};

