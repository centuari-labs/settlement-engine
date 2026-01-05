import type Redis from 'ioredis';

/**
 * Removes all pending messages from a Redis stream consumer group.
 * This helps ensure test isolation by clearing any pending entries
 * from previous test runs that may cause test failures.
 *
 * @param redis - The Redis client.
 * @param stream - Stream name.
 * @param consumerGroup - Consumer group name.
 */
export const removePendingMessages = async (
  redis: Redis,
  stream: string,
  consumerGroup: string,
): Promise<void> => {
  try {
    // Get pending entry summary
    // XPENDING returns: [total, start, end, consumers]
    const pendingInfo = (await (redis.xpending as unknown as (
      ...args: (string | number)[]
    ) => Promise<[number, string, string, Array<[string, string]>] | null>)(
      stream,
      consumerGroup,
    )) as [number, string, string, Array<[string, string]>] | null;

    if (!pendingInfo || !Array.isArray(pendingInfo) || pendingInfo.length < 1) {
      return;
    }

    const totalPending = pendingInfo[0] as number;
    if (totalPending === 0) {
      return;
    }

    // Get all pending entries
    // XPENDING stream group - + count returns: [[id, consumer, idle, deliveries], ...]
    const pendingDetails = (await (redis.xpending as unknown as (
      ...args: (string | number)[]
    ) => Promise<Array<[string, string, number, number]>>)(
      stream,
      consumerGroup,
      '-',
      '+',
      totalPending,
    )) as Array<[string, string, number, number]>;

    if (!pendingDetails || pendingDetails.length === 0) {
      return;
    }

    // ACK all pending entries to remove them from the PEL
    const entryIds = pendingDetails.map((entry) => entry[0]);
    if (entryIds.length > 0) {
      await redis.xack(stream, consumerGroup, ...entryIds);
    }
  } catch (error) {
    // Ignore errors if stream/group doesn't exist or has no pending entries
    // This is expected in some test scenarios
  }
};

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

