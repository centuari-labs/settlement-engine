import type Redis from 'ioredis';
import {
  REDIS_STREAMS,
  SETTLEMENT_MATCHES_DEAD_MAXLEN,
} from '../schemas/match';
import type { ReadMatchesOptions } from './settlementMatchConsumer';
import { logger } from '../logger';

/**
 * Build the `onInvalid` handler that dead-letters schema-invalid stream
 * entries before they are ACKed off the live stream (L5).
 *
 * Invalid entries used to be logged then immediately ACK-dropped, losing them
 * forever. They are now XADDed to a bounded dead-letter stream with the raw
 * payload + error so they remain inspectable. The consumer still ACKs the
 * entry afterwards, so a poison entry never blocks the live stream.
 *
 * The handler never throws: if dead-lettering itself fails it logs and
 * returns, so the subsequent ACK in the consumer always runs.
 */
export const createInvalidEntryHandler =
  (redis: Redis): NonNullable<ReadMatchesOptions['onInvalid']> =>
  async ({ id, stream, raw, error }): Promise<void> => {
    const message = error instanceof Error ? error.message : String(error);

    try {
      await redis.xadd(
        REDIS_STREAMS.SETTLEMENT_MATCHES_DEAD,
        'MAXLEN',
        '~',
        SETTLEMENT_MATCHES_DEAD_MAXLEN,
        '*',
        'id',
        id,
        'stream',
        stream,
        'raw',
        JSON.stringify(raw),
        'error',
        message,
        'ts',
        new Date().toISOString(),
      );
      logger.warn(
        {
          component: 'settlement-engine',
          deadStream: REDIS_STREAMS.SETTLEMENT_MATCHES_DEAD,
          stream,
          id,
        },
        'Dead-lettered invalid match entry',
      );
    } catch (deadLetterError) {
      // Never let dead-lettering throw — that would prevent the ACK and block
      // the live stream on a poison entry. Log and move on.
      logger.error(
        { component: 'settlement-engine', stream, id, err: deadLetterError },
        'Failed to dead-letter invalid match entry',
      );
    }
  };
