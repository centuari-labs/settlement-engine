import { BatchAccumulator } from '../batchAccumulator';
import { createMatchWithMeta } from '../../tests/helpers/testFixtures';

/**
 * Unit tests for BatchAccumulator.
 * Tests the accumulator's logic for collecting matches and determining when to process batches.
 */
describe('BatchAccumulator', () => {
  describe('constructor', () => {
    it('should initialize with batch size and interval', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      expect(accumulator.getPendingCount()).toBe(0);
    });
  });

  describe('addMatches', () => {
    it('should add matches to the queue', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      const match1 = createMatchWithMeta();
      const match2 = createMatchWithMeta();

      accumulator.addMatches([match1, match2]);

      expect(accumulator.getPendingCount()).toBe(2);
    });

    it('should add multiple batches of matches', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      const matches1 = [createMatchWithMeta(), createMatchWithMeta()];
      const matches2 = [createMatchWithMeta(), createMatchWithMeta()];

      accumulator.addMatches(matches1);
      accumulator.addMatches(matches2);

      expect(accumulator.getPendingCount()).toBe(4);
    });

    it('should handle empty array', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      accumulator.addMatches([]);
      expect(accumulator.getPendingCount()).toBe(0);
    });
  });

  describe('shouldProcess', () => {
    it('should return true when batch size threshold is reached', () => {
      const accumulator = new BatchAccumulator(3, 5000);
      const matches = [
        createMatchWithMeta(),
        createMatchWithMeta(),
        createMatchWithMeta(),
      ];

      accumulator.addMatches(matches);

      expect(accumulator.shouldProcess()).toBe(true);
    });

    it('should return true when batch size is exceeded', () => {
      const accumulator = new BatchAccumulator(2, 5000);
      const matches = [
        createMatchWithMeta(),
        createMatchWithMeta(),
        createMatchWithMeta(),
      ];

      accumulator.addMatches(matches);

      expect(accumulator.shouldProcess()).toBe(true);
    });

    it('should return true when time interval has elapsed with matches in queue', async () => {
      jest.useFakeTimers();
      const accumulator = new BatchAccumulator(10, 100);
      const match = createMatchWithMeta();

      accumulator.addMatches([match]);

      // Initially should not process
      expect(accumulator.shouldProcess()).toBe(false);

      // Advance time past the interval
      jest.advanceTimersByTime(100);

      expect(accumulator.shouldProcess()).toBe(true);

      jest.useRealTimers();
    });

    it('should return false when time interval has elapsed but queue is empty', async () => {
      jest.useFakeTimers();
      const accumulator = new BatchAccumulator(10, 100);

      // Advance time past the interval
      jest.advanceTimersByTime(100);

      expect(accumulator.shouldProcess()).toBe(false);

      jest.useRealTimers();
    });

    it('should return false when neither condition is met', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      const match = createMatchWithMeta();

      accumulator.addMatches([match]);

      expect(accumulator.shouldProcess()).toBe(false);
    });

    it('should return false when queue is empty', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      expect(accumulator.shouldProcess()).toBe(false);
    });
  });

  describe('needsMoreMatches', () => {
    it('should return true when queue length is less than batch size', () => {
      const accumulator = new BatchAccumulator(5, 5000);
      const matches = [createMatchWithMeta(), createMatchWithMeta()];

      accumulator.addMatches(matches);

      expect(accumulator.needsMoreMatches()).toBe(true);
    });

    it('should return false when queue length equals batch size', () => {
      const accumulator = new BatchAccumulator(3, 5000);
      const matches = [
        createMatchWithMeta(),
        createMatchWithMeta(),
        createMatchWithMeta(),
      ];

      accumulator.addMatches(matches);

      expect(accumulator.needsMoreMatches()).toBe(false);
    });

    it('should return false when queue length exceeds batch size', () => {
      const accumulator = new BatchAccumulator(2, 5000);
      const matches = [
        createMatchWithMeta(),
        createMatchWithMeta(),
        createMatchWithMeta(),
      ];

      accumulator.addMatches(matches);

      expect(accumulator.needsMoreMatches()).toBe(false);
    });

    it('should return true when queue is empty', () => {
      const accumulator = new BatchAccumulator(5, 5000);
      expect(accumulator.needsMoreMatches()).toBe(true);
    });
  });

  describe('getBatch', () => {
    it('should return all matches and clear the queue', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      const matches = [
        createMatchWithMeta(),
        createMatchWithMeta(),
        createMatchWithMeta(),
      ];

      accumulator.addMatches(matches);

      const batch = accumulator.getBatch();

      expect(batch).toHaveLength(3);
      expect(accumulator.getPendingCount()).toBe(0);
    });

    it('should return empty array when queue is empty', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      const batch = accumulator.getBatch();

      expect(batch).toHaveLength(0);
      expect(accumulator.getPendingCount()).toBe(0);
    });

    it('should reset timer after getting batch', async () => {
      jest.useFakeTimers();
      const accumulator = new BatchAccumulator(10, 100);
      const match = createMatchWithMeta();

      accumulator.addMatches([match]);

      // Advance time past interval
      jest.advanceTimersByTime(100);
      expect(accumulator.shouldProcess()).toBe(true);

      // Get batch (should reset timer)
      accumulator.getBatch();

      // Add another match and verify timer was reset
      accumulator.addMatches([match]);
      expect(accumulator.shouldProcess()).toBe(false);

      // Advance time again
      jest.advanceTimersByTime(100);
      expect(accumulator.shouldProcess()).toBe(true);

      jest.useRealTimers();
    });

    it('should return matches in the order they were added', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      const match1 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440001' });
      const match2 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440002' });
      const match3 = createMatchWithMeta({ matchId: '550e8400-e29b-41d4-a716-446655440003' });

      accumulator.addMatches([match1, match2, match3]);

      const batch = accumulator.getBatch();

      expect(batch[0]?.payload.matchId).toBe('550e8400-e29b-41d4-a716-446655440001');
      expect(batch[1]?.payload.matchId).toBe('550e8400-e29b-41d4-a716-446655440002');
      expect(batch[2]?.payload.matchId).toBe('550e8400-e29b-41d4-a716-446655440003');
    });
  });

  describe('getPendingCount', () => {
    it('should return correct queue length', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      expect(accumulator.getPendingCount()).toBe(0);

      accumulator.addMatches([createMatchWithMeta()]);
      expect(accumulator.getPendingCount()).toBe(1);

      accumulator.addMatches([createMatchWithMeta(), createMatchWithMeta()]);
      expect(accumulator.getPendingCount()).toBe(3);
    });

    it('should return 0 after getting batch', () => {
      const accumulator = new BatchAccumulator(10, 5000);
      accumulator.addMatches([createMatchWithMeta(), createMatchWithMeta()]);

      accumulator.getBatch();

      expect(accumulator.getPendingCount()).toBe(0);
    });
  });

  describe('resetTimer', () => {
    it('should update lastProcessedTime', async () => {
      jest.useFakeTimers();
      const accumulator = new BatchAccumulator(10, 100);
      const match = createMatchWithMeta();

      accumulator.addMatches([match]);

      // Advance time past interval
      jest.advanceTimersByTime(100);
      expect(accumulator.shouldProcess()).toBe(true);

      // Reset timer
      accumulator.resetTimer();

      // Add another match - should not process immediately
      accumulator.addMatches([match]);
      expect(accumulator.shouldProcess()).toBe(false);

      // Advance time again
      jest.advanceTimersByTime(100);
      expect(accumulator.shouldProcess()).toBe(true);

      jest.useRealTimers();
    });
  });
});

