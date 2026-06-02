/**
 * Tests for buildTransport — the RPC failover transport builder (Track D3).
 *
 * Uses REAL viem (no module mock) so we can inspect the constructed transport's
 * config.type. setup.ts globally mocks '../smartContract', so we unmock it.
 */

jest.unmock('../smartContract');

import { buildTransport } from '../smartContract';

describe('buildTransport (RPC failover — Track D3)', () => {
  it('returns a plain http transport for a single URL', () => {
    const transport = buildTransport(['https://primary.example.com']);
    // Instantiate the transport to read its viem config metadata.
    const { config } = transport({});
    expect(config.type).toBe('http');
  });

  it('returns a fallback transport when multiple URLs are provided', () => {
    const transport = buildTransport([
      'https://alchemy.example.com',
      'https://infura.example.com',
      'https://quicknode.example.com',
    ]);
    const { config } = transport({});
    expect(config.type).toBe('fallback');
  });

  it('throws when given an empty URL list', () => {
    expect(() => buildTransport([])).toThrow();
  });
});
