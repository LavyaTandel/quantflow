import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';
import { describe, expect, it } from 'vitest';
import {
  buildChecksumString,
  crc32,
  type ChecksumLevel,
} from '../../../src/ingest/kraken/crc32.js';

function docVector(): {
  bids: ChecksumLevel[];
  asks: ChecksumLevel[];
  expected: string;
  checksum: number;
} {
  const fixturePath = fileURLToPath(
    new URL('../../fixtures/kraken/book-two-symbols.jsonl', import.meta.url),
  );
  const firstLine = readFileSync(fixturePath, 'utf8').split('\n')[0]!;
  const frame = JSON.parse(firstLine) as {
    data: Array<{ bids: ChecksumLevel[]; asks: ChecksumLevel[]; checksum: number }>;
  };
  const { bids, asks, checksum } = frame.data[0]!;
  return {
    bids,
    asks,
    expected:
      '45285210000045286415457195345286615457110945289615456091145290215890660452918154553491452947445474945296135380000452975994554245299518772827452835100000004528341545820154528211000000045281010000000452803154592586452790799000045277633101034527753000000045277315460273745276615445238',
    checksum,
  };
}

describe('crc32 (IEEE 802.3)', () => {
  it('matches the standard known-answer vectors', () => {
    expect(crc32('')).toBe(0x00000000);
    expect(crc32('123456789')).toBe(0xcbf43926);
    expect(crc32('The quick brown fox jumps over the lazy dog')).toBe(0x414fa339);
  });

  it('agrees with node:zlib on assorted unicode-free inputs', () => {
    const inputs = [
      'a',
      'abc',
      '0123456789',
      '452852100000452864154571953',
      buildChecksumString([{ price: '100.0', qty: '1' }], [{ price: '101.0', qty: '2' }]),
    ];
    for (const input of inputs) {
      expect(crc32(input)).toBe(zlib.crc32(Buffer.from(input, 'utf8')));
    }
  });
});

describe('buildChecksumString', () => {
  it('reproduces the Kraken docs golden example end to end', () => {
    const vector = docVector();
    expect(buildChecksumString(vector.bids, vector.asks)).toBe(vector.expected);
    expect(crc32(buildChecksumString(vector.bids, vector.asks))).toBe(vector.checksum);
    expect(vector.checksum).toBe(3310070434);
  });

  it('strips decimal points and leading zeros from prices and quantities', () => {
    expect(buildChecksumString([], [{ price: '0.00100000', qty: '0.07990000' }])).toBe(
      '1000007990000',
    );
    expect(buildChecksumString([], [{ price: '45281.0', qty: '0.10000000' }])).toBe(
      '45281010000000',
    );
    expect(buildChecksumString([], [{ price: '123456.789', qty: '1.50' }])).toBe('123456789150');
    expect(buildChecksumString([], [{ price: '0.5', qty: '0.25' }])).toBe('525');
  });

  it('accepts numeric levels identically to their canonical strings', () => {
    expect(buildChecksumString([], [{ price: 45285.2, qty: 0.001 }])).toBe(
      buildChecksumString([], [{ price: '45285.2', qty: '0.001' }]),
    );
    expect(buildChecksumString([], [{ price: 2451, qty: 2 }])).toBe('24512');
  });

  it('sorts asks low-to-high and bids high-to-low regardless of input order', () => {
    const asks = [
      { price: '103', qty: '1' },
      { price: '101', qty: '2' },
      { price: '102', qty: '3' },
    ];
    const bids = [
      { price: '98', qty: '4' },
      { price: '100', qty: '5' },
      { price: '99', qty: '6' },
    ];
    expect(buildChecksumString(bids, asks)).toBe('1012102310311005996984');
  });

  it('always truncates to the top 10 levels per side', () => {
    const asks = Array.from({ length: 15 }, (_, i) => ({ price: String(500 + i), qty: '1' }));
    const bids = Array.from({ length: 12 }, (_, i) => ({ price: String(400 - i), qty: '2' }));
    const top10Asks = asks
      .slice(0, 10)
      .map((l) => `${l.price}1`)
      .join('');
    const top10Bids = bids
      .slice(0, 10)
      .map((l) => `${l.price}2`)
      .join('');
    expect(buildChecksumString(bids, asks)).toBe(top10Asks + top10Bids);
  });

  it('handles books with fewer than 10 levels', () => {
    expect(buildChecksumString([{ price: '99', qty: '1' }], [])).toBe('991');
    expect(buildChecksumString([], [])).toBe('');
  });
});
