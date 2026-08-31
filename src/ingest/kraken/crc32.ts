// CRC32 (IEEE 802.3) + Kraken v2 book-checksum string builder.
// Spec: docs.kraken.com/exchange/guides/websockets/book-checksum-v2 — top 10 levels,
// asks sorted low->high then bids high->low, per level: strip "." and leading zeros
// from price digits and qty digits, concatenate asks part THEN bids part.

const IEEE_POLY = 0xedb88320;

const TABLE: readonly number[] = (() => {
  const table = new Array<number>(256);
  for (let n = 0; n < 256; n += 1) {
    let c = n;
    for (let k = 0; k < 8; k += 1) {
      c = (c & 1) !== 0 ? IEEE_POLY ^ (c >>> 1) : c >>> 1;
    }
    table[n] = c >>> 0;
  }
  return table;
})();

export function crc32(input: string): number {
  const bytes = Buffer.from(input, 'utf8');
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i += 1) {
    crc = (TABLE[(crc ^ bytes[i]!) & 0xff]! ^ (crc >>> 8)) >>> 0;
  }
  return (crc ^ 0xffffffff) >>> 0;
}

export interface ChecksumLevel {
  price: number | string;
  qty: number | string;
}

export const CHECKSUM_DEPTH = 10;

function digits(value: number | string): string {
  const raw = typeof value === 'number' ? value.toString() : value.trim();
  const stripped = raw.replace('.', '').replace(/^0+/, '');
  return stripped.length > 0 ? stripped : '0';
}

function priceValue(level: ChecksumLevel): number {
  return typeof level.price === 'number' ? level.price : Number(level.price);
}

function levelDigits(level: ChecksumLevel): string {
  return digits(level.price) + digits(level.qty);
}

export function buildChecksumString(
  bids: readonly ChecksumLevel[],
  asks: readonly ChecksumLevel[],
): string {
  const askPart = [...asks]
    .sort((a, b) => priceValue(a) - priceValue(b))
    .slice(0, CHECKSUM_DEPTH)
    .map(levelDigits)
    .join('');
  const bidPart = [...bids]
    .sort((a, b) => priceValue(b) - priceValue(a))
    .slice(0, CHECKSUM_DEPTH)
    .map(levelDigits)
    .join('');
  return askPart + bidPart;
}
