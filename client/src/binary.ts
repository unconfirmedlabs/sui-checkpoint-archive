/**
 * Binary layout of a single entry in an epoch's `.idx` side-car.
 *
 * Each entry is a fixed 20 bytes, little-endian:
 *
 *     u64 seq       (0..8)   absolute checkpoint sequence number
 *     u64 offset    (8..16)  byte offset into the epoch's `.zst`
 *     u32 length    (16..20) length in bytes of that checkpoint's zstd frame
 *
 * Entries are stored in strictly ascending seq order with no gaps. That
 * means the offset of seq S within epoch N is:
 *
 *     (S - first_seq(N)) * 20
 *
 * which is how `idxEntryOffset` below computes it.
 */

export const IDX_ENTRY_BYTES = 20;

export interface IdxEntry {
  seq: bigint;
  offset: bigint;
  length: number;
}

/** Parse one 20-byte idx entry from a buffer. */
export function parseIdxEntry(
  buf: ArrayBuffer | ArrayBufferView,
  byteOffset = 0,
): IdxEntry {
  const view =
    buf instanceof ArrayBuffer
      ? new DataView(buf, byteOffset, IDX_ENTRY_BYTES)
      : new DataView(buf.buffer, buf.byteOffset + byteOffset, IDX_ENTRY_BYTES);
  return {
    seq: view.getBigUint64(0, true),
    offset: view.getBigUint64(8, true),
    length: view.getUint32(16, true),
  };
}

/** Byte offset of the idx entry for `targetSeq` given the epoch's `firstSeq`. */
export function idxEntryOffset(
  firstSeq: number | bigint,
  targetSeq: number | bigint,
): number {
  const first = typeof firstSeq === "bigint" ? firstSeq : BigInt(firstSeq);
  const target = typeof targetSeq === "bigint" ? targetSeq : BigInt(targetSeq);
  if (target < first) {
    throw new RangeError(`targetSeq ${target} < firstSeq ${first}`);
  }
  return Number(target - first) * IDX_ENTRY_BYTES;
}

/** Parse an entire .idx file into an array of entries. */
export function parseIdxFile(buf: ArrayBuffer | Uint8Array): IdxEntry[] {
  const bytes = buf instanceof Uint8Array ? buf : new Uint8Array(buf);
  if (bytes.byteLength % IDX_ENTRY_BYTES !== 0) {
    throw new Error(
      `idx file size ${bytes.byteLength} is not a multiple of ${IDX_ENTRY_BYTES}`,
    );
  }
  const count = bytes.byteLength / IDX_ENTRY_BYTES;
  const entries: IdxEntry[] = new Array(count);
  for (let i = 0; i < count; i++) {
    entries[i] = parseIdxEntry(bytes, i * IDX_ENTRY_BYTES);
  }
  return entries;
}
