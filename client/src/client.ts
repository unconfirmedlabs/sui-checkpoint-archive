import { ClientCache, expandTilde } from "./cache.ts";
import { parseIdxFile } from "./binary.ts";
import { HttpError, NotIndexedError } from "./errors.ts";
import type {
  CacheStats,
  CheckpointResult,
  ClientConfig,
  EpochMetadata,
  HealthResponse,
} from "./types.ts";

const DEFAULT_BASE_URL = "https://checkpoints.mainnet.sui.unconfirmed.cloud";
const DEFAULT_ARCHIVE_URL = "https://archive.checkpoints.mainnet.sui.unconfirmed.cloud";
const DEFAULT_ROUTING_TTL_SEC = 3600;

/**
 * Canonical chunk size for the proxy /chunks endpoint. Every client
 * MUST fetch in multiples of this size aligned on this boundary so
 * cache entries are shared across clients and across PoPs. Changing
 * this breaks cache sharing with every other client that uses a
 * different value.
 */
const CHUNK_BYTES = 16 * 1024 * 1024;

/**
 * Two transports in play:
 *
 *   - `baseUrl`    → Worker proxy. Edge-cached via Cache API. Best for
 *                    per-checkpoint lookups where cache-friendliness matters.
 *                    Used by `getCheckpoint`, `getEpoch`, `listEpochs`, `health`.
 *
 *   - `archiveUrl` → R2 public bucket custom domain. Not Cache-API-cached
 *                    (responses are too large and range reads fragment the
 *                    cache namespace anyway). Best for bulk work: whole-epoch
 *                    downloads, idx prefetch for offline iteration.
 *                    Used by `getEpochArchive`, `getEpochIdx`.
 *
 * The SDK never tries to fetch a single checkpoint via R2 direct. Per-ckpt
 * access goes through the proxy because the edge cache is the entire
 * performance story at warm steady state.
 */

export class SuiArchiveClient {
  readonly baseUrl: string;
  readonly archiveUrl: string;
  readonly routingTtlMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly cache: ClientCache;
  private routingRefresh: Promise<void> | null = null;
  private idxRefresh: Map<number, Promise<Uint8Array>> = new Map();

  constructor(config: ClientConfig = {}) {
    this.baseUrl = (config.baseUrl ?? DEFAULT_BASE_URL).replace(/\/$/, "");
    this.archiveUrl = (config.archiveUrl ?? DEFAULT_ARCHIVE_URL).replace(/\/$/, "");
    this.routingTtlMs = (config.routingTtlSec ?? DEFAULT_ROUTING_TTL_SEC) * 1000;
    this.fetchImpl = config.fetch ?? fetch;
    const dir = config.cacheDir ? expandTilde(config.cacheDir) : null;
    this.cache = new ClientCache(dir);
  }

  // ── Public API ─────────────────────────────────────────────────────────

  async findEpochByNumber(epoch: number): Promise<EpochMetadata | null> {
    const epochs = await this.getRouting();
    // epochs is sorted by epoch asc, 0-indexed-contiguous in practice.
    // Do a defensive binary search rather than assume index == epoch.
    let lo = 0;
    let hi = epochs.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const row = epochs[mid]!;
      if (row.epoch === epoch) return row;
      if (row.epoch < epoch) lo = mid + 1;
      else hi = mid - 1;
    }
    return null;
  }

  async findEpoch(seq: number | bigint): Promise<EpochMetadata | null> {
    const s = typeof seq === "bigint" ? Number(seq) : seq;
    const epochs = await this.getRouting();
    // Binary search for the greatest first_seq <= s.
    let lo = 0;
    let hi = epochs.length - 1;
    let hit: EpochMetadata | null = null;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      const row = epochs[mid]!;
      if (row.first_seq <= s) {
        hit = row;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (!hit) return null;
    if (hit.last_seq < s) return null;
    return hit;
  }

  async getCheckpoint(seq: number | bigint): Promise<CheckpointResult> {
    const s = typeof seq === "bigint" ? Number(seq) : seq;
    const epoch = await this.findEpoch(s);
    if (!epoch) throw new NotIndexedError(s);

    const url = `${this.baseUrl}/${s}.binpb.zst`;
    const resp = await this.fetchImpl(url);
    if (!resp.ok) throw new HttpError(resp.status, url, await resp.text());
    const bytes = new Uint8Array(await resp.arrayBuffer());
    return { seq: s, epoch: epoch.epoch, bytes };
  }

  /**
   * Stream the full `epoch-{N}.zst` archive directly from R2. Returns the
   * raw `Response` so callers can decide how to consume it (stream to disk,
   * pipe to zstd, read into memory, etc.). Bypasses the proxy; not
   * edge-cached via Cache API.
   *
   * For retrieving many sequential checkpoints, download the whole epoch
   * with this method, then iterate locally via `getEpochIdx` for offset
   * resolution.
   */
  async getEpochArchive(
    epoch: number,
    opts: { range?: { start: number; end?: number } } = {},
  ): Promise<Response> {
    const url = `${this.archiveUrl}/epoch-${epoch}.zst`;
    const headers: Record<string, string> = {};
    if (opts.range) {
      headers.range = `bytes=${opts.range.start}-${opts.range.end ?? ""}`;
    }
    const resp = await this.fetchImpl(url, { headers });
    if (!resp.ok && resp.status !== 206) {
      throw new HttpError(resp.status, url, await resp.text().catch(() => ""));
    }
    return resp;
  }

  /**
   * Stream one epoch's checkpoints as they decompress, in seq order, using
   * bounded memory regardless of epoch size.
   *
   * Protocol: all clients fetch fixed-size 16 MiB chunks via the proxy at
   *   `/epochs/:N/chunks/:idx`
   * This URL is the stable cache key: every client using this SDK hits the
   * same URLs, so CF edge cache is shared across clients and across PoPs.
   * First client in a region pays R2's ~300 MB/s cap; every subsequent
   * client in that region serves from edge cache at multi-GB/s.
   *
   * Chunks are raw byte slices of a multi-frame zstd stream. A chunk may
   * start and/or end mid-frame. The iterator maintains a sliding buffer
   * that spans at most two chunks so it can reassemble a frame that
   * crosses a chunk boundary before handing it to the zstd decoder.
   *
   * Memory ceiling: `concurrency * CHUNK_BYTES` plus one sliding buffer
   * of up to ~2 * CHUNK_BYTES. With defaults (8 * 16 MiB) that's roughly
   * 160 MiB resident, regardless of epoch size.
   *
   * Requires Bun (uses `Bun.zstdDecompressSync` for per-frame decode).
   */
  async *iterateEpoch(
    epoch: number,
    opts: { concurrency?: number; startSeq?: number; endSeq?: number } = {},
  ): AsyncIterable<{ seq: number; bytes: Uint8Array }> {
    const concurrency = Math.max(1, opts.concurrency ?? defaultConcurrency());

    const meta = await this.findEpochByNumber(epoch);
    if (!meta) throw new NotIndexedError(-1);

    const idxBytes = await this.getIdxBytes(meta);
    const epochSize = meta.zst_bytes;

    const allEntries = parseIdxFile(idxBytes);
    const entries = (opts.startSeq !== undefined || opts.endSeq !== undefined)
      ? allEntries.filter(e => {
          const seq = Number(e.seq);
          if (opts.startSeq !== undefined && seq < opts.startSeq) return false;
          if (opts.endSeq !== undefined && seq > opts.endSeq) return false;
          return true;
        })
      : allEntries;

    if (entries.length === 0) return;

    const fetchImpl = this.fetchImpl;
    const archive = this.archiveUrl;

    async function fetchChunk(idx: number): Promise<Uint8Array> {
      const start = idx * CHUNK_BYTES;
      const end = Math.min(start + CHUNK_BYTES, epochSize) - 1;
      const url = `${archive}/epoch-${epoch}.zst`;
      const resp = await fetchImpl(url, {
        headers: { range: `bytes=${start}-${end}` },
      });
      if (!resp.ok && resp.status !== 206) {
        throw new HttpError(resp.status, url, await resp.text().catch(() => ""));
      }
      return new Uint8Array(await resp.arrayBuffer());
    }

    // Start from the chunk containing the first needed entry's byte offset.
    const firstOffset = Number(entries[0]!.offset);
    const lastEntry = entries[entries.length - 1]!;
    const lastByte = Number(lastEntry.offset) + lastEntry.length;
    const startChunk = Math.floor(firstOffset / CHUNK_BYTES);
    const endChunk = Math.ceil(lastByte / CHUNK_BYTES);

    // Prefetch pool: keep up to `concurrency` chunk fetches in flight.
    const pool: Promise<Uint8Array>[] = [];
    let nextChunk = startChunk;
    const refill = () => {
      while (pool.length < concurrency && nextChunk < endChunk) {
        pool.push(fetchChunk(nextChunk++));
      }
    };
    refill();

    // Sliding buffer: the bytes we've received and not yet emitted past.
    // `windowStart` is the absolute byte offset of buffer[0] in the zst.
    let window: Uint8Array = new Uint8Array(new ArrayBuffer(0));
    let windowStart = startChunk * CHUNK_BYTES;

    // Ensure the sliding window covers bytes [windowStart, absEnd).
    const appendUntil = async (absEnd: number) => {
      while (windowStart + window.byteLength < absEnd) {
        if (pool.length === 0) throw new Error("ran out of chunks before frame end");
        const chunk = await pool.shift()!;
        refill();
        const merged = new Uint8Array(new ArrayBuffer(window.byteLength + chunk.byteLength));
        merged.set(window, 0);
        merged.set(chunk, window.byteLength);
        window = merged;
      }
    };

    for (const entry of entries) {
      const frameStart = Number(entry.offset);
      const frameEnd = frameStart + entry.length;
      await appendUntil(frameEnd);
      const localStart = frameStart - windowStart;
      const frame = window.subarray(localStart, localStart + entry.length);
      yield { seq: Number(entry.seq), bytes: decompressZstdFrame(frame) };

      // Drop bytes behind the current frame's end to keep memory bounded.
      // Align the drop to CHUNK_BYTES so we always retain the tail chunk
      // intact in case the next frame crosses into newly-fetched bytes.
      const consumed = frameEnd - windowStart;
      if (consumed >= CHUNK_BYTES) {
        const drop = consumed - (consumed % CHUNK_BYTES);
        window = window.subarray(drop);
        windowStart += drop;
      }
    }
  }

  /**
   * Fetch and cache the `.idx` for one epoch from R2. Useful for clients
   * that want to iterate checkpoints inside a locally-downloaded epoch
   * archive without hitting the network per checkpoint.
   */
  async getEpochIdx(epoch: number): Promise<Uint8Array> {
    const meta = await this.findEpochByNumber(epoch);
    if (!meta) throw new NotIndexedError(-1);
    return this.getIdxBytes(meta);
  }

  async getEpoch(epoch: number): Promise<EpochMetadata | null> {
    const url = `${this.baseUrl}/epochs/${epoch}`;
    const resp = await this.fetchImpl(url);
    if (resp.status === 404) return null;
    if (!resp.ok) throw new HttpError(resp.status, url, await resp.text());
    return (await resp.json()) as EpochMetadata;
  }

  async listEpochs(): Promise<EpochMetadata[]> {
    return this.getRouting();
  }

  async health(): Promise<HealthResponse> {
    const url = `${this.baseUrl}/health`;
    const resp = await this.fetchImpl(url);
    if (!resp.ok) throw new HttpError(resp.status, url, await resp.text());
    return (await resp.json()) as HealthResponse;
  }

  /**
   * Prefetch data into the cache. Refreshes the routing snapshot and
   * optionally prefetches per-epoch idx files from the R2 archive bucket.
   * Options:
   *   - epochs: list of epoch numbers, "all", or "recent"
   *   - recent: count used when epochs="recent"; defaults to 100
   *   - idxConcurrency: parallel idx downloads; defaults to 16
   */
  async warmup(
    opts: {
      epochs?: number[] | "all" | "recent";
      recent?: number;
      idxConcurrency?: number;
    } = {},
  ): Promise<void> {
    const routing = await this.refreshRouting();
    const target =
      opts.epochs === "all"
        ? routing.map((e) => e.epoch)
        : opts.epochs === "recent"
        ? routing.slice(-(opts.recent ?? 100)).map((e) => e.epoch)
        : Array.isArray(opts.epochs)
        ? opts.epochs
        : [];

    if (target.length === 0) return;

    const byEpoch = new Map(routing.map((e) => [e.epoch, e]));
    const conc = Math.max(1, opts.idxConcurrency ?? 16);
    let idx = 0;
    await Promise.all(
      Array.from({ length: conc }, async () => {
        while (true) {
          const i = idx++;
          if (i >= target.length) return;
          const n = target[i]!;
          const meta = byEpoch.get(n);
          if (!meta) continue;
          await this.getIdxBytes(meta);
        }
      }),
    );
  }

  async stats(): Promise<CacheStats> {
    const r = this.cache.getRouting();
    const disk = this.cache.diskStats() ?? undefined;
    return {
      epochs: r?.epochs.length ?? 0,
      idxCached: this.cache.memIdxCount(),
      routingAgeSec: r ? Math.floor((Date.now() - r.fetchedAt) / 1000) : null,
      disk,
    };
  }

  clearCache(): void {
    this.cache.clear();
  }

  // ── Internals ──────────────────────────────────────────────────────────

  private async getRouting(): Promise<EpochMetadata[]> {
    const r = this.cache.getRouting();
    const fresh = r && Date.now() - r.fetchedAt < this.routingTtlMs;
    if (fresh) return r!.epochs;
    return this.refreshRouting();
  }

  /**
   * Refresh routing snapshot. Concurrent callers share the in-flight request.
   */
  private async refreshRouting(): Promise<EpochMetadata[]> {
    if (this.routingRefresh) {
      await this.routingRefresh;
      return this.cache.getRouting()!.epochs;
    }
    const run = (async () => {
      const url = `${this.baseUrl}/epochs`;
      const resp = await this.fetchImpl(url);
      if (!resp.ok) throw new HttpError(resp.status, url, await resp.text());
      const body = (await resp.json()) as { epochs: EpochMetadata[] };
      const sorted = body.epochs.slice().sort((a, b) => a.epoch - b.epoch);
      this.cache.setRouting(sorted);
    })();
    this.routingRefresh = run;
    try {
      await run;
    } finally {
      this.routingRefresh = null;
    }
    return this.cache.getRouting()!.epochs;
  }

  /**
   * Load the `.idx` for an epoch, cache in memory + disk. Concurrent
   * callers share the in-flight request.
   */
  private async getIdxBytes(epoch: EpochMetadata): Promise<Uint8Array> {
    const cached = this.cache.getIdx(epoch.epoch);
    if (cached && cached.byteLength === epoch.idx_bytes) return cached;

    const inFlight = this.idxRefresh.get(epoch.epoch);
    if (inFlight) return inFlight;

    const run = (async () => {
      const url = `${this.archiveUrl}/${epoch.idx_key}`;
      const resp = await this.fetchImpl(url);
      if (!resp.ok) throw new HttpError(resp.status, url, await resp.text());
      const bytes = new Uint8Array(await resp.arrayBuffer());
      if (bytes.byteLength !== epoch.idx_bytes) {
        throw new Error(
          `idx size mismatch for epoch ${epoch.epoch}: got ${bytes.byteLength}, expected ${epoch.idx_bytes}`,
        );
      }
      this.cache.setIdx(epoch.epoch, bytes);
      return bytes;
    })();
    this.idxRefresh.set(epoch.epoch, run);
    try {
      return await run;
    } finally {
      this.idxRefresh.delete(epoch.epoch);
    }
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────

function defaultConcurrency(): number {
  const n = (globalThis as { navigator?: { hardwareConcurrency?: number } })
    .navigator?.hardwareConcurrency;
  return Math.min(n ?? 4, 16);
}

function decompressZstdFrame(frame: Uint8Array): Uint8Array {
  // Bun native libzstd. Throws if Bun isn't present or the frame is
  // malformed. The SDK requires Bun; no fallback by design.
  const g = globalThis as { Bun?: { zstdDecompressSync?: (b: Uint8Array) => Uint8Array } };
  const fn = g.Bun?.zstdDecompressSync;
  if (!fn) {
    throw new Error(
      "iterateEpoch requires Bun.zstdDecompressSync (Bun >= 1.1.5 or newer)",
    );
  }
  return fn(frame);
}
