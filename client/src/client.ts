import { ClientCache, expandTilde } from "./cache.ts";
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
  private readonly verify: boolean;
  private readonly fetchImpl: typeof fetch;
  private readonly cache: ClientCache;
  private routingRefresh: Promise<void> | null = null;
  private idxRefresh: Map<number, Promise<Uint8Array>> = new Map();

  constructor(config: ClientConfig = {}) {
    this.baseUrl = (config.baseUrl ?? DEFAULT_BASE_URL).replace(/\/$/, "");
    this.archiveUrl = (config.archiveUrl ?? DEFAULT_ARCHIVE_URL).replace(/\/$/, "");
    this.routingTtlMs = (config.routingTtlSec ?? DEFAULT_ROUTING_TTL_SEC) * 1000;
    this.verify = config.verify ?? false;
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

    const url = `${this.baseUrl}/${s}`;
    const resp = await this.fetchImpl(url);
    if (!resp.ok) throw new HttpError(resp.status, url, await resp.text());
    const bytes = new Uint8Array(await resp.arrayBuffer());

    if (this.verify) {
      const { verifyCheckpointZst } = await import("./verify.ts");
      await verifyCheckpointZst(bytes, epoch.epoch, this.getCommittee.bind(this));
    }
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

  private async getCommittee(_epoch: number): Promise<unknown> {
    // Wired up when the verify module lands. For now a placeholder that
    // lets verify.ts compile against this signature.
    throw new Error("committee bootstrapping not yet implemented");
  }
}
