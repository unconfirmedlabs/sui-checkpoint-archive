/**
 * Wire row returned by the proxy's /epochs and /epochs/:N endpoints.
 *
 * D1 only stores the first six fields; the rest are derived by the proxy
 * (and mirrored here so clients never have to rederive them).
 */
export interface EpochMetadata {
  epoch: number;
  first_seq: number;
  last_seq: number;
  zst_bytes: number;
  zst_sha256: string;
  idx_sha256: string;

  // Derived, always present.
  count: number;
  zst_key: string;
  idx_key: string;
  idx_bytes: number;
}

/** Health endpoint payload. */
export interface HealthResponse {
  network: string;
  epochs: number;
  min_seq: number | null;
  max_seq: number | null;
}

/** Result of a single-checkpoint fetch. */
export interface CheckpointResult {
  /** Checkpoint sequence number requested. */
  seq: number;
  /** Epoch the checkpoint belongs to. */
  epoch: number;
  /** Raw .binpb.zst bytes — one zstd frame, verified at ingest. */
  bytes: Uint8Array;
}

export interface ClientConfig {
  /**
   * Proxy base URL. Handles /epochs, /health, and the single-checkpoint
   * route that does server-side idx resolution. Defaults to the
   * unconfirmedlabs production proxy. Trailing slash tolerated.
   */
  baseUrl?: string;

  /**
   * R2 public bucket URL for direct object downloads (idx and zst files).
   * When idx is cached locally, `getCheckpoint` issues a single ranged
   * GET against this host, bypassing the proxy's serial round-trips.
   * Defaults to the unconfirmedlabs archive bucket.
   */
  archiveUrl?: string;

  /**
   * If set, persist routing snapshot and cached .idx files to this directory.
   * Tilde expansion is performed. Defaults to memory-only caching.
   */
  cacheDir?: string;

  /**
   * TTL for the routing snapshot, in seconds. When exceeded, the next
   * lookup refreshes from /epochs. Defaults to 1 hour.
   */
  routingTtlSec?: number;

  /**
   * Enable client-side BLS re-verification of every checkpoint returned by
   * `getCheckpoint`. Defaults to false. When true, the optional wasm BLS
   * verifier is lazily imported on first use.
   */
  verify?: boolean;

  /** Custom fetch for tests or non-standard runtimes. Defaults to global fetch. */
  fetch?: typeof fetch;
}

export interface CacheStats {
  /** Number of epochs in the routing snapshot. */
  epochs: number;
  /** Number of .idx files cached (memory or disk). */
  idxCached: number;
  /** Seconds since the routing snapshot was fetched. `null` if never fetched. */
  routingAgeSec: number | null;
  /** Optional disk stats when cacheDir is set. */
  disk?: {
    dir: string;
    routingExists: boolean;
    idxFiles: number;
    totalBytes: number;
  };
}
