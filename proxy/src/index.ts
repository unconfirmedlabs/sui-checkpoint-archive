import { Hono } from "hono";

type Bindings = {
  DB: D1Database;
  BUCKET: R2Bucket;
  NETWORK: string;
};

// ── Routing table row (one per epoch) ────────────────────────────────────────
//
// D1 only stores the fields that aren't trivially derivable. The wire shape
// returned to clients (via `toWireRow`) is richer: it fills in `count`,
// `zst_key`, `idx_key`, `idx_bytes` from convention so callers don't have to.

type EpochDbRow = {
  epoch: number;
  first_seq: number;
  last_seq: number;
  zst_bytes: number;
  zst_sha256: string;
  idx_sha256: string;
};

type EpochWireRow = EpochDbRow & {
  count: number;
  zst_key: string;
  idx_key: string;
  idx_bytes: number;
};

const IDX_ENTRY_BYTES = 20;
const EPOCH_COLS = "epoch, first_seq, last_seq, zst_bytes, zst_sha256, idx_sha256";

function zstKeyFor(epoch: number): string { return `epoch-${epoch}.zst`; }
function idxKeyFor(epoch: number): string { return `epoch-${epoch}.idx`; }

function toWireRow(r: EpochDbRow): EpochWireRow {
  const count = r.last_seq - r.first_seq + 1;
  return {
    ...r,
    count,
    zst_key: zstKeyFor(r.epoch),
    idx_key: idxKeyFor(r.epoch),
    idx_bytes: count * IDX_ENTRY_BYTES,
  };
}

async function findEpochForSeq(db: D1Database, seq: bigint): Promise<EpochDbRow | null> {
  const row = await db
    .prepare(
      `SELECT ${EPOCH_COLS}
         FROM epochs
        WHERE first_seq <= ?
        ORDER BY first_seq DESC
        LIMIT 1`,
    )
    .bind(seq.toString())
    .first<EpochDbRow>();

  if (!row) return null;
  if (BigInt(row.last_seq) < seq) return null;
  return row;
}

// 20-byte fixed entry: u64 seq (LE) | u64 zst_offset (LE) | u32 zst_length (LE).
function parseIdxEntry(buf: ArrayBuffer): { seq: bigint; offset: bigint; length: number } {
  const view = new DataView(buf);
  return {
    seq: view.getBigUint64(0, true),
    offset: view.getBigUint64(8, true),
    length: view.getUint32(16, true),
  };
}

async function readIdxEntry(
  bucket: R2Bucket,
  idxKey: string,
  firstSeq: bigint,
  targetSeq: bigint,
): Promise<{ offset: bigint; length: number } | null> {
  const entryOffset = Number(targetSeq - firstSeq) * IDX_ENTRY_BYTES;
  const obj = await bucket.get(idxKey, {
    range: { offset: entryOffset, length: IDX_ENTRY_BYTES },
  });
  if (!obj) return null;
  const buf = await obj.arrayBuffer();
  if (buf.byteLength !== IDX_ENTRY_BYTES) return null;
  const { seq, offset, length } = parseIdxEntry(buf);
  if (seq !== targetSeq) {
    throw new Error(`idx mismatch: requested ${targetSeq}, got ${seq} in ${idxKey}`);
  }
  return { offset, length };
}

// ── Edge cache helper ────────────────────────────────────────────────────────
//
// All checkpoint + range responses are immutable, so we cache them under
// their request URL in Cloudflare's Cache API. First request at a given
// edge fills the cache; every subsequent request skips D1 + R2.

async function withEdgeCache(
  c: { executionCtx: ExecutionContext; req: { url: string } },
  build: () => Promise<Response>,
): Promise<Response> {
  const cacheKey = new Request(c.req.url, { method: "GET" });
  const cache = caches.default;
  const hit = await cache.match(cacheKey);
  if (hit) return hit;
  const res = await build();
  if (res.status >= 200 && res.status < 400) {
    c.executionCtx.waitUntil(cache.put(cacheKey, res.clone()));
  }
  return res;
}

// ── App ──────────────────────────────────────────────────────────────────────

const app = new Hono<{ Bindings: Bindings }>();

app.get("/", (c) => c.text(`sui-checkpoint-proxy (${c.env.NETWORK})`));

app.get("/health", async (c) => {
  const { results } = await c.env.DB
    .prepare(
      `SELECT COUNT(*) AS n, MIN(first_seq) AS min_seq, MAX(last_seq) AS max_seq FROM epochs`,
    )
    .all<{ n: number; min_seq: number | null; max_seq: number | null }>();
  return c.json({
    network: c.env.NETWORK,
    epochs: results?.[0]?.n ?? 0,
    min_seq: results?.[0]?.min_seq ?? null,
    max_seq: results?.[0]?.max_seq ?? null,
  });
});

app.get("/epochs", async (c) => {
  const { results } = await c.env.DB
    .prepare(`SELECT ${EPOCH_COLS} FROM epochs ORDER BY epoch`)
    .all<EpochDbRow>();
  return c.json({ epochs: (results ?? []).map(toWireRow) });
});

app.get("/epochs/:epoch", async (c) => {
  const epoch = Number(c.req.param("epoch"));
  if (!Number.isInteger(epoch) || epoch < 0) return c.json({ error: "bad epoch" }, 400);
  const row = await c.env.DB
    .prepare(`SELECT ${EPOCH_COLS} FROM epochs WHERE epoch = ?`)
    .bind(epoch)
    .first<EpochDbRow>();
  if (!row) return c.json({ error: "epoch not indexed" }, 404);
  return c.json(toWireRow(row));
});

// Single-checkpoint fetch. Returns raw `.binpb.zst` bytes (one zstd frame).
// BLS-verified at ingest; clients who want to re-verify use the shared
// `sui-checkpoint-verifier` crate (see README).
app.get("/checkpoints/:seq", async (c) =>
  withEdgeCache(c, async () => {
    const raw = c.req.param("seq");
    let seq: bigint;
    try {
      seq = BigInt(raw);
    } catch {
      return c.json({ error: "bad seq" }, 400);
    }
    if (seq < 0n) return c.json({ error: "bad seq" }, 400);

    const row = await findEpochForSeq(c.env.DB, seq);
    if (!row) return c.json({ error: "checkpoint not indexed" }, 404);

    const entry = await readIdxEntry(c.env.BUCKET, idxKeyFor(row.epoch), BigInt(row.first_seq), seq);
    if (!entry) return c.json({ error: "idx miss" }, 500);

    const obj = await c.env.BUCKET.get(zstKeyFor(row.epoch), {
      range: { offset: Number(entry.offset), length: entry.length },
    });
    if (!obj) return c.json({ error: "zst miss" }, 500);

    return new Response(obj.body, {
      headers: {
        "content-type": "application/zstd",
        "content-length": String(entry.length),
        "x-sui-seq": String(seq),
        "x-sui-epoch": String(row.epoch),
        "cache-control": "public, max-age=31536000, immutable",
      },
    });
  }),
);

export default app;
