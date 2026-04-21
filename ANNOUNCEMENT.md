# A BLS-verified Sui checkpoint archive, served from the edge

We built an archive of every Sui mainnet checkpoint ever produced, verified independently at ingest, and served it from Cloudflare's edge network. It is live at:

```
https://checkpoints.mainnet.sui.unconfirmed.cloud
```

Today the archive covers 1,100+ epochs (roughly four years of Sui mainnet) totaling about 13.4 TB. Every byte was BLS-verified against its epoch's validator committee before it ever reached our storage. Anyone can re-verify the same way using the `sui-checkpoint-verifier` crate.

## What this is for

If you run a Sui light client, build indexers, do historical analytics, or want to reproduce state at any point in chain history, you need access to checkpoints. Sui publishes them at `checkpoints.mainnet.sui.io`, but that endpoint is optimized for ingestion by fullnodes, not casual range queries or point lookups. You typically download whole `.binpb.zst` files by sequence number.

We wanted something friendlier: fast point lookups over a globally cached edge network, with ingest-time cryptographic verification so consumers don't have to trust either us or the upstream archive.

## How it works

Three components.

**The ingester** is a Rust service that runs on a small VM. For each epoch, it:

1. Downloads every checkpoint in that epoch's sequence range from the Sui archive, in parallel.
2. Decodes each checkpoint's `CheckpointSummary` and re-verifies its BLS12-381 aggregate signature against the committee for that epoch. The committee itself is bootstrapped from the previous epoch's end-of-epoch checkpoint, so verification chains back to an onchain-signed starting point.
3. Concatenates the raw, byte-exact zstd frames into one `epoch-N.zst` object and uploads via S3 multipart with 32 MB parts at 32-way concurrency.
4. Writes a tiny `epoch-N.idx` side-car that maps every sequence number in the epoch to its (offset, length) inside the `.zst`.
5. Upserts one row into Cloudflare D1 with routing metadata for the epoch.

If BLS verification fails on any checkpoint, the whole epoch aborts. Nothing enters the archive without a valid signature from the correct validator set.

**The routing table** is a single D1 SQLite row per epoch. We considered a JSON manifest blob instead, but avoided it specifically because rewriting a shared blob on every epoch introduces a read-modify-write race. D1 gives us atomic per-epoch upserts with no coordination between writers. The schema is minimal: epoch number, first and last sequence numbers, the `.zst` size in bytes, and SHA256 hashes of both files for integrity. Keys and sizes that are trivially derivable from convention are not stored.

**The proxy** is a Cloudflare Worker written in Hono/TypeScript. It exposes a small API:

- `GET /:seq.binpb.zst` returns the raw `.binpb.zst` bytes for a single checkpoint, resolved by a single D1 query, a 20-byte range read of the epoch's `.idx`, and a final ranged `GET` against R2.
- `GET /epochs` returns metadata for every indexed epoch.
- `GET /epochs/:N` returns metadata for one epoch, including object keys so clients can pull the whole epoch directly from R2 for bulk work.
- `GET /health` returns summary stats.

All responses that can be cached are cached at Cloudflare's edge for up to a year with immutable semantics. Repeat requests for the same checkpoint never touch D1 or R2.

## Design choices that cut costs

Several decisions compound to make this extremely cheap to operate.

**One object per epoch instead of per checkpoint.** The naive design would store 71 million separate objects, one per checkpoint. Writing those would incur 71 million Class A operations; listing or walking them would be painful. Instead we concatenate every checkpoint in an epoch into one zstd-multiframe `.zst` object with a 20-byte-entry `.idx` side-car. Per-checkpoint reads become a ranged `GET` on the epoch's `.zst`, which is one Class B op regardless of the range size.

Concrete cost numbers for our backfill of the full Sui mainnet history (71.4M checkpoints across 1,103 epochs, 13.4 TB):

| Architecture | Class A ops (backfill) | Cost at R2 pricing |
|--------------|----------------------:|-------------------:|
| Per-checkpoint (naive) | 71.4M (one PutObject each) | $321 |
| Per-epoch (ours) | 404k (CreateMultipart + UploadPart ×400,841 + CompleteMultipart + idx PutObject, across all epochs) | $1.82 |

That's a roughly **175x reduction in one-time Class A ingest cost**, with no downside for point lookups. Ongoing ingest for new epochs costs roughly $0.002 per epoch in ops, which at one epoch per day works out to under $1 per year for follow-mode operation.

Bulk download cost (serving the whole archive to one consumer) is where per-epoch really wins. Per-epoch: 1,103 GetObjects = $0.0004. Per-checkpoint: 71.4M GetObjects = $25.70. A roughly 85,000x difference for the bulk-analytics path.

Range reads benefit from the same structure. Instead of exposing a ``range endpoint` (dropped)` endpoint that would fragment the cache namespace, we rely on the fact that clients wanting a contiguous range can either parallel-fetch per-checkpoint URLs (which multiplex over HTTP/2 and benefit from edge caching) or download the whole epoch's `.zst` directly from R2 in a single request and slice it locally with the `.idx`. Both approaches are cache-friendly and cheap; the dropped endpoint was the only cache-hostile shape in the API.

**No range endpoint.** We originally offered ``range endpoint` (dropped)` for bulk range reads. We removed it. The reason is cache economics. Per-checkpoint URLs live in a bounded namespace of 71 million, so hot ones cache well at the edge. Range URLs live in a combinatorial namespace of roughly 5 quadrillion possible `(from, to)` pairs; almost every range request is unique and misses the cache, punching through to D1 and R2 on every hit. Clients that want multiple checkpoints either issue parallel per-checkpoint requests, which multiplex cleanly over HTTP/2 and benefit from edge caching, or download the whole epoch directly from R2.

**D1 stores only what isn't derivable.** No per-checkpoint rows, no object keys that follow convention, no counts that can be computed. Six columns total: epoch, first_seq, last_seq, zst_bytes, zst_sha256, idx_sha256. One row per epoch. 1,103 rows for all of Sui mainnet history. D1 queries at this size are effectively instant.

**Edge caching as a first-class deployment layer.** Every `/:seq.binpb.zst` response gets `cache-control: public, max-age=31536000, immutable` and is stored in Cloudflare's Cache API. After the first hit on any edge PoP, subsequent requests in that region skip the Worker runtime, skip D1, skip R2, and return from edge RAM. This is the primary mechanism that makes the service scale for free.

**Worker and R2 in the same provider.** The binding between the Worker and R2 is a direct API call within Cloudflare's private backbone. There is no public internet hop between them on cache misses. No request signing, no TLS handshake per call. This keeps miss-path latency low.

## Independent verification at ingest

The cryptographic story is the part we're most proud of. Every checkpoint in the archive has been BLS-verified by us, independently of Mysten's upstream archive. The verifier is a small shared Rust crate (`sui-checkpoint-verifier`) that anyone can reuse.

For each checkpoint we:

1. Parse `CheckpointSummary` from the decompressed protobuf.
2. Build the Sui intent message: the 3-byte intent prefix, the BCS-encoded summary, and the little-endian epoch number.
3. Verify the signature's `RoaringBitmap` against the committee's ordered validator list.
4. Use `blst` portable to check the BLS12-381 aggregate signature.

If any of this fails, ingest aborts and nothing enters the archive. This gives consumers a stronger guarantee than "these are the bytes Sui served us." It says "these bytes were signed by the correct validator set at the time of production, and we verified it." Consumers can re-verify using the same crate if they choose not to trust us.

## Performance

All numbers below were measured from the ingester VM in Los Angeles with a single Bun process (HTTP/2 connection pooling, shared TLS session). Real-world latency from users elsewhere is dominated by their distance to the nearest Cloudflare PoP, typically 5 to 30 ms in major metros.

**Warm cache (edge hits, no origin round-trip):**

| Concurrency | Throughput | p50 | p95 | p99 |
|------------:|-----------:|----:|----:|----:|
| 32 | 1,490 req/s | 20 ms | 32 ms | 45 ms |
| 128 | 7,570 req/s | 15 ms | 30 ms | 46 ms |
| 256 | 14,590 req/s | 14 ms | 32 ms | 64 ms |

The warm-cache path is essentially "Cloudflare's RAM responds to the client." Our Worker, D1, and R2 are all bypassed. Throughput scales with concurrency until client-side bottlenecks.

**Cold path (D1 query plus two R2 range reads on every request):**

| Concurrency | Throughput | p50 | p95 | p99 |
|------------:|-----------:|----:|----:|----:|
| 32 | 62 req/s | 423 ms | 1,023 ms | 1,794 ms |
| 128 | 192 req/s | 529 ms | 879 ms | 1,121 ms |
| 256 | 217 req/s | 1,099 ms | 1,396 ms | 1,596 ms |

Cold throughput plateaus around 200 req/s because every miss does three serial steps: a D1 lookup to find the epoch, a 20-byte range read on the `.idx` for the offset, and a ranged `GET` on the `.zst` for the frame itself. At 256 concurrency the queue depth grows faster than we can drain, so p50 latency climbs without a matching throughput gain. 128 is the sweet spot for cold-heavy workloads.

**Metadata endpoints:**

| Endpoint | p50 | p99 | Response |
|----------|----:|----:|---------:|
| `/epochs` (full listing, 1,103 rows) | 250 ms | 519 ms | 359 KB JSON |
| `/health` | 64 ms | 80 ms | small JSON |

In practice, real clients hit a mix of warm and cold URLs, with warm dominating over time as the hot set of recently-requested checkpoints populates each PoP's edge cache. A client that follows the chain tip or scans a contiguous range of recent epochs will see warm-cache latency after the first request to each URL.

## How to use it

Fetch one checkpoint by sequence number:

```bash
curl -o checkpoint.binpb.zst \
  https://checkpoints.mainnet.sui.unconfirmed.cloud/12345678.binpb.zst
zstd -d checkpoint.binpb.zst -o checkpoint.binpb
```

List all indexed epochs:

```bash
curl https://checkpoints.mainnet.sui.unconfirmed.cloud/epochs | jq '.epochs[:3]'
```

Inspect a specific epoch's metadata:

```bash
curl https://checkpoints.mainnet.sui.unconfirmed.cloud/epochs/500
```

For bulk historical analysis, pull the whole `epoch-N.zst` object directly from the bucket, use the `.idx` for per-checkpoint offsets, and process locally. Light clients can re-verify with `sui-checkpoint-verifier`.

## What's next

The service is running continuously under systemd and will ingest new epochs as Sui produces them. Future work on our side:

1. Mirror the archive to S3 Glacier Deep Archive as a disaster-recovery backup (roughly $1 per TB per month, cheap insurance).
2. Testnet coverage, identical architecture, separate bucket.
3. Publish a small client SDK that handles per-checkpoint fetch, offset resolution via `.idx`, and BLS re-verification in one call.

If you build something on top of this, or you hit a missing feature, let us know.
