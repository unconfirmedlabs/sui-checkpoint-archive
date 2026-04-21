# sui-checkpoint-archive

A BLS-verified archive of every Sui checkpoint ever produced, served from Cloudflare's edge network with optional client-side re-verification.

Live endpoints:

- Mainnet proxy: `https://checkpoints.mainnet.sui.unconfirmed.cloud`
- Mainnet bucket: `https://archive.checkpoints.mainnet.sui.unconfirmed.cloud`
- Testnet proxy: `https://checkpoints.testnet.sui.unconfirmed.cloud`

## Quick start

Fetch a single checkpoint by sequence number:

```bash
curl -o 12345678.binpb.zst \
  https://checkpoints.mainnet.sui.unconfirmed.cloud/checkpoints/12345678
zstd -d 12345678.binpb.zst -o 12345678.binpb
```

List indexed epochs:

```bash
curl https://checkpoints.mainnet.sui.unconfirmed.cloud/epochs | jq '.epochs[:3]'
```

Or use the TypeScript SDK:

```ts
import { SuiArchiveClient } from "@unconfirmedlabs/sui-checkpoint-archive";

const client = new SuiArchiveClient({ cacheDir: "~/.cache/sui-archive" });
const { bytes, epoch } = await client.getCheckpoint(12_345_678);
```

Longer-form walkthrough of the architecture, cost math, and benchmarks in [`ANNOUNCEMENT.md`](./ANNOUNCEMENT.md).

## Repository layout

This is a monorepo with four components:

| Directory | Language | Purpose |
|-----------|----------|---------|
| [`ingester/`](./ingester) | Rust | Pulls checkpoints from Sui's upstream archive, BLS-verifies each against its epoch's validator committee, concatenates into one `.zst` per epoch with a 20-byte-entry `.idx` side-car, uploads to R2 via multipart. Writes routing rows to D1. |
| [`verifier/`](./verifier) | Rust | Shared BLS verifier. Wraps `sui-sdk-types` + `blst`. Builds as a native rlib (used by the ingester) and targets wasm-pack for use from the TS client. |
| [`proxy/`](./proxy) | TypeScript (Hono / Cloudflare Workers) | Edge proxy. Resolves seq → epoch via D1, performs range reads against R2, caches immutable responses in the Cloudflare Cache API. |
| [`client/`](./client) | TypeScript (Bun) | `@unconfirmedlabs/sui-checkpoint-archive` SDK. In-memory routing cache, optional on-disk `.idx` cache, proxy transport for point lookups, direct-R2 transport for bulk epoch work. |

## How it works (one-paragraph)

The **ingester** fetches every checkpoint in an epoch from `checkpoints.mainnet.sui.io` in parallel, decodes and BLS-verifies each `CheckpointSummary` against the epoch's validator committee (bootstrapped from the previous epoch's end-of-epoch checkpoint), and concatenates the raw zstd frames into a single `epoch-N.zst` object in R2 with a 20-byte-per-checkpoint `epoch-N.idx` side-car. One D1 row per epoch records the routing metadata and SHA256 integrity hashes. The **proxy** serves `/checkpoints/:seq` by looking up the epoch in D1, doing a 20-byte range read on the `.idx` for the offset, then a ranged `GET` on the `.zst` for the frame — all cached at the edge, so warm reads are sub-30ms and skip both D1 and R2 entirely. The **client SDK** caches routing and idx files locally so repeat lookups on the same epoch are near-free, and exposes a `getEpochArchive` path that bypasses the proxy entirely for bulk workloads.

## Operating the archive yourself

You don't need to run anything to *consume* the archive — the public endpoints above serve everyone. If you want to run your own copy:

1. Create a Cloudflare R2 bucket and a D1 database.
2. `cp ingester/.env.example ingester/.env` and fill in credentials.
3. `cd ingester && cargo build --release`
4. Run `wrangler d1 migrations apply DB --remote` from `proxy/` to set up the routing table.
5. `./target/release/sui-checkpoint-ingester --epoch N` to backfill an epoch.
6. `cd proxy && bun run deploy` to ship the Worker.

A full mainnet backfill is ~13.4 TB of R2 storage and takes roughly a day on a beefy VM. See the [ingester README](./ingester/README.md) for concurrency tuning and troubleshooting.

## Status

- **Mainnet backfill:** complete through epoch 1102 (~266M checkpoints, 13.4 TB, zero ingest failures).
- **Testnet backfill:** in progress at time of writing.
- **Client SDK:** v0.1.0, functional but not yet published to npm.
- **Wasm verifier bundle for the SDK:** pending; currently the SDK's `verify: true` option throws with a clear message pointing to the Rust verifier crate.

## License

Apache-2.0 — see [`LICENSE`](./LICENSE).
