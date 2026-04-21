# @unconfirmedlabs/sui-checkpoint-archive

TypeScript client for the [unconfirmedlabs Sui checkpoint archive](../ANNOUNCEMENT.md). Gives you point lookups, range iteration, and optional on-device BLS re-verification of every checkpoint in Sui mainnet history.

```bash
bun add @unconfirmedlabs/sui-checkpoint-archive
```

## Quick start

```ts
import { SuiArchiveClient } from "@unconfirmedlabs/sui-checkpoint-archive";

const client = new SuiArchiveClient({
  cacheDir: "~/.cache/sui-archive",  // optional; memory-only if omitted
});

const { bytes, epoch } = await client.getCheckpoint(12_345_678);
// bytes: raw .binpb.zst, one zstd frame
// epoch: which Sui epoch it belongs to
```

## What it does

- **Routing cache** — one `/epochs` fetch gives you a sorted in-memory table. Binary-search for seq → epoch with no network round-trip after the first call.
- **Idx caching** — `.idx` side-cars (20 bytes per checkpoint) are fetched per-epoch on demand and cached. Optional disk persistence.
- **Point lookups** — `getCheckpoint(seq)` returns the raw compressed bytes.
- **Bulk warmup** — prefetch recent N epochs or everything into the local cache for offline use.
- **Optional client-side BLS verification** — pass `verify: true` to re-run the same signature check our ingester did. (Wasm verifier coming in a follow-up release.)

## CLI

```bash
bunx @unconfirmedlabs/sui-checkpoint-archive warmup --recent 100
bunx @unconfirmedlabs/sui-checkpoint-archive get 12345678 > ckpt.binpb.zst
bunx @unconfirmedlabs/sui-checkpoint-archive epochs --json | jq '.[0]'
bunx @unconfirmedlabs/sui-checkpoint-archive stats
```

## Why you'd want it

- You're building a Sui light client, indexer, or historical analytics pipeline and want byte-exact, BLS-verified checkpoints without running a full archival node.
- Your workload is read-heavy and would benefit from a warm local cache of `.idx` files so lookups become "one network round-trip for the checkpoint itself."
- You want to re-verify signatures locally and not trust any intermediary, including us.

## Scope

This package is deliberately small. It returns raw bytes. It does **not** decode the protobuf. If you want decoded checkpoint content (transactions, events, balance changes, etc.), use [`jun`](https://github.com/unconfirmedlabs/jun) on top of this SDK, or bring your own protobuf decoder.

## Config

```ts
new SuiArchiveClient({
  baseUrl: "https://checkpoints.mainnet.sui.unconfirmed.cloud",  // default
  cacheDir: "~/.cache/sui-archive",                              // optional
  routingTtlSec: 3600,                                           // default 1h
  verify: false,                                                 // opt-in BLS
});
```

## License

Apache-2.0
