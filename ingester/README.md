# sui-checkpoint-ingester

Rust rewrite of the Go `checkpoint-sync` tool. Aggregates one Sui epoch's
checkpoints into a single `.zst` + `.idx` pair in R2 and upserts a routing
row in D1 — powering `sui-checkpoint-archive/proxy`.

Uses the official MystenLabs crates for verification:
- [`sui-sdk-types`](https://crates.io/crates/sui-sdk-types) for BCS types
- [`fastcrypto`](https://crates.io/crates/fastcrypto) for BLS12-381 min-sig

## Build

```sh
cargo build --release
```

Cross-compile for linux:

```sh
# Install the target if needed:
rustup target add x86_64-unknown-linux-gnu
# Or use cross:
cargo install cross
cross build --release --target x86_64-unknown-linux-gnu
```

## Usage

```sh
source .env
# Ingest a completed epoch (auto-detects range via GraphQL).
./target/release/sui-checkpoint-ingester --epoch 1100

# Verify every checkpoint during ingest (~30 min on 370K checkpoints).
./target/release/sui-checkpoint-ingester --epoch 1100 --verify

# Smoke test on a small range (override resolved epoch bounds).
./target/release/sui-checkpoint-ingester --epoch 1100 \
  --from 265747328 --to 265747427 \
  --epoch-zst-key test/smoke.zst --epoch-idx-key test/smoke.idx

# Manually dump an epoch's committee (for ad-hoc D1 backfill).
./target/release/sui-checkpoint-ingester --dump-committee 1100
```

## Output format

`.zst` — concatenated zstd frames (RFC 8878 multi-frame stream), one per checkpoint.

`.idx` — 20 bytes per checkpoint:
- `u64` seq (LE)
- `u64` byte offset into `.zst` (LE)
- `u32` byte length of that checkpoint's frame (LE)

Fixed-size entries mean the proxy can look up checkpoint `s` via:
`offset = (s - first_seq) × 20` → one 20-byte range read.
