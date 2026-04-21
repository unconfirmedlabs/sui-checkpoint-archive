#!/usr/bin/env bash
# Build the WASM verifier for the Cloudflare Worker.
#
# Requirements:
#   * rustup with the `wasm32-unknown-unknown` target installed
#   * LLVM 21+ clang (for blst's C sources). Install via `brew install llvm`.
#   * wabt (for wasm-strip). Install via `brew install wabt`.
#
# Output: ../proxy/src/suilight.wasm
set -euo pipefail

CLANG=/opt/homebrew/opt/llvm@21/bin/clang
LLVM_AR=/opt/homebrew/opt/llvm@21/bin/llvm-ar

[[ -x "$CLANG" ]] || { echo "install: brew install llvm" >&2; exit 1; }
command -v wasm-strip >/dev/null || { echo "install: brew install wabt" >&2; exit 1; }

cd "$(dirname "$0")"

CC_wasm32_unknown_unknown="$CLANG" \
AR_wasm32_unknown_unknown="$LLVM_AR" \
cargo build --release --target wasm32-unknown-unknown --features wasm

WASM="target/wasm32-unknown-unknown/release/sui_checkpoint_verifier.wasm"
OUT="../proxy/src/suilight.wasm"

BEFORE=$(wc -c < "$WASM")
wasm-strip "$WASM"
AFTER=$(wc -c < "$WASM")

cp "$WASM" "$OUT"
GZIP=$(gzip -c9 "$OUT" | wc -c)

printf 'Built %s\n' "$OUT"
printf '  raw:  %d bytes (%d KB)\n' "$AFTER" $((AFTER/1024))
printf '  gzip: %d bytes (%d KB)\n' "$GZIP"  $((GZIP/1024))
printf '  stripped %d bytes of custom sections\n' $((BEFORE-AFTER))
