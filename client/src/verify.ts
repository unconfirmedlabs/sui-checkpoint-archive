/**
 * Optional client-side BLS verification.
 *
 * Placeholder: the wasm bundle of `sui-checkpoint-verifier` will live at
 * `../verifier/pkg/sui_checkpoint_verifier_bg.wasm` once we run wasm-pack.
 * Until that build exists, calling this function throws, which is fine
 * because it's only imported when `ClientConfig.verify === true`.
 */

export async function verifyCheckpointZst(
  _bytes: Uint8Array,
  _epoch: number,
  _getCommittee: (epoch: number) => Promise<unknown>,
): Promise<void> {
  throw new Error(
    "client-side verification not yet implemented — wasm build of verifier pending. " +
      "Re-verify with the Rust `sui-checkpoint-verifier` crate for now.",
  );
}
