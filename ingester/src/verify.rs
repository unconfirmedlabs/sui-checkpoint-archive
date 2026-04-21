//! Thin async wrapper around the shared verifier crate — fetches the
//! predecessor end-of-epoch checkpoint and extracts its next committee.

use crate::archive;
use anyhow::Result;

pub async fn resolve_epoch_committee(
    http: &reqwest::Client,
    archive_url: &str,
    prev_last_seq: u64,
) -> Result<Vec<u8>> {
    let zst = archive::fetch_zst(http, archive_url, prev_last_seq, 6).await?;
    sui_checkpoint_verifier::extract_next_committee_from_zst(&zst)
}

pub fn verify_checkpoint_zst(
    zst: &[u8],
    committee_packed: &[u8],
    committee_epoch: u64,
) -> Result<()> {
    sui_checkpoint_verifier::verify_checkpoint_zst(zst, committee_packed, committee_epoch)
}
