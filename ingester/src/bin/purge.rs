//! One-shot bucket + D1 purge tool. Deletes every object in the bucket (all
//! keys) and wipes the `epochs` table. Used once before re-running backfill
//! under new integrity guarantees.
//!
//! Required env:
//!   S3_ENDPOINT / S3_BUCKET / AWS_REGION / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//!   CF_API_TOKEN / CF_ACCOUNT_ID / CF_D1_DATABASE_ID
//!
//! Gated on `--yes` to avoid accidental execution.

use anyhow::{Context, Result};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use clap::Parser;

#[path = "../d1.rs"]
mod d1;
#[path = "../epoch.rs"]
mod epoch;
#[path = "../archive.rs"]
mod archive;
#[path = "../verify.rs"]
mod verify;

#[derive(Parser, Debug)]
#[command(about = "Purge R2 bucket + D1 epochs table")]
struct Cli {
    #[arg(long)]
    yes: bool,

    #[arg(long, env = "S3_BUCKET")]
    bucket: Option<String>,

    /// Only abort in-progress multipart uploads. Don't delete completed
    /// objects, don't truncate D1. Safe to run against a live bucket to
    /// clear out orphaned multiparts from earlier failed ingests.
    #[arg(long)]
    abort_multiparts_only: bool,

    /// List objects only — don't delete anything. Prints counts by extension
    /// and the set of epoch numbers present. Safe to run against a live bucket.
    #[arg(long)]
    list_only: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    if !cli.yes && !cli.list_only {
        anyhow::bail!("refusing to purge without --yes (--list-only is safe and doesn't need it)");
    }

    let bucket = cli
        .bucket
        .or_else(|| std::env::var("S3_BUCKET").ok())
        .context("S3_BUCKET not set")?;

    let s3 = epoch::make_s3_client().await?;

    // ── 0. List-only mode: report what's in the bucket, exit. ───────────────
    if cli.list_only {
        use std::collections::BTreeSet;
        let mut zst_epochs: BTreeSet<u64> = BTreeSet::new();
        let mut idx_epochs: BTreeSet<u64> = BTreeSet::new();
        let mut other: Vec<String> = Vec::new();
        let mut total_bytes: u64 = 0;
        let mut continuation: Option<String> = None;
        loop {
            let mut req = s3.list_objects_v2().bucket(&bucket).max_keys(1000);
            if let Some(tok) = &continuation {
                req = req.continuation_token(tok);
            }
            let resp = req.send().await.context("list_objects_v2")?;
            for o in resp.contents.unwrap_or_default() {
                let key = o.key.unwrap_or_default();
                total_bytes += o.size.unwrap_or(0) as u64;
                if let Some(n) = key.strip_prefix("epoch-").and_then(|s| s.strip_suffix(".zst")) {
                    if let Ok(e) = n.parse::<u64>() { zst_epochs.insert(e); continue; }
                }
                if let Some(n) = key.strip_prefix("epoch-").and_then(|s| s.strip_suffix(".idx")) {
                    if let Ok(e) = n.parse::<u64>() { idx_epochs.insert(e); continue; }
                }
                other.push(key);
            }
            continuation = resp.next_continuation_token;
            if continuation.is_none() { break; }
        }
        println!("zst_count={}  idx_count={}  other_count={}  total_bytes={}",
                 zst_epochs.len(), idx_epochs.len(), other.len(), total_bytes);
        if let (Some(zmin), Some(zmax)) = (zst_epochs.iter().next(), zst_epochs.iter().next_back()) {
            println!("zst_range={zmin}..={zmax}");
        }
        let only_in_zst: Vec<_> = zst_epochs.difference(&idx_epochs).collect();
        let only_in_idx: Vec<_> = idx_epochs.difference(&zst_epochs).collect();
        if !only_in_zst.is_empty() { println!("WARN zst-without-idx: {:?}", only_in_zst); }
        if !only_in_idx.is_empty() { println!("WARN idx-without-zst: {:?}", only_in_idx); }
        if !other.is_empty() { println!("WARN non-epoch objects: {other:?}"); }
        return Ok(());
    }

    // ── 1. List + delete all objects in the bucket. ──────────────────────────
    if !cli.abort_multiparts_only {
        let mut continuation: Option<String> = None;
        let mut total_deleted = 0usize;
        loop {
            let mut req = s3.list_objects_v2().bucket(&bucket).max_keys(1000);
            if let Some(tok) = &continuation {
                req = req.continuation_token(tok);
            }
            let resp = req.send().await.context("list_objects_v2")?;
            let objects = resp.contents.unwrap_or_default();
            if objects.is_empty() {
                break;
            }

            let ids: Vec<ObjectIdentifier> = objects
                .iter()
                .filter_map(|o| o.key.clone())
                .map(|k| ObjectIdentifier::builder().key(k).build().expect("key"))
                .collect();
            let n = ids.len();
            s3.delete_objects()
                .bucket(&bucket)
                .delete(
                    Delete::builder()
                        .set_objects(Some(ids))
                        .quiet(true)
                        .build()
                        .expect("delete"),
                )
                .send()
                .await
                .context("delete_objects")?;
            total_deleted += n;
            tracing::info!("deleted batch of {n} (total {total_deleted})");

            continuation = resp.next_continuation_token;
            if continuation.is_none() {
                break;
            }
        }
        tracing::info!("R2 purge complete: {total_deleted} objects deleted");
    }

    // ── 2. Abort in-progress multipart uploads. ──────────────────────────────
    // In --abort-multiparts-only mode we skip any upload initiated in the
    // last 10 minutes — those are likely the live ingester's current epoch,
    // and aborting them would crash it.
    let cutoff = if cli.abort_multiparts_only {
        Some(aws_sdk_s3::primitives::DateTime::from_secs(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64)
                - 600,
        ))
    } else {
        None
    };
    let mp = s3
        .list_multipart_uploads()
        .bucket(&bucket)
        .send()
        .await
        .context("list_multipart_uploads")?;
    let mut aborted = 0usize;
    let mut skipped_recent = 0usize;
    if let Some(uploads) = mp.uploads {
        for u in uploads {
            if let (Some(key), Some(upload_id)) = (u.key.clone(), u.upload_id.clone()) {
                if let (Some(cut), Some(init)) = (cutoff, u.initiated) {
                    if init > cut {
                        skipped_recent += 1;
                        tracing::info!("skipping recent multipart (in-flight): {key}");
                        continue;
                    }
                }
                s3.abort_multipart_upload()
                    .bucket(&bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .send()
                    .await
                    .context("abort_multipart_upload")?;
                aborted += 1;
                tracing::info!("aborted multipart upload: {key}");
            }
        }
    }
    tracing::info!("multipart aborts: {aborted}, skipped_recent: {skipped_recent}");

    // ── 3. Truncate D1 epochs table. ─────────────────────────────────────────
    if !cli.abort_multiparts_only {
        if let Some(creds) = d1::D1Creds::from_env() {
            d1::d1_query(&creds, "DELETE FROM epochs", vec![]).await?;
            tracing::info!("D1 epochs table cleared");
        } else {
            tracing::warn!("D1 creds missing — skipping table truncate");
        }
    }

    Ok(())
}
