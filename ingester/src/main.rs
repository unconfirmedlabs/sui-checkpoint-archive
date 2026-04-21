//! Sui checkpoint archive ingester.
//!
//! Fetches all checkpoints in a given epoch from the Sui archive, verifies
//! every one against that epoch's committee using the official MystenLabs
//! crates (via `sui-checkpoint-verifier`), concatenates them into a single
//! `.zst` object in R2 (RFC 8878 multi-frame zstd stream, one frame per
//! checkpoint), builds a side-car `.idx`, and upserts a routing row in D1.
//!
//! The invariant: *if a checkpoint is in the archive, it was BLS-verified
//! at ingest against the correct epoch committee.* Proxy consumers don't
//! need to re-verify, and clients who don't trust us can verify themselves
//! using the `sui-checkpoint-verifier` crate.
//!
//! Epoch 0 is the single exception: no predecessor committee exists, so
//! those checkpoints are served unverified by construction.
//!
//! Command shape:
//!   sui-checkpoint-ingester --epoch 1100
//!
//! Environment variables:
//!   S3_ENDPOINT / S3_BUCKET / AWS_REGION
//!   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
//!   CF_API_TOKEN / CF_ACCOUNT_ID / CF_D1_DATABASE_ID  (for D1 upsert)

mod archive;
mod d1;
mod epoch;
mod graphql;
mod verify;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// The epoch to aggregate. Range is resolved via GraphQL unless
    /// --from/--to override it.
    #[arg(long)]
    epoch: u64,

    /// Override start seq (smoke-test helper).
    #[arg(long)]
    from: Option<u64>,

    /// Override end seq (inclusive, smoke-test helper).
    #[arg(long)]
    to: Option<u64>,

    /// Archive download concurrency.
    #[arg(long, default_value_t = 500)]
    fetch_workers: usize,

    /// Override the R2 object key for the .zst (default: epoch-{N}.zst).
    #[arg(long)]
    epoch_zst_key: Option<String>,

    /// Override the R2 object key for the .idx (default: epoch-{N}.idx).
    #[arg(long)]
    epoch_idx_key: Option<String>,

    /// Sui archive base URL.
    #[arg(long, env = "ARCHIVE_URL", default_value = "https://checkpoints.mainnet.sui.io")]
    archive_url: String,

    /// Sui GraphQL endpoint used to resolve epoch → checkpoint range.
    #[arg(long, env = "SUI_GRAPHQL_URL", default_value = "https://graphql.mainnet.sui.io/graphql")]
    graphql_url: String,

    /// Override the bucket name (or use S3_BUCKET env).
    #[arg(long, env = "S3_BUCKET")]
    bucket: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let epoch = cli.epoch;

    let bucket = cli
        .bucket
        .clone()
        .or_else(|| std::env::var("S3_BUCKET").ok())
        .context("S3_BUCKET not set and --bucket not provided")?;

    let http = reqwest::Client::builder()
        .tcp_keepalive(std::time::Duration::from_secs(60))
        .pool_max_idle_per_host(1024)
        .build()?;

    let (from, to) = match (cli.from, cli.to) {
        (Some(f), Some(t)) => {
            tracing::info!("override range for epoch {epoch}: {f} → {t}");
            (f, t)
        }
        _ => {
            tracing::info!("resolving range for epoch {epoch}...");
            let (f, t) = graphql::resolve_epoch_range(&http, &cli.graphql_url, epoch).await?;
            tracing::info!("epoch {epoch}: {f} → {t} ({} checkpoints)", t - f + 1);
            (f, t)
        }
    };

    let zst_key = cli
        .epoch_zst_key
        .clone()
        .unwrap_or_else(|| format!("epoch-{epoch}.zst"));
    let idx_key = cli
        .epoch_idx_key
        .clone()
        .unwrap_or_else(|| format!("epoch-{epoch}.idx"));

    // Bootstrap the committee for this epoch from its predecessor's
    // end-of-epoch checkpoint. Epoch 0 has no predecessor, so it's served
    // unverified by construction.
    let committee = if from > 0 {
        tracing::info!("bootstrapping committee from predecessor EoE (seq {})...", from - 1);
        Some(verify::resolve_epoch_committee(&http, &cli.archive_url, from - 1).await?)
    } else {
        tracing::info!("epoch 0: no predecessor committee — ingesting without BLS verification");
        None
    };

    let s3 = epoch::make_s3_client().await?;
    let result = epoch::run_epoch(epoch::EpochJob {
        epoch,
        from,
        to,
        zst_key,
        idx_key,
        bucket,
        archive_url: cli.archive_url,
        fetch_workers: cli.fetch_workers,
        committee,
        http,
        s3,
    })
    .await?;

    println!("{}", serde_json::to_string(&result)?);

    if let Some(creds) = d1::D1Creds::from_env() {
        d1::upsert_epoch(&creds, &result).await?;
        tracing::info!("D1 row upserted for epoch {}", result.epoch);
    }

    Ok(())
}
