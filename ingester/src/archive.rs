//! HTTP fetch against the Sui checkpoint archive, with exponential backoff.

use anyhow::{Context, Result};
use std::time::Duration;

pub async fn fetch_zst(
    http: &reqwest::Client,
    archive_url: &str,
    seq: u64,
    max_attempts: u32,
) -> Result<Vec<u8>> {
    let url = format!("{archive_url}/{seq}.binpb.zst");
    let mut backoff = Duration::from_millis(250);
    let mut last_err: Option<anyhow::Error> = None;

    for _attempt in 1..=max_attempts {
        match http.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let bytes = resp.bytes().await.context("read body")?.to_vec();
                if bytes.len() < 10 {
                    last_err = Some(anyhow::anyhow!(
                        "short body for seq {seq}: {} bytes",
                        bytes.len()
                    ));
                } else {
                    return Ok(bytes);
                }
            }
            Ok(resp) => {
                last_err = Some(anyhow::anyhow!(
                    "seq {seq} HTTP {}",
                    resp.status().as_u16()
                ));
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("seq {seq} request: {e}"));
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }

    Err(last_err
        .unwrap_or_else(|| anyhow::anyhow!("seq {seq} failed (unknown)")) )
}

