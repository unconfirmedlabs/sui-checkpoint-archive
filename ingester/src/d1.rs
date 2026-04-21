//! Minimal Cloudflare D1 REST client. Used to upsert the routing row after
//! each successful ingest. Skipped silently when credentials are missing.
//!
//! Env:
//!   CF_API_TOKEN          — API token with `D1:edit` scoped to the DB
//!   CF_ACCOUNT_ID         — Cloudflare account ID
//!   CF_D1_DATABASE_ID     — target D1 database UUID

use crate::epoch::EpochResult;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

pub struct D1Creds {
    pub api_token: String,
    pub account_id: String,
    pub database_id: String,
}

impl D1Creds {
    pub fn from_env() -> Option<Self> {
        let api_token = std::env::var("CF_API_TOKEN").ok()?;
        let account_id = std::env::var("CF_ACCOUNT_ID").ok()?;
        let database_id = std::env::var("CF_D1_DATABASE_ID").ok()?;
        if api_token.is_empty() || account_id.is_empty() || database_id.is_empty() {
            return None;
        }
        Some(Self {
            api_token,
            account_id,
            database_id,
        })
    }
}

#[derive(Debug, Serialize)]
struct D1Request<'a> {
    sql: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    params: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct D1Response {
    success: bool,
    #[serde(default)]
    errors: Vec<D1Error>,
}

#[derive(Debug, Deserialize)]
struct D1Error {
    message: String,
}

pub async fn d1_query(creds: &D1Creds, sql: &str, params: Vec<serde_json::Value>) -> Result<()> {
    let body = D1Request { sql, params };
    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query",
        creds.account_id, creds.database_id,
    );
    let resp = reqwest::Client::new()
        .post(&url)
        .bearer_auth(&creds.api_token)
        .json(&body)
        .send()
        .await
        .context("d1 request")?;
    let status = resp.status();
    let text = resp.text().await.context("d1 read")?;
    if !status.is_success() {
        anyhow::bail!("d1 HTTP {status}: {text}");
    }
    let parsed: D1Response = serde_json::from_str(&text).context("d1 parse")?;
    if !parsed.success {
        let first = parsed.errors.into_iter().next().map(|e| e.message);
        anyhow::bail!("d1 failure: {}", first.unwrap_or_default());
    }
    Ok(())
}

/// Upsert the routing row for one ingested epoch.
///
/// Schema is intentionally minimal: everything derivable from convention
/// (count, zst_key, idx_key, idx_bytes) is computed by the proxy at read
/// time, not stored. Ingest-time metadata (zst_parts, ingested_at) is
/// reported via EpochResult stdout JSON, not persisted.
pub async fn upsert_epoch(creds: &D1Creds, r: &EpochResult) -> Result<()> {
    let sql = "INSERT INTO epochs
           (epoch, first_seq, last_seq, zst_bytes, zst_sha256, idx_sha256)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(epoch) DO UPDATE SET
           first_seq  = excluded.first_seq,
           last_seq   = excluded.last_seq,
           zst_bytes  = excluded.zst_bytes,
           zst_sha256 = excluded.zst_sha256,
           idx_sha256 = excluded.idx_sha256";
    let params = vec![
        serde_json::json!(r.epoch),
        serde_json::json!(r.first_seq),
        serde_json::json!(r.last_seq),
        serde_json::json!(r.zst_bytes),
        serde_json::json!(r.zst_sha256),
        serde_json::json!(r.idx_sha256),
    ];
    d1_query(creds, sql, params).await
}
