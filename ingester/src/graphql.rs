//! Tiny GraphQL client — just enough to resolve an epoch to its first/last
//! checkpoint sequence numbers.
//!
//! Strategy: query `epoch(epochId: N).firstCheckpoint` and the same for N+1,
//! then last_seq = next.first_seq - 1. If the N+1 epoch hasn't started yet,
//! the epoch is still in flight and we refuse to ingest.

use anyhow::{Context, Result};
use serde::Deserialize;

pub async fn resolve_epoch_range(
    http: &reqwest::Client,
    graphql_url: &str,
    epoch: u64,
) -> Result<(u64, u64)> {
    let body = format!(
        r#"{{"query":"{{ cur: epoch(epochId: {epoch}) {{ checkpoints(first:1) {{ nodes {{ sequenceNumber }} }} }} nxt: epoch(epochId: {next}) {{ checkpoints(first:1) {{ nodes {{ sequenceNumber }} }} }} }}"}}"#,
        epoch = epoch,
        next = epoch + 1,
    );

    let resp = http
        .post(graphql_url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .context("graphql POST")?
        .error_for_status()?;

    let parsed: GqlResponse = resp.json().await.context("graphql parse")?;
    if let Some(errs) = parsed.errors {
        if let Some(first) = errs.into_iter().next() {
            anyhow::bail!("graphql: {}", first.message);
        }
    }
    let data = parsed.data.context("graphql: empty data")?;

    let first = data
        .cur
        .checkpoints
        .nodes
        .first()
        .map(|n| n.sequence_number)
        .with_context(|| format!("epoch {epoch} not found"))?;

    let next_first = data.nxt.checkpoints.nodes.first().map(|n| n.sequence_number);
    let last = match next_first {
        Some(next_first) => next_first - 1,
        None => anyhow::bail!(
            "epoch {epoch} is not yet complete (epoch {} hasn't started)",
            epoch + 1
        ),
    };
    Ok((first, last))
}

#[derive(Debug, Deserialize)]
struct GqlResponse {
    data: Option<GqlData>,
    errors: Option<Vec<GqlError>>,
}

#[derive(Debug, Deserialize)]
struct GqlError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct GqlData {
    cur: EpochPartial,
    nxt: EpochPartial,
}

#[derive(Debug, Deserialize)]
struct EpochPartial {
    checkpoints: Checkpoints,
}

#[derive(Debug, Deserialize)]
struct Checkpoints {
    nodes: Vec<CheckpointNode>,
}

#[derive(Debug, Deserialize)]
struct CheckpointNode {
    #[serde(rename = "sequenceNumber")]
    sequence_number: u64,
}
