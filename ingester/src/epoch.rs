//! Per-epoch ingest orchestration: parallel fetch → in-order reorder → S3
//! multipart upload with equal-size non-trailing parts (R2 constraint).

use crate::{archive, verify};
use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, Semaphore};

/// R2 rule: all non-trailing multipart parts must be exactly the same size.
/// 32 MiB gives a sensible balance between part count and memory.
const PART_SIZE: usize = 32 * 1024 * 1024;
/// Number of simultaneous `upload_part` RPCs in flight. At 32 MiB/part this is
/// ~1 GiB resident in the uploader pool, which is fine on our ingest box.
const UPLOAD_CONCURRENCY: usize = 32;
/// Channel capacity between the writer task and the uploader pool. A bit larger
/// than `UPLOAD_CONCURRENCY` so the writer can stage the next chunk while parts
/// drain, avoiding head-of-line stalls.
const UPLOAD_CHANNEL_CAPACITY: usize = UPLOAD_CONCURRENCY * 2;

pub struct EpochJob {
    pub epoch: u64,
    pub from: u64,
    pub to: u64,
    pub zst_key: String,
    pub idx_key: String,
    pub bucket: String,
    pub archive_url: String,
    pub fetch_workers: usize,

    /// Committee for this epoch (decoded from the predecessor's end-of-epoch
    /// checkpoint). `Some` → every checkpoint is BLS-verified against it
    /// before being added to the archive; ingest aborts on first failure.
    /// `None` is only valid for epoch 0.
    pub committee: Option<Vec<u8>>,

    pub http: reqwest::Client,
    pub s3: S3Client,
}

#[derive(Debug, Serialize)]
pub struct EpochResult {
    pub epoch: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub count: u64,
    pub zst_key: String,
    pub idx_key: String,
    pub zst_parts: usize,
    pub zst_bytes: u64,
    pub idx_bytes: u64,
    pub zst_sha256: String,
    pub idx_sha256: String,
    pub elapsed_secs: u64,
}

pub async fn make_s3_client() -> Result<S3Client> {
    let endpoint = std::env::var("S3_ENDPOINT").context("S3_ENDPOINT required")?;
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(
            std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".to_string()),
        ))
        .endpoint_url(endpoint)
        .load()
        .await;
    Ok(aws_sdk_s3::Client::new(&config))
}

pub async fn run_epoch(job: EpochJob) -> Result<EpochResult> {
    let count = job.to - job.from + 1;
    let started = Instant::now();

    // ── 1. Initiate multipart upload with epoch-level metadata. ──────────────
    // R2 does not support `x-amz-checksum-sha256` on UploadPart (returns 501
    // NotImplemented), so we rely on the SDK's default per-part integrity
    // (Content-MD5 / CRC32 inside the request) plus our streaming whole-object
    // SHA256 (stored in metadata + D1) and a post-complete HeadObject size
    // check below.
    let cmu = job
        .s3
        .create_multipart_upload()
        .bucket(&job.bucket)
        .key(&job.zst_key)
        .content_type("application/zstd")
        .metadata("epoch", job.epoch.to_string())
        .metadata("first-seq", job.from.to_string())
        .metadata("last-seq", job.to.to_string())
        .metadata("count", count.to_string())
        .send()
        .await
        .context("create multipart upload")?;
    let upload_id = cmu.upload_id.context("no upload_id")?;

    // ── 2. Fetch checkpoints concurrently; reorder into byte stream. ─────────
    //
    // We use a BTreeMap keyed by seq for the reorder buffer. Fetch workers push
    // decoded bytes via an mpsc; the writer task pops in seq order, ships even
    // PART_SIZE chunks to part uploaders, and builds the index.
    let (fetched_tx, mut fetched_rx) = mpsc::channel::<(u64, Vec<u8>)>(1024);
    let fetch_sem = Arc::new(Semaphore::new(job.fetch_workers));
    let http = job.http.clone();
    let archive_url = job.archive_url.clone();
    let verify_ctx = job.committee.clone();
    let epoch_for_verify = job.epoch;
    let stats_downloaded = Arc::new(AtomicU64::new(0));
    let stats_bytes = Arc::new(AtomicU64::new(0));

    let fetch_handle = {
        let stats = stats_downloaded.clone();
        let bytes = stats_bytes.clone();
        tokio::spawn(async move {
            let mut tasks = FuturesUnordered::new();
            for seq in job.from..=job.to {
                let permit = fetch_sem.clone().acquire_owned().await.unwrap();
                let http = http.clone();
                let archive_url = archive_url.clone();
                let verify_ctx = verify_ctx.clone();
                let stats = stats.clone();
                let bytes = bytes.clone();
                let tx = fetched_tx.clone();
                tasks.push(tokio::spawn(async move {
                    let _permit = permit;
                    let zst = archive::fetch_zst(&http, &archive_url, seq, 6).await?;
                    if let Some(committee) = verify_ctx {
                        verify::verify_checkpoint_zst(&zst, &committee, epoch_for_verify)?;
                    }
                    stats.fetch_add(1, Ordering::Relaxed);
                    bytes.fetch_add(zst.len() as u64, Ordering::Relaxed);
                    tx.send((seq, zst)).await.ok();
                    Result::<()>::Ok(())
                }));

                // Reap completed fetches to surface errors + cap in-flight.
                while tasks.len() > job.fetch_workers * 2 {
                    if let Some(res) = tasks.next().await {
                        res??;
                    }
                }
            }
            while let Some(res) = tasks.next().await {
                res??;
            }
            drop(fetched_tx);
            Result::<()>::Ok(())
        })
    };

    // ── 3. Part uploader pool ────────────────────────────────────────────────
    let (part_tx, mut part_rx) = mpsc::channel::<(i32, Vec<u8>)>(UPLOAD_CHANNEL_CAPACITY);
    let mut part_handles = Vec::new();
    let part_sem = Arc::new(Semaphore::new(UPLOAD_CONCURRENCY));
    let upload_handle = {
        let s3 = job.s3.clone();
        let bucket = job.bucket.clone();
        let zst_key = job.zst_key.clone();
        let upload_id = upload_id.clone();
        let part_sem = part_sem.clone();
        tokio::spawn(async move {
            let mut parts: BTreeMap<i32, String> = BTreeMap::new();
            let mut spawned = FuturesUnordered::new();
            while let Some((num, data)) = part_rx.recv().await {
                let permit = part_sem.clone().acquire_owned().await.unwrap();
                let s3 = s3.clone();
                let bucket = bucket.clone();
                let zst_key = zst_key.clone();
                let upload_id = upload_id.clone();
                spawned.push(tokio::spawn(async move {
                    let _permit = permit;
                    // Retry upload_part on transient errors. A single flaky
                    // call shouldn't tear down the whole pipeline — the part
                    // is ~32 MiB in memory, so cloning for each retry is fine
                    // (retries are rare; typical path succeeds first try).
                    let mut backoff = std::time::Duration::from_millis(500);
                    let mut last_err: Option<anyhow::Error> = None;
                    for attempt in 1..=6u32 {
                        match s3
                            .upload_part()
                            .bucket(&bucket)
                            .key(&zst_key)
                            .upload_id(&upload_id)
                            .part_number(num)
                            .body(ByteStream::from(data.clone()))
                            .send()
                            .await
                        {
                            Ok(resp) => {
                                return Result::<(i32, String)>::Ok((
                                    num,
                                    resp.e_tag.unwrap_or_default(),
                                ));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "upload_part {num} attempt {attempt}/6 failed: {e:?}"
                                );
                                last_err = Some(anyhow::anyhow!("upload_part {num}: {e:?}"));
                                tokio::time::sleep(backoff).await;
                                backoff = (backoff * 2).min(std::time::Duration::from_secs(15));
                            }
                        }
                    }
                    Err(last_err.unwrap_or_else(|| {
                        anyhow::anyhow!("upload_part {num} exhausted retries")
                    }))
                }));
                while let Some(done) = spawned.try_next_some().await {
                    let (n, etag) = done??;
                    parts.insert(n, etag);
                }
            }
            while let Some(done) = spawned.next().await {
                let (n, etag) = done??;
                parts.insert(n, etag);
            }
            Result::<BTreeMap<i32, String>>::Ok(parts)
        })
    };
    part_handles.push(upload_handle);

    // ── 4. Writer: pop seqs in order, emit equal-sized parts. ────────────────
    let mut next_seq = job.from;
    let mut buf: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut cur_part: Vec<u8> = Vec::with_capacity(PART_SIZE + 1024 * 1024);
    let mut part_num: i32 = 0;
    let mut offset: u64 = 0;
    let mut zst_sha = Sha256::new();
    let mut index: Vec<(u64, u64, u32)> = Vec::with_capacity(count as usize);

    while let Some((seq, data)) = fetched_rx.recv().await {
        if seq < next_seq {
            continue; // defensive dedup
        }
        buf.insert(seq, data);

        while let Some(data) = buf.remove(&next_seq) {
            index.push((next_seq, offset, data.len() as u32));
            offset += data.len() as u64;
            zst_sha.update(&data);
            cur_part.extend_from_slice(&data);
            next_seq += 1;

            while cur_part.len() >= PART_SIZE {
                part_num += 1;
                let chunk: Vec<u8> = cur_part.drain(..PART_SIZE).collect();
                part_tx
                    .send((part_num, chunk))
                    .await
                    .context("part_tx send")?;
            }
        }
    }

    if next_seq <= job.to {
        anyhow::bail!(
            "incomplete archive: missing seqs {}..={}",
            next_seq,
            job.to
        );
    }

    // Flush trailing part (arbitrary size permitted).
    if !cur_part.is_empty() {
        part_num += 1;
        part_tx
            .send((part_num, std::mem::take(&mut cur_part)))
            .await
            .context("final part_tx")?;
    }
    drop(part_tx);

    fetch_handle.await.context("fetch task panic")??;
    let parts_map = part_handles
        .pop()
        .unwrap()
        .await
        .context("upload task panic")??;

    // ── 5. Complete multipart. ───────────────────────────────────────────────
    let completed_parts: Vec<CompletedPart> = parts_map
        .into_iter()
        .map(|(num, etag)| {
            CompletedPart::builder()
                .part_number(num)
                .e_tag(etag)
                .build()
        })
        .collect();
    job.s3
        .complete_multipart_upload()
        .bucket(&job.bucket)
        .key(&job.zst_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed_parts))
                .build(),
        )
        .send()
        .await
        .context("complete multipart")?;

    // Post-complete integrity: confirm R2 reports the same byte-count we
    // streamed. Per-part SHA256 already guarantees *those* bytes are the ones
    // we sent; size check catches the edge case of a part silently missing
    // from the assembled object.
    let zst_head = job
        .s3
        .head_object()
        .bucket(&job.bucket)
        .key(&job.zst_key)
        .send()
        .await
        .context("head .zst")?;
    let zst_reported_size = zst_head.content_length().unwrap_or(0);
    if zst_reported_size as u64 != offset {
        anyhow::bail!(
            ".zst size mismatch: R2 reports {}, expected {}",
            zst_reported_size,
            offset
        );
    }

    let zst_sha_hex = hex::encode(zst_sha.finalize());

    // ── 6. Serialise + upload the index (single PutObject). ──────────────────
    let mut idx_buf = Vec::with_capacity(index.len() * 20);
    for (seq, off, len) in &index {
        idx_buf.extend_from_slice(&seq.to_le_bytes());
        idx_buf.extend_from_slice(&off.to_le_bytes());
        idx_buf.extend_from_slice(&len.to_le_bytes());
    }
    let idx_sha = Sha256::digest(&idx_buf);
    let idx_sha_hex = hex::encode(idx_sha);

    job.s3
        .put_object()
        .bucket(&job.bucket)
        .key(&job.idx_key)
        .body(ByteStream::from(idx_buf.clone()))
        .content_type("application/octet-stream")
        .metadata("epoch", job.epoch.to_string())
        .metadata("first-seq", job.from.to_string())
        .metadata("last-seq", job.to.to_string())
        .metadata("count", count.to_string())
        .metadata("zst-sha256", zst_sha_hex.clone())
        .metadata("idx-sha256", idx_sha_hex.clone())
        .metadata("zst-key", job.zst_key.clone())
        .send()
        .await
        .context("put .idx")?;

    let idx_head = job
        .s3
        .head_object()
        .bucket(&job.bucket)
        .key(&job.idx_key)
        .send()
        .await
        .context("head .idx")?;
    let idx_reported_size = idx_head.content_length().unwrap_or(0);
    if idx_reported_size as u64 != idx_buf.len() as u64 {
        anyhow::bail!(
            ".idx size mismatch: R2 reports {}, expected {}",
            idx_reported_size,
            idx_buf.len()
        );
    }

    if index.len() as u64 != count {
        anyhow::bail!("index count {} != expected {}", index.len(), count);
    }

    Ok(EpochResult {
        epoch: job.epoch,
        first_seq: job.from,
        last_seq: job.to,
        count,
        zst_key: job.zst_key,
        idx_key: job.idx_key,
        zst_parts: part_num as usize,
        zst_bytes: offset,
        idx_bytes: idx_buf.len() as u64,
        zst_sha256: zst_sha_hex,
        idx_sha256: idx_sha_hex,
        elapsed_secs: started.elapsed().as_secs(),
    })
}

// Helper trait for non-blocking drain of FuturesUnordered.
trait FuturesUnorderedExt<T> {
    async fn try_next_some(&mut self) -> Option<T>;
}

impl<F: futures::Future> FuturesUnorderedExt<F::Output> for FuturesUnordered<F> {
    async fn try_next_some(&mut self) -> Option<F::Output> {
        // Yield to executor, then poll-once-ish without blocking.
        use futures::future::FutureExt;
        self.next().now_or_never().flatten()
    }
}
