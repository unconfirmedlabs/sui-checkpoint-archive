/**
 * Load test. Compares three modes:
 *   1. proxy only                    — /:seq on the Worker
 *   2. direct-R2, cold idx           — fetch idx on first hit per epoch, then range-GET zst
 *   3. direct-R2, warmed idx         — all idx prefetched via warmup, just range-GET zst
 */

import { SuiArchiveClient } from "./src/index.ts";

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  const i = Math.max(0, Math.min(sorted.length - 1, Math.floor(sorted.length * p)));
  return sorted[i]!;
}

interface Result {
  label: string;
  n: number;
  conc: number;
  ok: number;
  err: number;
  elapsedSec: number;
  rps: number;
  mean: number;
  p50: number;
  p95: number;
  p99: number;
}

async function runBatch(
  client: SuiArchiveClient,
  label: string,
  total: number,
  concurrency: number,
  seqFn: (i: number) => number,
): Promise<Result> {
  const latencies: number[] = [];
  let ok = 0;
  let err = 0;
  let inFlight = 0;
  const queue: number[] = Array.from({ length: total }, (_, i) => i);
  const t0 = performance.now();

  await new Promise<void>((resolve) => {
    const launch = () => {
      while (inFlight < concurrency && queue.length > 0) {
        const i = queue.shift()!;
        inFlight++;
        const start = performance.now();
        client
          .getCheckpoint(seqFn(i))
          .then((r) => {
            latencies.push(performance.now() - start);
            ok++;
            if (r.bytes.byteLength === 0) err++;
          })
          .catch(() => {
            err++;
          })
          .finally(() => {
            inFlight--;
            if (queue.length === 0 && inFlight === 0) resolve();
            else launch();
          });
      }
    };
    launch();
  });

  latencies.sort((a, b) => a - b);
  const elapsedSec = (performance.now() - t0) / 1000;
  const mean = latencies.reduce((s, v) => s + v, 0) / (latencies.length || 1);
  return {
    label,
    n: total,
    conc: concurrency,
    ok,
    err,
    elapsedSec,
    rps: ok / elapsedSec,
    mean,
    p50: pct(latencies, 0.5),
    p95: pct(latencies, 0.95),
    p99: pct(latencies, 0.99),
  };
}

function fmt(r: Result): string {
  return (
    `  ${r.label.padEnd(36)} n=${r.n} conc=${r.conc} ok=${r.ok} err=${r.err} ` +
    `elapsed=${r.elapsedSec.toFixed(2)}s rps=${r.rps.toFixed(1)} ` +
    `p50=${r.p50.toFixed(0)}ms p95=${r.p95.toFixed(0)}ms p99=${r.p99.toFixed(0)}ms`
  );
}

async function main() {
  const proxyClient = new SuiArchiveClient({ mode: "proxy" });
  const directClient = new SuiArchiveClient({ mode: "direct" });

  // Prime routing caches (cheap, one /epochs call each).
  const routing = await proxyClient.listEpochs();
  await directClient.listEpochs();
  const maxSeq = routing[routing.length - 1]!.last_seq;
  console.log(`# routing primed: ${routing.length} epochs, maxSeq=${maxSeq}`);
  console.log();

  const rnd = () => Math.floor(Math.random() * maxSeq);

  // ── Mode 1: proxy only ───────────────────────────────────────────────
  console.log("# Mode 1: proxy (always /:seq on Worker)");
  console.log(fmt(await runBatch(proxyClient, "proxy cold conc=128", 1500, 128, rnd)));
  console.log();

  // ── Mode 2: direct-R2, cold idx ──────────────────────────────────────
  console.log("# Mode 2: direct-R2, cold idx (idx fetched per-call first time)");
  console.log(fmt(await runBatch(directClient, "direct cold-idx conc=128", 1500, 128, rnd)));
  console.log();

  // ── Mode 3: direct-R2, warmed idx ────────────────────────────────────
  console.log("# Mode 3: warming all 1103 idx files...");
  const t0 = performance.now();
  await directClient.warmup({ epochs: "all", idxConcurrency: 32 });
  const warmSec = ((performance.now() - t0) / 1000).toFixed(1);
  const stats = await directClient.stats();
  console.log(`  warmup done in ${warmSec}s, idxCached=${stats.idxCached}`);
  console.log(fmt(await runBatch(directClient, "direct warm-idx conc=128", 1500, 128, rnd)));
  console.log(fmt(await runBatch(directClient, "direct warm-idx conc=256", 1500, 256, rnd)));
  console.log();

  // ── Baseline throughput (warm cache, same URL) ───────────────────────
  const hot = 1_234_567;
  console.log("# Warm edge cache (same URL, no local work)");
  console.log(fmt(await runBatch(proxyClient, "proxy warm conc=256", 20000, 256, () => hot)));
  console.log(fmt(await runBatch(directClient, "direct warm conc=256", 20000, 256, () => hot)));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
