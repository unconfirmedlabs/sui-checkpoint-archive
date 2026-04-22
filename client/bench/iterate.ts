/**
 * Benchmark + correctness check for SuiArchiveClient.iterateEpoch.
 *
 * Usage (all args optional):
 *   bun bench/iterate.ts --epoch 500 --concurrency 8 --chunk-mb 16
 *
 * Or sweep mode (runs a matrix of concurrency x chunk):
 *   bun bench/iterate.ts --sweep --epochs 100,500,1000
 */

import { SuiArchiveClient } from "../src/index.ts";

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  if (i !== -1 && i + 1 < process.argv.length) return process.argv[i + 1];
  return process.env[name.toUpperCase().replace(/-/g, "_")];
}
function flag(name: string): boolean {
  if (process.argv.includes(`--${name}`)) return true;
  const v = process.env[name.toUpperCase().replace(/-/g, "_")];
  return v === "1" || v === "true";
}

interface Result {
  epoch: number;
  concurrency: number;
  chunkMB: number;
  frames: number;
  decompressedMB: number;
  elapsedSec: number;
  throughputMBps: number;
  framesPerSec: number;
  peakRssMB: number;
  ok: boolean;
  firstSeq: number;
  lastSeq: number;
  startSeq: number;
  endSeq: number;
}

async function runOne(epoch: number, concurrency: number, chunkMB: number): Promise<Result> {
  const client = new SuiArchiveClient();

  // Cross-check: fetch metadata for correctness verification.
  const meta = await client.findEpochByNumber(epoch);
  if (!meta) throw new Error(`epoch ${epoch} not indexed`);

  const startSeq = arg("start-seq") !== undefined ? parseInt(arg("start-seq")!, 10) : undefined;
  const endSeq = arg("end-seq") !== undefined ? parseInt(arg("end-seq")!, 10) : undefined;

  const t0 = performance.now();
  let rssMax = process.memoryUsage().rss;
  let frames = 0;
  let decompressedBytes = 0;
  let firstSeq = -1;
  let lastSeq = -1;
  let prev = -1;
  let ok = true;

  const rssTimer = setInterval(() => {
    const rss = process.memoryUsage().rss;
    if (rss > rssMax) rssMax = rss;
  }, 100);

  try {
    for await (const { seq, bytes } of client.iterateEpoch(epoch, { concurrency, startSeq, endSeq })) {
      if (firstSeq === -1) firstSeq = seq;
      lastSeq = seq;
      if (prev !== -1 && seq !== prev + 1) {
        console.error(`  [!] non-monotonic seq at ${seq} (prev ${prev})`);
        ok = false;
      }
      prev = seq;
      frames++;
      decompressedBytes += bytes.byteLength;
      if (frames % 20000 === 0) {
        const elapsed = (performance.now() - t0) / 1000;
        const rss = process.memoryUsage().rss / (1024 * 1024);
        process.stderr.write(
          `  [${frames.toLocaleString()} frames, ` +
          `${(decompressedBytes / 1024 / 1024).toFixed(0)}MB, ` +
          `${(decompressedBytes / 1024 / 1024 / elapsed).toFixed(0)}MB/s, ` +
          `rss=${rss.toFixed(0)}MB, seq=${seq}]\n`,
        );
      }
    }
  } finally {
    clearInterval(rssTimer);
  }

  const elapsedSec = (performance.now() - t0) / 1000;

  // Verify against idx/meta. Skip full-epoch checks when a seq range is specified.
  const isRange = startSeq !== undefined || endSeq !== undefined;
  if (!isRange) {
    const idx = await client.getEpochIdx(epoch);
    const expectedFrames = idx.byteLength / 20;
    if (frames !== expectedFrames) {
      console.error(`  [!] frame count mismatch: got ${frames}, expected ${expectedFrames}`);
      ok = false;
    }
    if (firstSeq !== meta.first_seq) {
      console.error(`  [!] firstSeq mismatch: got ${firstSeq}, expected ${meta.first_seq}`);
      ok = false;
    }
    if (lastSeq !== meta.last_seq) {
      console.error(`  [!] lastSeq mismatch: got ${lastSeq}, expected ${meta.last_seq}`);
      ok = false;
    }
  }

  const decompressedMB = decompressedBytes / (1024 * 1024);
  return {
    epoch,
    concurrency,
    chunkMB,
    frames,
    decompressedMB,
    elapsedSec,
    throughputMBps: decompressedMB / elapsedSec,
    framesPerSec: frames / elapsedSec,
    peakRssMB: rssMax / (1024 * 1024),
    ok,
    firstSeq,
    lastSeq,
    startSeq: startSeq ?? firstSeq,
    endSeq: endSeq ?? lastSeq,
  };
}

function fmt(r: Result): string {
  const range = r.startSeq !== r.firstSeq || r.endSeq !== r.lastSeq
    ? ` seq=${r.startSeq}-${r.endSeq}`
    : "";
  return (
    `  epoch=${r.epoch.toString().padStart(4)} ` +
    `conc=${r.concurrency.toString().padStart(2)} ` +
    `chunk=${r.chunkMB.toString().padStart(3)}MB ` +
    `frames=${r.frames.toString().padStart(7)} ` +
    `decomp=${r.decompressedMB.toFixed(0).padStart(5)}MB ` +
    `elapsed=${r.elapsedSec.toFixed(1).padStart(6)}s ` +
    `thru=${r.throughputMBps.toFixed(0).padStart(4)}MB/s ` +
    `ckpts/s=${r.framesPerSec.toFixed(0).padStart(5)} ` +
    `rss=${r.peakRssMB.toFixed(0).padStart(4)}MB ` +
    `${r.ok ? "OK" : "FAIL"}${range}`
  );
}

async function main() {
  if (flag("sweep")) {
    const epochsArg = arg("epochs") ?? "100,500,1000";
    const epochs = epochsArg.split(",").map((s) => parseInt(s, 10));
    const concurrencies = [1, 2, 4, 8, 16];
    const chunkMBs = [4, 16, 64];
    console.log("# sweep");
    for (const epoch of epochs) {
      for (const conc of concurrencies) {
        for (const chunkMB of chunkMBs) {
          const r = await runOne(epoch, conc, chunkMB);
          console.log(fmt(r));
        }
      }
    }
    return;
  }

  const epoch = parseInt(arg("epoch") ?? "500", 10);
  const concurrency = parseInt(arg("concurrency") ?? "8", 10);
  const chunkMB = parseInt(arg("chunk-mb") ?? "16", 10);
  const r = await runOne(epoch, concurrency, chunkMB);
  console.log(fmt(r));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
