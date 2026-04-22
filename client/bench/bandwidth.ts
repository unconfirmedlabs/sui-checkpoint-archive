/**
 * Pure network bandwidth probe. No decompression. Exposes:
 * - single full-object fetch
 * - parallel range GETs at various concurrency
 * - error/status tracking so we see rate limits
 * - cache-hint inspection
 */

const URL = process.env.URL ??
  "https://archive.checkpoints.mainnet.sui.unconfirmed.cloud/epoch-500.zst";
const TOTAL_SIZE = parseInt(process.env.TOTAL_SIZE ?? "7870055916", 10);

async function drain(stream: ReadableStream<Uint8Array>): Promise<number> {
  const reader = stream.getReader();
  let total = 0;
  while (true) {
    const { value, done } = await reader.read();
    if (done) return total;
    total += value.byteLength;
  }
}

async function singleStream(url: string) {
  const t0 = performance.now();
  const resp = await fetch(url);
  const cfCache = resp.headers.get("cf-cache-status");
  const server = resp.headers.get("server");
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  const bytes = await drain(resp.body!);
  const elapsedSec = (performance.now() - t0) / 1000;
  const mbps = bytes / 1024 / 1024 / elapsedSec;
  console.log(
    `  single-stream                            ` +
    `${(bytes / 1024 / 1024).toFixed(0).padStart(5)}MB in ${elapsedSec.toFixed(1)}s ` +
    `-> ${mbps.toFixed(0)}MB/s  cf-cache=${cfCache ?? "?"}`,
  );
}

async function multipart(
  url: string,
  totalSize: number,
  concurrency: number,
  chunkBytes: number,
) {
  const chunks: { start: number; end: number }[] = [];
  for (let s = 0; s < totalSize; s += chunkBytes) {
    chunks.push({ start: s, end: Math.min(s + chunkBytes - 1, totalSize - 1) });
  }

  let bytesRead = 0;
  let ok = 0;
  let err = 0;
  const statuses: Record<number, number> = {};
  let cfCacheHits: Record<string, number> = {};

  const t0 = performance.now();
  const pool: Promise<void>[] = [];
  let next = 0;

  async function one(c: { start: number; end: number }): Promise<void> {
    try {
      const r = await fetch(url, { headers: { range: `bytes=${c.start}-${c.end}` } });
      statuses[r.status] = (statuses[r.status] ?? 0) + 1;
      const cc = r.headers.get("cf-cache-status") ?? "-";
      cfCacheHits[cc] = (cfCacheHits[cc] ?? 0) + 1;
      if (!r.ok && r.status !== 206) {
        err++;
        return;
      }
      const n = await drain(r.body!);
      bytesRead += n;
      ok++;
    } catch (e) {
      err++;
    }
  }

  const refill = () => {
    while (pool.length < concurrency && next < chunks.length) {
      const c = chunks[next++]!;
      const p = one(c);
      pool.push(p);
    }
  };

  refill();
  while (pool.length > 0) {
    await pool.shift();
    refill();
  }

  const elapsedSec = (performance.now() - t0) / 1000;
  const mbps = bytesRead / 1024 / 1024 / elapsedSec;
  const totalExpected = totalSize;
  const completeness = (bytesRead / totalExpected * 100).toFixed(0);
  console.log(
    `  conc=${concurrency.toString().padStart(3)} ` +
    `${(bytesRead / 1024 / 1024).toFixed(0).padStart(5)}MB ` +
    `(${completeness}% of full) ` +
    `elapsed=${elapsedSec.toFixed(1).padStart(5)}s ` +
    `mbps=${mbps.toFixed(0).padStart(4)} ` +
    `ok=${ok}/${chunks.length} err=${err}  ` +
    `status=${JSON.stringify(statuses)} cf=${JSON.stringify(cfCacheHits)}`,
  );
}

async function main() {
  const sizeGB = (TOTAL_SIZE / 1024 / 1024 / 1024).toFixed(2);
  console.log(`# bandwidth probe: ${URL}`);
  console.log(`# expected ${sizeGB} GB`);
  console.log();

  console.log("# single full-file stream (cold cache likely):");
  await singleStream(URL);
  console.log();

  console.log("# multipart range reads, 16 MB chunks:");
  for (const c of [1, 4, 8, 16, 32, 64]) {
    await multipart(URL, TOTAL_SIZE, c, 16 * 1024 * 1024);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
