#!/usr/bin/env bun
/**
 * Minimal CLI for @unconfirmedlabs/sui-checkpoint-archive.
 *
 * Usage:
 *   sui-archive warmup [--recent N | --all]
 *   sui-archive get <seq>              # writes raw .binpb.zst bytes to stdout
 *   sui-archive epochs [--json]
 *   sui-archive stats
 *   sui-archive clear
 *
 * Env:
 *   SUI_ARCHIVE_URL        override proxy base URL
 *   SUI_ARCHIVE_CACHE_DIR  persistent cache directory (default: ~/.cache/sui-archive)
 */

import { SuiArchiveClient } from "./client.ts";

const argv = process.argv.slice(2);
const cmd = argv[0] ?? "";
const rest = argv.slice(1);

const client = new SuiArchiveClient({
  baseUrl: process.env.SUI_ARCHIVE_URL,
  cacheDir: process.env.SUI_ARCHIVE_CACHE_DIR ?? "~/.cache/sui-archive",
});

function flag(args: string[], name: string): boolean {
  return args.includes(`--${name}`);
}

function opt(args: string[], name: string): string | undefined {
  const i = args.indexOf(`--${name}`);
  if (i === -1 || i + 1 >= args.length) return undefined;
  return args[i + 1];
}

function printUsage(): void {
  process.stderr.write(
    [
      "sui-archive — client for the Sui checkpoint archive",
      "",
      "commands:",
      "  warmup [--recent N | --all]   prefetch routing and idx files",
      "  get <seq>                     write raw .binpb.zst bytes to stdout",
      "  epochs [--json]               list indexed epochs",
      "  stats                         show cache state",
      "  clear                         nuke the local cache",
      "",
    ].join("\n"),
  );
}

async function main(): Promise<number> {
  switch (cmd) {
    case "warmup": {
      const all = flag(rest, "all");
      const recentRaw = opt(rest, "recent");
      const spec = all ? ("all" as const) : "recent" as const;
      const recent = recentRaw ? parseInt(recentRaw, 10) : 100;
      process.stderr.write(`warming up (${all ? "all" : `recent ${recent}`})...\n`);
      const t0 = performance.now();
      await client.warmup({ epochs: spec, recent });
      const dt = ((performance.now() - t0) / 1000).toFixed(1);
      const s = await client.stats();
      process.stderr.write(
        `done in ${dt}s — epochs=${s.epochs} idx cached=${s.idxCached}` +
          (s.disk ? ` disk=${(s.disk.totalBytes / 1e6).toFixed(1)}MB` : "") +
          "\n",
      );
      return 0;
    }
    case "get": {
      const raw = rest[0];
      if (!raw) {
        process.stderr.write("usage: sui-archive get <seq>\n");
        return 1;
      }
      const seq = Number(raw);
      if (!Number.isFinite(seq) || seq < 0) {
        process.stderr.write("bad seq\n");
        return 1;
      }
      const { bytes, epoch } = await client.getCheckpoint(seq);
      process.stderr.write(`got seq=${seq} epoch=${epoch} bytes=${bytes.byteLength}\n`);
      process.stdout.write(bytes);
      return 0;
    }
    case "epochs": {
      const rows = await client.listEpochs();
      if (flag(rest, "json")) {
        process.stdout.write(JSON.stringify(rows, null, 2));
        process.stdout.write("\n");
      } else {
        process.stdout.write(
          ["epoch\tfirst_seq\tlast_seq\tcount\tzst_MB"].join("\t") + "\n",
        );
        for (const r of rows) {
          process.stdout.write(
            [r.epoch, r.first_seq, r.last_seq, r.count, (r.zst_bytes / 1e6).toFixed(1)].join("\t") + "\n",
          );
        }
      }
      return 0;
    }
    case "stats": {
      const s = await client.stats();
      process.stdout.write(JSON.stringify(s, null, 2) + "\n");
      return 0;
    }
    case "clear": {
      client.clearCache();
      process.stderr.write("cache cleared\n");
      return 0;
    }
    case "":
    case "help":
    case "--help":
    case "-h":
      printUsage();
      return 0;
    default:
      process.stderr.write(`unknown command: ${cmd}\n`);
      printUsage();
      return 1;
  }
}

main()
  .then((code) => process.exit(code))
  .catch((err) => {
    process.stderr.write(`error: ${err instanceof Error ? err.message : String(err)}\n`);
    process.exit(1);
  });
