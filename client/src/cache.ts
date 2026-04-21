/**
 * Two-layer cache: in-memory always, disk optionally.
 *
 * - Routing snapshot (array of EpochMetadata) lives in memory with a TTL.
 *   If cacheDir is set, it's also persisted to `routing.json` and
 *   reloaded on construction.
 * - Per-epoch .idx files are cached in memory (recent-used) and on disk
 *   (permanent, until explicitly cleared). Idx files are immutable by
 *   construction so they never need to be refreshed.
 */

import { homedir } from "node:os";
import { join } from "node:path";
import { mkdirSync, readFileSync, writeFileSync, readdirSync, statSync, existsSync, unlinkSync, rmSync } from "node:fs";
import type { EpochMetadata } from "./types.ts";

export interface RoutingSnapshot {
  fetchedAt: number; // ms since epoch
  epochs: EpochMetadata[]; // sorted by epoch asc
}

export class ClientCache {
  private routing: RoutingSnapshot | null = null;
  private idxMem: Map<number, Uint8Array> = new Map(); // epoch -> idx bytes

  constructor(readonly dir: string | null) {
    if (this.dir) {
      mkdirSync(this.idxDirPath(), { recursive: true });
      this.loadRoutingFromDisk();
    }
  }

  // ── Routing ────────────────────────────────────────────────────────────

  getRouting(): RoutingSnapshot | null {
    return this.routing;
  }

  setRouting(epochs: EpochMetadata[]): void {
    this.routing = { fetchedAt: Date.now(), epochs };
    if (this.dir) {
      writeFileSync(this.routingPath(), JSON.stringify(this.routing), "utf8");
    }
  }

  private loadRoutingFromDisk(): void {
    if (!this.dir) return;
    const p = this.routingPath();
    if (!existsSync(p)) return;
    try {
      const parsed = JSON.parse(readFileSync(p, "utf8")) as RoutingSnapshot;
      if (Array.isArray(parsed.epochs)) this.routing = parsed;
    } catch {
      // corrupt cache; ignore, will refetch.
    }
  }

  // ── Idx ────────────────────────────────────────────────────────────────

  getIdx(epoch: number): Uint8Array | null {
    const mem = this.idxMem.get(epoch);
    if (mem) return mem;
    if (!this.dir) return null;
    const p = this.idxPath(epoch);
    if (!existsSync(p)) return null;
    const bytes = readFileSync(p);
    this.idxMem.set(epoch, bytes);
    return bytes;
  }

  setIdx(epoch: number, bytes: Uint8Array): void {
    this.idxMem.set(epoch, bytes);
    if (this.dir) writeFileSync(this.idxPath(epoch), bytes);
  }

  // ── Paths + helpers ────────────────────────────────────────────────────

  private routingPath(): string {
    if (!this.dir) throw new Error("no cache dir configured");
    return join(this.dir, "routing.json");
  }

  private idxDirPath(): string {
    if (!this.dir) throw new Error("no cache dir configured");
    return join(this.dir, "idx");
  }

  private idxPath(epoch: number): string {
    return join(this.idxDirPath(), `epoch-${epoch}.idx`);
  }

  // ── Stats ──────────────────────────────────────────────────────────────

  diskStats(): { dir: string; routingExists: boolean; idxFiles: number; totalBytes: number } | null {
    if (!this.dir) return null;
    const rp = this.routingPath();
    const routingExists = existsSync(rp);
    let idxFiles = 0;
    let totalBytes = routingExists ? statSync(rp).size : 0;
    const idxDir = this.idxDirPath();
    if (existsSync(idxDir)) {
      for (const f of readdirSync(idxDir)) {
        if (!f.endsWith(".idx")) continue;
        idxFiles++;
        totalBytes += statSync(join(idxDir, f)).size;
      }
    }
    return { dir: this.dir, routingExists, idxFiles, totalBytes };
  }

  memIdxCount(): number {
    return this.idxMem.size;
  }

  clear(): void {
    this.routing = null;
    this.idxMem.clear();
    if (!this.dir) return;
    const rp = this.routingPath();
    if (existsSync(rp)) unlinkSync(rp);
    const idxDir = this.idxDirPath();
    if (existsSync(idxDir)) rmSync(idxDir, { recursive: true, force: true });
    mkdirSync(idxDir, { recursive: true });
  }
}

/** Expand `~` at the start of a path. */
export function expandTilde(path: string): string {
  if (path === "~") return homedir();
  if (path.startsWith("~/")) return join(homedir(), path.slice(2));
  return path;
}
