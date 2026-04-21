export class ArchiveError extends Error {
  constructor(message: string, override readonly cause?: unknown) {
    super(message);
    this.name = "ArchiveError";
  }
}

/** Checkpoint sequence is out of the indexed range or no such epoch. */
export class NotIndexedError extends ArchiveError {
  constructor(readonly seq: number) {
    super(`checkpoint ${seq} is not in the archive`);
    this.name = "NotIndexedError";
  }
}

/** HTTP error from the proxy. */
export class HttpError extends ArchiveError {
  constructor(readonly status: number, readonly url: string, body: string) {
    super(`HTTP ${status} on ${url}: ${body.slice(0, 200)}`);
    this.name = "HttpError";
  }
}

/** Idx entry returned a sequence that doesn't match what we requested. */
export class IdxMismatchError extends ArchiveError {
  constructor(expected: bigint, got: bigint, key: string) {
    super(`idx mismatch in ${key}: requested ${expected}, got ${got}`);
    this.name = "IdxMismatchError";
  }
}
