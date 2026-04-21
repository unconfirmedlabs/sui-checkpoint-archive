-- Routing table for checkpoint proxy.
-- One row per Sui epoch we've ingested into R2. Per-checkpoint offsets
-- live in the `.idx` file alongside each `.zst` archive, not here.

CREATE TABLE epochs (
    epoch      INTEGER PRIMARY KEY,
    first_seq  INTEGER NOT NULL,
    last_seq   INTEGER NOT NULL,
    count      INTEGER NOT NULL,
    zst_key    TEXT NOT NULL,
    idx_key    TEXT NOT NULL,
    zst_bytes  INTEGER NOT NULL,
    idx_bytes  INTEGER NOT NULL,
    zst_sha256 TEXT NOT NULL,
    idx_sha256 TEXT NOT NULL,
    zst_parts  INTEGER NOT NULL,
    ingested_at INTEGER NOT NULL
);

-- Fast path: find the epoch row containing a given checkpoint seq.
--   SELECT * FROM epochs WHERE first_seq <= ? ORDER BY first_seq DESC LIMIT 1
CREATE INDEX idx_epochs_first_seq ON epochs(first_seq);
