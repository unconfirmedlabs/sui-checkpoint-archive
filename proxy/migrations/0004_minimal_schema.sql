-- Reduce `epochs` to the minimum routing + integrity columns. Every dropped
-- column is either derivable (count, zst_key, idx_key, idx_bytes) or was only
-- ever ingest-time metadata (zst_parts, ingested_at).
--
-- Derivations the proxy now does inline:
--   count      = last_seq - first_seq + 1
--   zst_key    = 'epoch-' || epoch || '.zst'
--   idx_key    = 'epoch-' || epoch || '.idx'
--   idx_bytes  = count * 20

ALTER TABLE epochs DROP COLUMN count;
ALTER TABLE epochs DROP COLUMN zst_key;
ALTER TABLE epochs DROP COLUMN idx_key;
ALTER TABLE epochs DROP COLUMN idx_bytes;
ALTER TABLE epochs DROP COLUMN zst_parts;
ALTER TABLE epochs DROP COLUMN ingested_at;
