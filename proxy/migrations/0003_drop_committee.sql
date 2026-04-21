-- Committee data lives in the archive itself (epoch N's committee is embedded
-- in the end-of-epoch checkpoint of epoch N-1). The ingester verifies against
-- it at ingest; the proxy no longer needs it for on-demand verification.

ALTER TABLE epochs DROP COLUMN committee;
