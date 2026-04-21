-- Committee blob for each epoch: used by the Worker's optional verify path.
-- Packed binary layout (matches the Zig C ABI):
--   u32 n_validators | n × (96-byte BLS pubkey | u64 LE stake)
-- Size: ~10–16 KB per epoch (tens of KB total for Sui's lifetime).

ALTER TABLE epochs ADD COLUMN committee BLOB;
