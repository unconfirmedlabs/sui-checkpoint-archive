//! Sui checkpoint verifier.
//!
//! Thin wrapper over the official MystenLabs crates (sui-sdk-types +
//! fastcrypto) plus a minimal protobuf parser and roaring-bitmap decoder.
//!
//! Dual-target:
//!   * **native** (default): used by the ingester via normal Rust calls.
//!   * **wasm32**: compiled to `wasm32-unknown-unknown` with an extern-C ABI
//!     (matches the Zig light-client ABI) for loading into a Cloudflare
//!     Worker. Enable the `wasm` feature.
//!
//! Committee packed layout (identical to the Zig light client):
//!   u32 n_validators                 (LE)
//!   for each:
//!     96-byte BLS12-381 G2 pubkey
//!     u64 stake                      (LE)

#![cfg_attr(all(target_arch = "wasm32", not(test)), no_main)]

use anyhow::{Context, Result};

pub const ENTRY_SIZE: usize = 96 + 8;
pub const INTENT_PREFIX: [u8; 3] = [2, 0, 0]; // CheckpointSummary, V0, Sui

/// Decompress a zstd stream (possibly multi-frame) into a single Vec.
fn zstd_decode(zst: &[u8]) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut decoder = ruzstd::decoding::StreamingDecoder::new(zst)
        .map_err(|e| anyhow::anyhow!("zstd init: {e:?}"))?;
    let mut out = Vec::new();
    decoder
        .read_to_end(&mut out)
        .map_err(|e| anyhow::anyhow!("zstd read: {e}"))?;
    Ok(out)
}

// ── Protobuf parser (only the fields we need) ────────────────────────────────

pub struct ParsedProto<'a> {
    pub summary_bcs: &'a [u8],
    pub sig_epoch: u64,
    pub sig: [u8; 48],
    pub bitmap: &'a [u8],
}

fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let b = *data.get(*pos).context("proto: unexpected end")?;
        *pos += 1;
        result |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift = shift.checked_add(7).context("proto: varint overflow")?;
        if shift >= 64 {
            anyhow::bail!("proto: varint overflow");
        }
    }
}

fn skip_field(data: &[u8], pos: &mut usize, wire_type: u8) -> Result<()> {
    match wire_type {
        0 => {
            read_varint(data, pos)?;
        }
        1 => {
            if *pos + 8 > data.len() {
                anyhow::bail!("proto: unexpected end");
            }
            *pos += 8;
        }
        2 => {
            let len = read_varint(data, pos)? as usize;
            if *pos + len > data.len() {
                anyhow::bail!("proto: unexpected end");
            }
            *pos += len;
        }
        5 => {
            if *pos + 4 > data.len() {
                anyhow::bail!("proto: unexpected end");
            }
            *pos += 4;
        }
        _ => anyhow::bail!("proto: unsupported wire type {wire_type}"),
    }
    Ok(())
}

fn extract_bcs_value(data: &[u8]) -> Result<Option<&[u8]>> {
    let mut pos = 0usize;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if wire_type != 2 {
            skip_field(data, &mut pos, wire_type)?;
            continue;
        }
        let len = read_varint(data, &mut pos)? as usize;
        if pos + len > data.len() {
            anyhow::bail!("proto: unexpected end");
        }
        let sub = &data[pos..pos + len];
        pos += len;
        if field_num != 1 {
            continue;
        }

        let mut p2 = 0usize;
        while p2 < sub.len() {
            let t2 = read_varint(sub, &mut p2)?;
            let fn2 = (t2 >> 3) as u32;
            let wt2 = (t2 & 7) as u8;
            if wt2 != 2 {
                skip_field(sub, &mut p2, wt2)?;
                continue;
            }
            let l2 = read_varint(sub, &mut p2)? as usize;
            if p2 + l2 > sub.len() {
                anyhow::bail!("proto: unexpected end");
            }
            let val = &sub[p2..p2 + l2];
            p2 += l2;
            if fn2 == 2 {
                return Ok(Some(val));
            }
        }
    }
    Ok(None)
}

pub fn parse_proto(data: &[u8]) -> Result<ParsedProto<'_>> {
    let mut summary_bcs: Option<&[u8]> = None;
    let mut sig_epoch: u64 = 0;
    let mut sig: Option<[u8; 48]> = None;
    let mut bitmap: Option<&[u8]> = None;

    let mut pos = 0usize;
    while pos < data.len() {
        let tag = read_varint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 7) as u8;
        if wire_type != 2 {
            skip_field(data, &mut pos, wire_type)?;
            continue;
        }
        let len = read_varint(data, &mut pos)? as usize;
        if pos + len > data.len() {
            anyhow::bail!("proto: unexpected end");
        }
        let payload = &data[pos..pos + len];
        pos += len;

        match field_num {
            3 => summary_bcs = extract_bcs_value(payload)?,
            4 => {
                let mut p2 = 0usize;
                while p2 < payload.len() {
                    let t2 = read_varint(payload, &mut p2)?;
                    let fn2 = (t2 >> 3) as u32;
                    let wt2 = (t2 & 7) as u8;
                    if wt2 == 0 {
                        let v = read_varint(payload, &mut p2)?;
                        if fn2 == 1 {
                            sig_epoch = v;
                        }
                    } else if wt2 == 2 {
                        let l2 = read_varint(payload, &mut p2)? as usize;
                        if p2 + l2 > payload.len() {
                            anyhow::bail!("proto: unexpected end");
                        }
                        let sub = &payload[p2..p2 + l2];
                        p2 += l2;
                        if fn2 == 2 {
                            if sub.len() != 48 {
                                anyhow::bail!("sig length {} != 48", sub.len());
                            }
                            let mut arr = [0u8; 48];
                            arr.copy_from_slice(sub);
                            sig = Some(arr);
                        } else if fn2 == 3 {
                            bitmap = Some(sub);
                        }
                    } else {
                        skip_field(payload, &mut p2, wt2)?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ParsedProto {
        summary_bcs: summary_bcs.context("proto: missing summary_bcs")?,
        sig_epoch,
        sig: sig.context("proto: missing sig")?,
        bitmap: bitmap.context("proto: missing bitmap")?,
    })
}

// ── Committee pack / unpack ──────────────────────────────────────────────────

pub fn pack_committee(validators: &[(Vec<u8>, u64)]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + validators.len() * ENTRY_SIZE);
    out.extend_from_slice(&(validators.len() as u32).to_le_bytes());
    for (pk, stake) in validators {
        assert_eq!(pk.len(), 96, "pubkey must be 96 bytes");
        out.extend_from_slice(pk);
        out.extend_from_slice(&stake.to_le_bytes());
    }
    out
}

fn unpack_committee(packed: &[u8]) -> Result<Vec<(&[u8], u64)>> {
    if packed.len() < 4 {
        anyhow::bail!("committee: too small");
    }
    let n = u32::from_le_bytes(packed[0..4].try_into().unwrap()) as usize;
    let body = &packed[4..];
    if body.len() != n * ENTRY_SIZE {
        anyhow::bail!(
            "committee: expected {} body bytes, got {}",
            n * ENTRY_SIZE,
            body.len()
        );
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let off = i * ENTRY_SIZE;
        let pk = &body[off..off + 96];
        let stake = u64::from_le_bytes(body[off + 96..off + 104].try_into().unwrap());
        out.push((pk, stake));
    }
    Ok(out)
}

// ── RoaringBitmap signer decoder (strict: key=0, stored offsets verified) ────

fn decode_roaring_signers(data: &[u8]) -> Result<Vec<usize>> {
    const SERIAL_COOKIE_NO_RUN: u32 = 12346;
    if data.len() < 8 {
        anyhow::bail!("roaring: truncated");
    }
    let cookie = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if cookie != SERIAL_COOKIE_NO_RUN {
        anyhow::bail!("roaring: unsupported cookie {cookie}");
    }
    let num_containers = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    let header_end = 8 + num_containers * 8;
    if data.len() < header_end {
        anyhow::bail!("roaring: truncated header");
    }

    let mut out = Vec::new();
    let mut offset = header_end;

    for i in 0..num_containers {
        let hdr = 8 + i * 4;
        let key = u16::from_le_bytes(data[hdr..hdr + 2].try_into().unwrap());
        if key != 0 {
            anyhow::bail!("roaring: non-zero key {key} not supported");
        }
        let stored_offset_pos = 8 + num_containers * 4 + i * 4;
        let stored_offset = u32::from_le_bytes(
            data[stored_offset_pos..stored_offset_pos + 4].try_into().unwrap(),
        ) as usize;
        if stored_offset != offset {
            anyhow::bail!(
                "roaring: offset mismatch (stored {stored_offset}, expected {offset})"
            );
        }
        let cardinality = u16::from_le_bytes(data[hdr + 2..hdr + 4].try_into().unwrap()) as usize + 1;

        if cardinality <= 4096 {
            let needed = cardinality * 2;
            if offset + needed > data.len() {
                anyhow::bail!("roaring: truncated array container");
            }
            for j in 0..cardinality {
                let v = u16::from_le_bytes(
                    data[offset + j * 2..offset + j * 2 + 2].try_into().unwrap(),
                );
                out.push(v as usize);
            }
            offset += needed;
        } else {
            if offset + 1024 > data.len() {
                anyhow::bail!("roaring: truncated bitset container");
            }
            for bit in 0..8192 {
                if data[offset + bit / 8] & (1u8 << (bit % 8)) != 0 {
                    out.push(bit);
                }
            }
            offset += 1024;
        }
    }
    Ok(out)
}

// ── Public native API ────────────────────────────────────────────────────────

/// Decompress + parse a checkpoint `.binpb.zst`, extract the
/// `next_epoch_committee` from `end_of_epoch_data`, pack into the canonical
/// wire layout.
pub fn extract_next_committee_from_zst(zst: &[u8]) -> Result<Vec<u8>> {
    let proto = zstd_decode(zst).context("zstd decompress")?;
    extract_next_committee(&proto)
}

pub fn extract_next_committee(proto: &[u8]) -> Result<Vec<u8>> {
    let parsed = parse_proto(proto)?;
    let summary: sui_sdk_types::CheckpointSummary =
        bcs::from_bytes(parsed.summary_bcs).context("bcs decode CheckpointSummary")?;
    let eoe = summary
        .end_of_epoch_data
        .as_ref()
        .context("not an end-of-epoch checkpoint")?;
    let validators: Vec<(Vec<u8>, u64)> = eoe
        .next_epoch_committee
        .iter()
        .map(|m| (m.public_key.inner().to_vec(), m.stake))
        .collect();
    Ok(pack_committee(&validators))
}

/// Domain separation tag for Sui's BLS12-381 min-sig signatures. Sig lives
/// in G1, message is hashed to G1; DST names the hash-to-curve variant.
pub const SUI_DST: &[u8] = b"BLS_SIG_BLS12381G1_XMD:SHA-256_SSWU_RO_NUL_";

/// Verify a decompressed checkpoint proto against a packed committee.
/// Canonical Sui message: `[2,0,0] ++ bcs(summary) ++ u64_le(epoch)`.
pub fn verify_checkpoint(
    proto: &[u8],
    committee_packed: &[u8],
    committee_epoch: u64,
) -> Result<()> {
    use blst::min_sig::{AggregatePublicKey, PublicKey, Signature};
    use blst::BLST_ERROR;

    let parsed = parse_proto(proto)?;
    let validators = unpack_committee(committee_packed)?;

    if parsed.sig_epoch != committee_epoch {
        anyhow::bail!(
            "wrong epoch: sig={} committee={}",
            parsed.sig_epoch,
            committee_epoch
        );
    }

    let signers = decode_roaring_signers(parsed.bitmap)?;
    let total_stake: u64 = validators.iter().map(|(_, s)| *s).sum();
    let signed_stake: u64 = signers
        .iter()
        .map(|&i| {
            validators
                .get(i)
                .map(|(_, s)| *s)
                .ok_or_else(|| anyhow::anyhow!("signer index {i} out of range"))
        })
        .sum::<Result<_>>()?;
    if signed_stake * 3 < total_stake * 2 + 1 {
        anyhow::bail!("insufficient stake: {signed_stake}/{total_stake}");
    }

    // Aggregate the signing pubkeys (with subgroup + infinity checks). Sui
    // enforces proof-of-possession when committees rotate, so we trust the
    // pubkeys are individually valid G2 points; but we still deserialise
    // strictly — a malformed committee row should never verify.
    let pks: Vec<PublicKey> = signers
        .iter()
        .map(|&i| {
            PublicKey::from_bytes(validators[i].0)
                .map_err(|e| anyhow::anyhow!("bad pubkey: {:?}", e))
        })
        .collect::<Result<_>>()?;
    if pks.is_empty() {
        anyhow::bail!("no signers");
    }
    let mut agg = AggregatePublicKey::from_public_key(&pks[0]);
    let pk_refs: Vec<&PublicKey> = pks.iter().skip(1).collect();
    for pk in pk_refs {
        agg.add_public_key(pk, true)
            .map_err(|e| anyhow::anyhow!("aggregate pk: {:?}", e))?;
    }
    let agg_pk = agg.to_public_key();

    let sig = Signature::from_bytes(&parsed.sig)
        .map_err(|e| anyhow::anyhow!("bad sig: {:?}", e))?;

    let mut msg = Vec::with_capacity(INTENT_PREFIX.len() + parsed.summary_bcs.len() + 8);
    msg.extend_from_slice(&INTENT_PREFIX);
    msg.extend_from_slice(parsed.summary_bcs);
    msg.extend_from_slice(&committee_epoch.to_le_bytes());

    let result = sig.verify(true, &msg, SUI_DST, &[], &agg_pk, true);
    if result != BLST_ERROR::BLST_SUCCESS {
        anyhow::bail!("aggregate verify failed: {:?}", result);
    }
    Ok(())
}

/// Convenience: decompress a `.binpb.zst` and verify in one call (used by
/// the WASM entrypoint since Workers can't natively decompress zstd).
pub fn verify_checkpoint_zst(
    zst: &[u8],
    committee_packed: &[u8],
    committee_epoch: u64,
) -> Result<()> {
    let proto = zstd_decode(zst).context("zstd decompress")?;
    verify_checkpoint(&proto, committee_packed, committee_epoch)
}

// ── WASM C ABI (extern "C") ──────────────────────────────────────────────────
//
// Mirrors the Zig light-client ABI so the existing TS glue in the Worker
// keeps working unchanged, just pointing at a different `.wasm`.

#[cfg(target_arch = "wasm32")]
mod wasm_abi {
    use super::*;

    /// Host allocates a buffer in our linear memory.
    #[no_mangle]
    pub extern "C" fn suilight_alloc(n: usize) -> *mut u8 {
        let mut v = Vec::<u8>::with_capacity(n);
        let ptr = v.as_mut_ptr();
        std::mem::forget(v);
        ptr
    }

    /// Host frees a buffer previously returned by `suilight_alloc`.
    #[no_mangle]
    pub unsafe extern "C" fn suilight_free(ptr: *mut u8, n: usize) {
        drop(Vec::from_raw_parts(ptr, 0, n));
    }

    /// Returns 0 on success, non-zero on rejection. Code 4 = zstd error.
    #[no_mangle]
    pub unsafe extern "C" fn suilight_verify_checkpoint_zst(
        zst_ptr: *const u8,
        zst_len: usize,
        committee_ptr: *const u8,
        committee_len: usize,
        committee_epoch: u64,
    ) -> i32 {
        let zst = std::slice::from_raw_parts(zst_ptr, zst_len);
        let committee = std::slice::from_raw_parts(committee_ptr, committee_len);
        match verify_checkpoint_zst(zst, committee, committee_epoch) {
            Ok(()) => 0,
            Err(e) => {
                let msg = e.to_string();
                if msg.starts_with("wrong epoch") {
                    10
                } else if msg.starts_with("insufficient stake") {
                    11
                } else if msg.contains("bad pubkey") {
                    12
                } else if msg.contains("bad sig") || msg.contains("aggregate verify") {
                    13
                } else if msg.contains("signer index") {
                    14
                } else if msg.contains("roaring") {
                    15
                } else if msg.contains("zstd") {
                    4
                } else if msg.contains("proto") {
                    1
                } else if msg.contains("bcs") {
                    3
                } else {
                    99
                }
            }
        }
    }

    /// Returns the number of bytes written on success, or a negative error code.
    #[no_mangle]
    pub unsafe extern "C" fn suilight_extract_next_committee_zst(
        zst_ptr: *const u8,
        zst_len: usize,
        out_ptr: *mut u8,
        out_cap: usize,
    ) -> i64 {
        let zst = std::slice::from_raw_parts(zst_ptr, zst_len);
        match extract_next_committee_from_zst(zst) {
            Ok(bytes) => {
                if bytes.len() > out_cap {
                    return -4;
                }
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
                bytes.len() as i64
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("zstd") {
                    -6
                } else if msg.contains("proto") {
                    -1
                } else if msg.contains("bcs") {
                    -2
                } else if msg.contains("not an end-of-epoch") {
                    -3
                } else {
                    -5
                }
            }
        }
    }
}
