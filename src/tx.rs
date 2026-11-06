use base64::Engine;

/// Largest legacy/v0 transaction (`PACKET_DATA_SIZE`).
const MAX_LEGACY_LEN: usize = 1232;
/// Largest v1 transaction (SIMD-0385).
const MAX_V1_LEN: usize = 4096;
/// Agave's caps on the encoded string (`rpc.rs`, v4.4). base58 only ever
/// carries up to 1232 bytes; base64 of a v1 transaction runs to 4096.
const MAX_BASE58_LEN: usize = 1683;
const MAX_BASE64_LEN: usize = 5464;
/// First byte of a v1 transaction. v1 puts the message first and the
/// signatures last; legacy/v0 open with the signature count, which can't have
/// the high bit set within 1232 bytes.
const V1_PREFIX: u8 = 0x81;

const MAX_COMPUTE_UNITS: u64 = 1_400_000;
/// Default allotment for an instruction when no SetComputeUnitLimit is given:
/// builtins get 3,000 CU, everything else 200,000 (SIMD-0170, live on mainnet
/// since 2025-03-17).
const DEFAULT_INSTRUCTION_COMPUTE_UNITS: u64 = 200_000;
const DEFAULT_BUILTIN_COMPUTE_UNITS: u64 = 3_000;
const COMPUTE_BUDGET_ID: [u8; 32] = [
    3, 6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229, 187,
    197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0,
];
/// Programs that count as builtins for the default limit — Agave v4.4
/// `builtins-default-costs` minus the Vote program, which SIMD-0387 moved out.
/// Stake, Config and ALT are on-chain BPF programs now and take the full 200k.
const BUILTIN_PROGRAM_IDS: [[u8; 32]; 8] = [
    [0; 32], // System
    COMPUTE_BUDGET_ID,
    [
        2, 168, 246, 145, 78, 136, 161, 176, 226, 16, 21, 62, 247, 99, 174, 43, 0, 194, 185, 61,
        22, 193, 36, 210, 192, 83, 122, 16, 4, 128, 0, 0,
    ], // BPFLoaderUpgradeab1e
    [
        2, 168, 246, 145, 78, 136, 161, 107, 189, 35, 149, 133, 95, 100, 4, 217, 180, 244, 86, 183,
        130, 27, 176, 20, 87, 73, 66, 140, 0, 0, 0, 0,
    ], // BPFLoader1 (deprecated)
    [
        2, 168, 246, 145, 78, 136, 161, 110, 57, 90, 225, 40, 148, 143, 250, 105, 86, 147, 55, 104,
        24, 221, 71, 67, 82, 33, 243, 198, 0, 0, 0, 0,
    ], // BPFLoader2
    [
        5, 18, 180, 17, 81, 81, 227, 122, 173, 10, 139, 197, 211, 136, 46, 123, 127, 218, 76, 243,
        210, 192, 40, 200, 207, 131, 54, 24, 0, 0, 0, 0,
    ], // LoaderV4
    [
        4, 198, 252, 32, 240, 80, 204, 240, 85, 132, 215, 33, 28, 159, 140, 245, 158, 193, 71, 133,
        187, 22, 106, 30, 40, 48, 232, 18, 32, 0, 0, 0,
    ], // KeccakSecp256k1
    [
        3, 125, 70, 214, 124, 147, 251, 190, 18, 249, 66, 143, 131, 141, 64, 255, 5, 112, 116, 73,
        39, 244, 138, 100, 252, 202, 112, 68, 128, 0, 0, 0,
    ], // Ed25519SigVerify
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecodeError {
    Unparsed,
    Unsupported,
    InvalidBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionInfo {
    pub cu_limit: u64,
    pub cu_limit_defaulted: bool,
    pub cu_price_micro_lamports: Option<u64>,
    pub requested_priority_fee_lamports: Option<u64>,
    pub num_instructions: u64,
    pub num_signatures: u64,
    pub is_versioned: bool,
}

/// Decode a single sendTransaction JSON-RPC body. JSON and text decoding are
/// intentionally kept here so callers can run the whole operation off-path.
pub fn decode_request(body: &[u8]) -> Result<TransactionInfo, DecodeError> {
    let request: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| DecodeError::Unparsed)?;
    let params = request
        .get("params")
        .and_then(serde_json::Value::as_array)
        .ok_or(DecodeError::Unparsed)?;
    let encoded = params
        .first()
        .and_then(serde_json::Value::as_str)
        .ok_or(DecodeError::Unparsed)?;
    if encoded.len() > MAX_BASE64_LEN {
        return Err(DecodeError::Unparsed);
    }
    let encoding = params
        .get(1)
        .and_then(|v| v.get("encoding"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("base58");
    let bytes = match encoding {
        "base58" if encoded.len() > MAX_BASE58_LEN => return Err(DecodeError::Unparsed),
        "base58" => bs58::decode(encoded)
            .into_vec()
            .map_err(|_| DecodeError::Unparsed)?,
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| DecodeError::Unparsed)?,
        _ => return Err(DecodeError::Unsupported),
    };
    let max_len = if bytes.first() == Some(&V1_PREFIX) {
        MAX_V1_LEN
    } else {
        MAX_LEGACY_LEN
    };
    if bytes.len() > max_len {
        return Err(DecodeError::Unparsed);
    }
    decode_transaction(&bytes)
}

pub fn decode_transaction(bytes: &[u8]) -> Result<TransactionInfo, DecodeError> {
    match bytes.first() {
        Some(&V1_PREFIX) => return decode_v1(bytes),
        // A later message-first version.
        Some(first) if *first > V1_PREFIX => return Err(DecodeError::Unsupported),
        _ => {}
    }
    let mut cursor = Cursor::new(bytes);
    let num_signatures = cursor.shortvec()?;
    cursor.skip(
        num_signatures
            .checked_mul(64)
            .ok_or(DecodeError::Unparsed)?,
    )?;

    let first = cursor.byte()?;
    let is_versioned = first & 0x80 != 0;
    if is_versioned {
        if first != 0x80 {
            return Err(DecodeError::Unsupported);
        }
        cursor.skip(3)?;
    } else {
        // The first legacy header byte was consumed above.
        cursor.skip(2)?;
    }

    let key_count = cursor.shortvec()?;
    let keys_len = key_count.checked_mul(32).ok_or(DecodeError::Unparsed)?;
    let keys = cursor.take(keys_len)?;
    cursor.skip(32)?; // recent blockhash

    let instruction_count = cursor.shortvec()?;
    let mut explicit_limit = None;
    let mut price = None;
    let mut default_limit = 0u64;

    for _ in 0..instruction_count {
        let program_index = cursor.byte()? as usize;
        if program_index >= key_count {
            return Err(DecodeError::Unparsed);
        }
        let accounts = cursor.shortvec()?;
        cursor.skip(accounts)?;
        let data_len = cursor.shortvec()?;
        let data = cursor.take(data_len)?;
        let program_key = &keys[program_index * 32..program_index * 32 + 32];
        default_limit = default_limit.saturating_add(
            if BUILTIN_PROGRAM_IDS.iter().any(|id| id == program_key) {
                DEFAULT_BUILTIN_COMPUTE_UNITS
            } else {
                DEFAULT_INSTRUCTION_COMPUTE_UNITS
            },
        );
        if program_key != COMPUTE_BUDGET_ID {
            continue;
        }
        let Some(tag) = data.first() else {
            return Err(DecodeError::InvalidBudget);
        };
        // Agave reads these with borsh's `try_from_slice_unchecked`, so trailing
        // bytes after the value are allowed (some clients write the limit as a
        // u64); a short value or a duplicate fails the transaction.
        match *tag {
            2 => {
                if data.len() < 5 || explicit_limit.is_some() {
                    return Err(DecodeError::InvalidBudget);
                }
                explicit_limit = Some(u32::from_le_bytes(data[1..5].try_into().unwrap()) as u64);
            }
            3 => {
                if data.len() < 9 || price.is_some() {
                    return Err(DecodeError::InvalidBudget);
                }
                price = Some(u64::from_le_bytes(data[1..9].try_into().unwrap()));
            }
            _ => {}
        }
    }

    let (cu_limit, effective_cu_limit, cu_limit_defaulted) = match explicit_limit {
        Some(limit) => (limit, limit.min(MAX_COMPUTE_UNITS), false),
        None => {
            let limit = default_limit.min(MAX_COMPUTE_UNITS);
            (limit, limit, true)
        }
    };
    let requested_priority_fee_lamports = price.map(|p| {
        let product = (effective_cu_limit as u128).saturating_mul(p as u128);
        let fee = product.saturating_add(999_999) / 1_000_000;
        fee.min(u64::MAX as u128) as u64
    });

    Ok(TransactionInfo {
        cu_limit,
        cu_limit_defaulted,
        cu_price_micro_lamports: price,
        requested_priority_fee_lamports,
        num_instructions: instruction_count as u64,
        num_signatures: num_signatures as u64,
        is_versioned,
    })
}

/// Decode a v1 transaction (SIMD-0385):
///
/// ```text
/// 0x81 · header [u8; 3] · config mask u32 · lifetime [u8; 32]
/// · num_instructions u8 · num_addresses u8 · addresses [[u8; 32]]
/// · config values [u8; 4] per set mask bit
/// · instruction headers (program u8, num_accounts u8, data_len u16)
/// · instruction payloads (account indexes, data) · signatures [[u8; 64]]
/// ```
///
/// The compute budget lives in the config values, not in instructions
/// (ComputeBudget instructions in a v1 transaction are ignored): bits 0+1 are
/// the priority fee as a u64 **total in lamports**, bit 2 the compute unit
/// limit, bits 3 and 4 the loaded-accounts-data and heap limits. An unset limit
/// or fee is 0, as in Agave's `transaction_meta.rs`.
fn decode_v1(bytes: &[u8]) -> Result<TransactionInfo, DecodeError> {
    let mut cursor = Cursor::new(bytes);
    cursor.skip(1)?; // version
    let num_signatures = cursor.byte()? as usize;
    cursor.skip(2)?; // readonly signed / unsigned counts
    let mask = u32::from_le_bytes(cursor.take(4)?.try_into().unwrap());
    cursor.skip(32)?; // lifetime
    let num_instructions = cursor.byte()? as usize;
    let num_addresses = cursor.byte()? as usize;
    cursor.skip(num_addresses * 32)?;

    let (mut fee_low, mut fee_high, mut limit) = (None, None, None);
    for bit in (0..32).filter(|bit| mask & (1 << bit) != 0) {
        let value = u32::from_le_bytes(cursor.take(4)?.try_into().unwrap());
        match bit {
            0 => fee_low = Some(value),
            1 => fee_high = Some(value),
            2 => limit = Some(value),
            _ => {}
        }
    }
    let fee = match (fee_low, fee_high) {
        (Some(low), Some(high)) => Some(u64::from(high) << 32 | u64::from(low)),
        (None, None) => None,
        _ => return Err(DecodeError::InvalidBudget), // half a u64
    };

    let mut payload_len = 0usize;
    for _ in 0..num_instructions {
        if cursor.byte()? as usize >= num_addresses {
            return Err(DecodeError::Unparsed);
        }
        let accounts = cursor.byte()? as usize;
        let data_len = u16::from_le_bytes(cursor.take(2)?.try_into().unwrap()) as usize;
        payload_len += accounts + data_len;
    }
    cursor.skip(payload_len)?;
    if cursor.remaining() != num_signatures * 64 {
        return Err(DecodeError::Unparsed);
    }

    let cu_limit = u64::from(limit.unwrap_or(0));
    let effective_cu_limit = cu_limit.min(MAX_COMPUTE_UNITS);
    Ok(TransactionInfo {
        cu_limit,
        cu_limit_defaulted: limit.is_none(),
        // What getRecentPrioritizationFees reports for v1: floor(fee × 1e6 / limit).
        cu_price_micro_lamports: fee.map(|fee| {
            (u128::from(fee) * 1_000_000)
                .checked_div(u128::from(effective_cu_limit))
                .map_or(0, |price| price.min(u64::MAX as u128) as u64)
        }),
        requested_priority_fee_lamports: fee,
        num_instructions: num_instructions as u64,
        num_signatures: num_signatures as u64,
        is_versioned: true,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn byte(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(len).ok_or(DecodeError::Unparsed)?;
        let result = self.bytes.get(self.pos..end).ok_or(DecodeError::Unparsed)?;
        self.pos = end;
        Ok(result)
    }

    fn skip(&mut self, len: usize) -> Result<(), DecodeError> {
        self.take(len).map(|_| ())
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.pos
    }

    fn shortvec(&mut self) -> Result<usize, DecodeError> {
        let mut value = 0usize;
        for shift in (0..=14).step_by(7) {
            let byte = self.byte()?;
            if shift == 14 && byte > 3 {
                return Err(DecodeError::Unparsed);
            }
            value |= ((byte & 0x7f) as usize) << shift;
            if byte & 0x80 == 0 {
                if shift > 0 && byte == 0 {
                    return Err(DecodeError::Unparsed);
                }
                return Ok(value);
            }
        }
        Err(DecodeError::Unparsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transaction(versioned: bool, instructions: &[(&[u8; 32], Vec<u8>)]) -> Vec<u8> {
        let mut out = vec![1];
        out.extend([0u8; 64]);
        if versioned {
            out.push(0x80);
        }
        out.extend([1, 0, 0]);
        out.push(instructions.len() as u8);
        for (key, _) in instructions {
            out.extend(*key);
        }
        out.extend([0u8; 32]);
        out.push(instructions.len() as u8);
        for (i, (_, data)) in instructions.iter().enumerate() {
            out.extend([i as u8, 0, data.len() as u8]);
            out.extend(data);
        }
        if versioned {
            out.push(0); // no ALT lookups; ignored by the decoder
        }
        out
    }

    fn limit(value: u32) -> Vec<u8> {
        let mut data = vec![2];
        data.extend(value.to_le_bytes());
        data
    }

    fn price(value: u64) -> Vec<u8> {
        let mut data = vec![3];
        data.extend(value.to_le_bytes());
        data
    }

    const OTHER: [u8; 32] = [7; 32];

    #[test]
    fn legacy_defaults_limit_and_base58_encoding() {
        let bytes = transaction(false, &[(&OTHER, vec![1]), (&OTHER, vec![2])]);
        let body = serde_json::json!({"params": [bs58::encode(bytes).into_string()]}).to_string();
        let info = decode_request(body.as_bytes()).unwrap();
        assert_eq!(info.cu_limit, 400_000);
        assert!(info.cu_limit_defaulted);
        assert_eq!(info.cu_price_micro_lamports, None);
        assert!(!info.is_versioned);
    }

    #[test]
    fn legacy_explicit_base64_and_limit_only_decode() {
        let bytes = transaction(false, &[(&COMPUTE_BUDGET_ID, limit(350_000))]);
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        let body = serde_json::json!({"params": [encoded, {"encoding":"base64"}]}).to_string();
        let info = decode_request(body.as_bytes()).unwrap();
        assert_eq!(info.cu_limit, 350_000);
        assert!(!info.cu_limit_defaulted);
        assert_eq!(info.cu_price_micro_lamports, None);
    }

    #[test]
    fn v0_base64_decodes_limit_price_and_ceiling() {
        let bytes = transaction(
            true,
            &[
                (&COMPUTE_BUDGET_ID, limit(1)),
                (&COMPUTE_BUDGET_ID, price(1)),
                (&OTHER, vec![9]),
            ],
        );
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        let body = serde_json::json!({"params": [encoded, {"encoding":"base64"}]}).to_string();
        let info = decode_request(body.as_bytes()).unwrap();
        assert_eq!(info.cu_limit, 1);
        assert_eq!(info.requested_priority_fee_lamports, Some(1));
        assert!(info.is_versioned);

        let mut with_lookup = transaction(true, &[(&OTHER, vec![9])]);
        with_lookup.pop();
        with_lookup.push(1);
        with_lookup.extend([8u8; 32]);
        with_lookup.extend([1, 0, 1, 0]);
        assert!(decode_transaction(&with_lookup).unwrap().is_versioned);
    }

    #[test]
    fn price_only_uses_default_and_limit_is_clamped() {
        let info = decode_transaction(&transaction(
            false,
            &[(&COMPUTE_BUDGET_ID, price(5)), (&OTHER, vec![1])],
        ))
        .unwrap();
        // SIMD-0170: the ComputeBudget instruction is a builtin (3,000 CU), the
        // other instruction 200,000 — so ceil(203,000 × 5 / 1e6) = 2 lamports.
        assert_eq!(info.cu_limit, 203_000);
        assert_eq!(info.requested_priority_fee_lamports, Some(2));

        let info = decode_transaction(&transaction(
            false,
            &[
                (&COMPUTE_BUDGET_ID, limit(2_000_000)),
                (&COMPUTE_BUDGET_ID, price(u64::MAX)),
            ],
        ))
        .unwrap();
        assert_eq!(info.cu_limit, 2_000_000);
        assert_eq!(info.requested_priority_fee_lamports, Some(u64::MAX));
    }

    #[test]
    fn zero_price_is_present_and_budget_key_not_invoked_is_ignored() {
        let info = decode_transaction(&transaction(
            false,
            &[(&COMPUTE_BUDGET_ID, vec![1]), (&OTHER, price(0))],
        ))
        .unwrap();
        assert_eq!(info.cu_price_micro_lamports, None);
        assert_eq!(info.cu_limit, 203_000);

        let info =
            decode_transaction(&transaction(false, &[(&COMPUTE_BUDGET_ID, price(0))])).unwrap();
        assert_eq!(info.cu_price_micro_lamports, Some(0));
        assert_eq!(info.requested_priority_fee_lamports, Some(0));
        assert_eq!(info.cu_limit, 3_000);
    }

    /// SIMD-0170 default limits: 3,000 CU per builtin instruction, 200,000 per
    /// other one. Vote left the builtin list with SIMD-0387.
    #[test]
    fn default_limit_counts_builtins_at_3000() {
        const SYSTEM: [u8; 32] = [0; 32];
        let info = decode_transaction(&transaction(
            false,
            &[(&SYSTEM, vec![2]), (&SYSTEM, vec![2])],
        ))
        .unwrap();
        assert_eq!(info.cu_limit, 6_000);
        assert!(info.cu_limit_defaulted);

        let vote = bs58::decode("Vote111111111111111111111111111111111111111")
            .into_vec()
            .unwrap();
        let vote: [u8; 32] = vote.try_into().unwrap();
        let info = decode_transaction(&transaction(true, &[(&vote, vec![1]), (&SYSTEM, vec![2])]))
            .unwrap();
        assert_eq!(info.cu_limit, 203_000);

        let many: Vec<_> = (0..8).map(|_| (&OTHER, vec![1])).collect();
        assert_eq!(
            decode_transaction(&transaction(false, &many))
                .unwrap()
                .cu_limit,
            1_400_000,
            "capped"
        );
    }

    #[test]
    fn builtin_ids_match_program_addresses() {
        let names = [
            "11111111111111111111111111111111",
            "ComputeBudget111111111111111111111111111111",
            "BPFLoaderUpgradeab1e11111111111111111111111",
            "BPFLoader1111111111111111111111111111111111",
            "BPFLoader2111111111111111111111111111111111",
            "LoaderV411111111111111111111111111111111111",
            "KeccakSecp256k11111111111111111111111111111",
            "Ed25519SigVerify111111111111111111111111111",
        ];
        for (id, name) in BUILTIN_PROGRAM_IDS.iter().zip(names) {
            assert_eq!(bs58::encode(id).into_string(), name);
        }
    }

    /// Seen on mainnet: SetComputeUnitLimit serialized with a u64, i.e. four
    /// trailing zero bytes. The runtime accepts it, so the decoder must too.
    #[test]
    fn budget_values_with_trailing_bytes_decode() {
        let mut limit_u64 = limit(100_001);
        limit_u64.extend([0; 4]);
        let mut price_long = price(7);
        price_long.push(0);
        let info = decode_transaction(&transaction(
            false,
            &[
                (&COMPUTE_BUDGET_ID, limit_u64),
                (&COMPUTE_BUDGET_ID, price_long),
            ],
        ))
        .unwrap();
        assert_eq!(info.cu_limit, 100_001);
        assert_eq!(info.cu_price_micro_lamports, Some(7));
    }

    #[test]
    fn duplicate_or_malformed_budget_is_invalid() {
        for instructions in [
            vec![
                (&COMPUTE_BUDGET_ID, limit(1)),
                (&COMPUTE_BUDGET_ID, limit(2)),
            ],
            vec![
                (&COMPUTE_BUDGET_ID, price(1)),
                (&COMPUTE_BUDGET_ID, price(2)),
            ],
            vec![(&COMPUTE_BUDGET_ID, vec![2, 1])],
            vec![(&COMPUTE_BUDGET_ID, vec![])],
        ] {
            assert_eq!(
                decode_transaction(&transaction(false, &instructions)),
                Err(DecodeError::InvalidBudget)
            );
        }
    }

    #[test]
    fn malformed_and_unsupported_inputs_are_classified() {
        assert_eq!(decode_transaction(&[]), Err(DecodeError::Unparsed));
        let mut future = transaction(true, &[]);
        future[65] = 0x81;
        assert_eq!(decode_transaction(&future), Err(DecodeError::Unsupported));
        assert_eq!(decode_transaction(&[0x80]), Err(DecodeError::Unparsed));

        let body = br#"{"params":["%%%",{"encoding":"base64"}]}"#;
        assert_eq!(decode_request(body), Err(DecodeError::Unparsed));
        let body = br#"{"params":["0OIl"]}"#;
        assert_eq!(decode_request(body), Err(DecodeError::Unparsed));
        let body = br#"{"params":["abc",{"encoding":"base32"}]}"#;
        assert_eq!(decode_request(body), Err(DecodeError::Unsupported));

        let oversized = "1".repeat(MAX_BASE58_LEN + 1);
        let body = serde_json::json!({"params": [oversized]}).to_string();
        assert_eq!(decode_request(body.as_bytes()), Err(DecodeError::Unparsed));
        // 1233 bytes is too long for legacy/v0…
        let oversized = base64::engine::general_purpose::STANDARD.encode([0u8; 1233]);
        let body = serde_json::json!({"params": [oversized, {"encoding":"base64"}]}).to_string();
        assert_eq!(decode_request(body.as_bytes()), Err(DecodeError::Unparsed));
        // …and 4097 too long for v1.
        let mut v1 = vec![V1_PREFIX];
        v1.resize(4097, 0);
        let oversized = base64::engine::general_purpose::STANDARD.encode(v1);
        assert!(oversized.len() <= MAX_BASE64_LEN + 4);
        let body = serde_json::json!({"params": [oversized, {"encoding":"base64"}]}).to_string();
        assert_eq!(decode_request(body.as_bytes()), Err(DecodeError::Unparsed));
    }

    #[test]
    fn out_of_range_program_index_and_bad_shortvec_are_unparsed() {
        let mut bytes = transaction(false, &[(&OTHER, vec![1])]);
        let instruction_program_index = 1 + 64 + 3 + 1 + 32 + 32 + 1;
        bytes[instruction_program_index] = 1;
        assert_eq!(decode_transaction(&bytes), Err(DecodeError::Unparsed));
        assert_eq!(
            decode_transaction(&[0x80, 0x80, 0x80]),
            Err(DecodeError::Unparsed)
        );
        let mut noncanonical = transaction(false, &[(&OTHER, vec![1])]);
        noncanonical.splice(0..1, [0x81, 0x00]);
        assert_eq!(
            decode_transaction(&noncanonical),
            Err(DecodeError::Unparsed)
        );

        let complete = transaction(false, &[(&OTHER, vec![1, 2, 3])]);
        for end in [1, 64, 66, 70, complete.len() - 1] {
            assert_eq!(
                decode_transaction(&complete[..end]),
                Err(DecodeError::Unparsed),
                "truncation at {end}"
            );
        }
    }

    #[test]
    fn compute_budget_constant_matches_program_address() {
        assert_eq!(
            bs58::encode(COMPUTE_BUDGET_ID).into_string(),
            "ComputeBudget111111111111111111111111111111"
        );
    }

    // Real wire-format bytes, produced independently by solana-sdk's own
    // bincode/short_vec Transaction and VersionedTransaction serializers (the
    // same serialization a wallet or the solana-client crate uses before
    // base58/base64-encoding for `sendTransaction`) — not hand-rolled — so a
    // misunderstanding of the wire format can't hide behind the same
    // misunderstanding used to build a synthetic fixture. Each carries
    // SetComputeUnitLimit(300_000) [+ SetComputeUnitPrice(5_000)] ahead of an
    // unrelated instruction. Regenerate with a throwaway solana-sdk script if
    // the wire format ever needs re-verifying.
    mod fixtures {
        // len=267 base58=5wwwrqircAGorg6UHcwvBF6KC9KELxHCtiDAYUNVbVatbsiTcssEt9HkjksCMo7xaDfRjgn4rDWim8Ahdwh8hBB9FSvNsQJNRP8B9puk982kYqcPXJnjTuVHA5UiugK7R41s2bZtZT6LyutkiE2xh3moqZPg9KKHnuLxMgaXqoNvvSr8wZN3SDysaXAFmskaTkZK5sqUXcaLyRx2vZiDqMt8ExZnbGC4J48asGhL9SPxKdgXLTEqfAJpN5otBsgyx9VsGESvbTu4137Nwx35p3UvWvZEpJGErZm6utnxfNZqRA3UmRKvb5pZd2dpNRk1PUbkXCekmbk9PMpTGAuQjMgnsX1VLvLqguJCEvikpwJf
        pub const LEGACY_LIMIT_PRICE_TRANSFER: [u8; 267] = [
            1, 175, 169, 202, 67, 186, 44, 21, 132, 167, 193, 119, 97, 144, 20, 62, 86, 157, 47,
            197, 232, 125, 170, 20, 212, 52, 241, 131, 163, 98, 143, 50, 217, 164, 8, 84, 119, 57,
            165, 218, 172, 24, 90, 198, 135, 11, 62, 240, 160, 2, 241, 192, 64, 87, 254, 242, 77,
            216, 158, 131, 187, 51, 229, 186, 5, 1, 0, 2, 4, 198, 250, 24, 80, 183, 238, 201, 66,
            125, 88, 235, 179, 191, 228, 25, 43, 14, 248, 27, 172, 179, 177, 107, 250, 168, 120,
            121, 250, 221, 86, 0, 2, 0, 0, 0, 1, 144, 112, 123, 195, 239, 37, 189, 201, 142, 215,
            92, 183, 13, 97, 200, 177, 6, 220, 36, 141, 142, 246, 30, 29, 29, 177, 202, 64, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 3, 6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140,
            229, 187, 197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 0, 5, 2,
            224, 147, 4, 0, 3, 0, 9, 3, 136, 19, 0, 0, 0, 0, 0, 0, 2, 2, 0, 1, 12, 2, 0, 0, 0, 64,
            66, 15, 0, 0, 0, 0, 0,
        ];
        // len=255 base58=277PruXiMgc54Y9oMVGQ9Kkf5WrsU7PeoS17SEJiwdqSW11yB1jgYdUr9JjBEJA1TRMYJEmLV49PGYemu1mDxMLvHmBdbiYqjHkBmbVtTxRipkmhbVWy6Gbne76HtTRJvGK5TCKHix8wZcCe3zVjfiowf9fPzXoURcHWQ7fTSQGRLZbjHwwtpXdM5iFsbNt86hRRjQQfCWTYSpkQyj4EFHz9b1T2oQHpoSw8TNwZc4fTVZmw8AdD2DqJvYgyy1Z5KD4ELkqDqZv8DumC7g8MsiTt57F32656cbyFNF8PuRU9GWzbB8yctUgbbjsYxR5nKofENdj5vdaZ8zxHx8NHmxg2kT7M
        pub const LEGACY_LIMIT_ONLY: [u8; 255] = [
            1, 209, 232, 241, 245, 150, 34, 135, 158, 103, 250, 5, 162, 254, 219, 147, 106, 119, 7,
            211, 19, 185, 113, 13, 25, 85, 97, 242, 23, 130, 28, 112, 222, 35, 50, 151, 52, 176,
            231, 58, 110, 53, 219, 100, 109, 74, 253, 81, 10, 166, 50, 157, 122, 81, 226, 8, 234,
            221, 95, 123, 22, 215, 65, 75, 2, 1, 0, 2, 4, 198, 250, 24, 80, 183, 238, 201, 66, 125,
            88, 235, 179, 191, 228, 25, 43, 14, 248, 27, 172, 179, 177, 107, 250, 168, 120, 121,
            250, 221, 86, 0, 2, 0, 0, 0, 1, 144, 112, 123, 195, 239, 37, 189, 201, 142, 215, 92,
            183, 13, 97, 200, 177, 6, 220, 36, 141, 142, 246, 30, 29, 29, 177, 202, 64, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3,
            6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229,
            187, 197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 2, 3, 0, 5, 2, 224, 147,
            4, 0, 2, 2, 0, 1, 12, 2, 0, 0, 0, 64, 66, 15, 0, 0, 0, 0, 0,
        ];
        // len=269 base58=zAHcK2uetEawthKWcWx6cvzPY9ngrnEovkwfsZYWkf3t43y7WMvmpRcP3RfkmDSqDsMtxw8BWsUwFkpzaA9zNagKz6MipN8HPQ4dkfAM7W1zsZ38mTeLqxKGJxPG5kyTRDTKvY5LHP8XaQGndGQACA4Xpt2cuLTq5f76epABxaXKPvuSwy61AxuDeNDcZKcD3mxRprrH82b9BfJwCC3WArx3QHpxRRwXFFmSMkdWEvHu1erk5wGyPoFECLPh149XArKpjF2Z4fNVLGFikPeHK9uZvYWuEdao9izAXRDo2cN8kNLPKqWSP5qguJtR2HQCypjjFYry2qR7Mdrg6MfXMqv56Peawfdq9rzfmGCp9GjN2K
        pub const V0_NO_ALT: [u8; 269] = [
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 2, 4, 198, 250, 24, 80, 183, 238, 201, 66, 125, 88,
            235, 179, 191, 228, 25, 43, 14, 248, 27, 172, 179, 177, 107, 250, 168, 120, 121, 250,
            221, 86, 0, 2, 0, 0, 0, 1, 144, 112, 123, 195, 239, 37, 189, 201, 142, 215, 92, 183,
            13, 97, 200, 177, 6, 220, 36, 141, 142, 246, 30, 29, 29, 177, 202, 64, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 6,
            70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229, 187,
            197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 0, 5, 2, 224, 147, 4, 0,
            3, 0, 9, 3, 136, 19, 0, 0, 0, 0, 0, 0, 2, 2, 0, 1, 12, 2, 0, 0, 0, 64, 66, 15, 0, 0, 0,
            0, 0, 0,
        ];
        // len=260 base58=JYQxKqa9ELP6TUexGksb7AdwZ6DGR8yKXKZ9KKvCn4y7WhdKXCyA6bHNis3tVfyZWfnFc2ZZy65fxS2ksACT2TwwMPmkmhXDU8hErjGPikwJaV9Aph1e5ZRRMizTYLYY3ZLzAEvehUvfZnNsgQYw4MbGhHfmbidSFq3y7UbaHh3W2a29WU3mKBUsDsP6PoqYphoWsHSJ2hWeQpY4qCxdwyvgjPYfTzMLhuhuUdRpB7qwkwtq3TPtxDuPYScgXXHJcgAeFxdudGsMSPaRdyoXTSaDeuesbPmzQsk37jHJa1ehkCfB1sXHn5jCJmijaK2BmJyi8xG5bec8oZLyeoykYNpMkswYT8cW5V
        pub const V0_WITH_ALT: [u8; 260] = [
            1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 128, 1, 0, 2, 3, 198, 250, 24, 80, 183, 238, 201, 66, 125, 88,
            235, 179, 191, 228, 25, 43, 14, 248, 27, 172, 179, 177, 107, 250, 168, 120, 121, 250,
            221, 86, 0, 2, 0, 0, 0, 2, 145, 250, 236, 105, 216, 94, 42, 23, 79, 107, 56, 162, 20,
            17, 30, 62, 29, 38, 95, 0, 169, 158, 226, 113, 46, 23, 128, 229, 3, 6, 70, 111, 229,
            33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229, 187, 197, 247, 18,
            107, 44, 67, 155, 58, 64, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
            7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 2, 0, 5, 2, 224, 147, 4, 0, 2, 0, 9, 3,
            136, 19, 0, 0, 0, 0, 0, 0, 1, 1, 3, 1, 9, 1, 0, 0, 0, 3, 30, 202, 179, 86, 185, 44,
            165, 66, 156, 67, 58, 14, 75, 81, 197, 96, 95, 10, 40, 248, 178, 148, 124, 155, 162,
            75, 215, 16, 1, 0, 0,
        ];
    }

    // Real v1 transactions from mainnet slot 466,007,5xx (2026-11-06), taken
    // from `getBlock` with `maxSupportedTransactionVersion: 1`. The expected fees
    // are what the runtime charged: `meta.fee` minus 5,000 per signature.
    mod v1_fixtures {
        // len=257 mask=0xf sig=2TY5kD2wJQVHGPKaCqyU4wNXdb7pyDMivXGktgNhGuGeFv3GvMZ1v2MBdF7KmMNKoNfX6WNkn5y3jHzWtqFfFEwa
        // fee paid 5324 = 5000 base + 324 priority; limit 24; 1 ix
        pub const V1_FEE_AND_LIMIT: &str = concat!(
            "gQEAAQ8AAADxpwvSwWwDZGKZjjmA+T7pc4FybFpHT6EFTBwyGAwOoAED1uHdYSOgHIfEFaT+/////1sWwSJ8TzNrw4",
            "nWmDcSyYpnqSuGAaXe6clNBmTkI4f8kiOs3J//AwibHXJ374kCwgloGfZKAvdXJHoeJzNoY5iNbKgT2jEcJ/lZlSdr",
            "kFHoRAEAAAAAAAAYAAAAcQMAAAICIQAAAdFYVgUAAAAAocVWBQAAAABjtsYbQEIPANiuiK+hAQAA/0jmiiXKXV5LEj",
            "JLa8BvvFpvC5pdCReSlv7YTEEMUZdnD1ENbEShwFr5jlZ6yb8OMbTCa+KTBiP5uesj8xjz/wE=",
        );
        // len=587 mask=0x1f sig=5CXc9RrEwVsH9DnTAYJtetJ7ZUipBnkZKr3T6ZUJzBHLhPzwziRnRcyUj2W5mgxMgBnAAUeCeJ6jR3FxkMzjCGZy
        // fee paid 5005 = 5000 base + 5 priority; limit 610; 1 ix
        pub const V1_FEE_LIMIT_HEAP: &str = concat!(
            "gQEAAR8AAABW4WP0pwilODFa0zv7813b4XLv2HlbAH8F2JIi1bWhcQEDaKLdQ+LwfDvX9PJ4Uj+1MDLhFU1YcLoOdl",
            "BRwcItglvyyHeIvEHlnUg8fuAqXSjUT4UTmj04WFjKgCbdQYPkAgv4/JLU3rMUWgjuC2u6N5eRRt1SZoMgKa8hW58V",
            "BcyaBQAAAAAAAABiAgAAQKgCAACAAAACAmcBAAE3AEz5Mdk4AAAAY7bGGwAAAAAiFgDD7ME7NndGvgQAw+wL3kF3Rr",
            "4MAMPsQzjSKUy+DwDD7MmNS3dGvgoAw+wXYF93Rr4GAMPsh8Zfd0a+DQDD7C3YQnhGvgsAw+xFdV93Rr4QAMPs4y9Z",
            "d0a+EwDD7FEYCHdGvh4Aw+zV2MR1Rr4OAMPsPYmyd0a+HADD7BdIHGNGvgAAw+yBJUF3Rr4iAMPsHcc5c0a+IADD7D",
            "8T1n9GvicAw+yT0BB9Rr4hAMPsBWy8cka+EQDD7DVlTXhGvgEAw+zxLUF3Rr4IAMPssWQrdka+KQDD7K9SknNGviYA",
            "w+zJgah2Rr4UAMPsWTWbd0a+HQDD7Ckljn9GvgMAw+ydvFN3Rr4bAMPsf/bVdka+KADD7M0N4H5GvhoAw+yNGkt3Rr",
            "4SAMPsNWVNeEa+JQDD7HtjqnJGvh8Aw+y30EFzRr4HAMPsZwRcd0a+AgDD7G0JTnZGvtICUBtAARZKjIbYap9wg8/i",
            "o1qFz+IGZ8aJn5tsdq8kuwyW/fCPVjhngJonPM+xxjqySYsoCHgQS9To24SjTQw=",
        );
        // len=322 mask=0xc sig=MczyW3Eymc7miNGxknDTZeWqt4ewUdeqFzjSBu5ihjufax46oPGJKmf7W6ryLFD5AcNkRh16VfghHy4pqVnHj98
        // fee paid 5000 = 5000 base + 0 priority; limit 20000; 3 ix; ComputeBudget ix tags ['02', '03']
        pub const V1_NO_FEE_WITH_IGNORED_BUDGET_IXS: &str = concat!(
            "gQEAAwwAAAAYSjgeGymsjMOP8FEO4vi8Jqj9oNNNqdAlIKUoD+vzNwMECJ7rBJUAwhnucTD+W8jojhTzhhvYVpuR23",
            "ruCEPIWGiW8PnGQjAugJaVd/Xf/2vr5Ksf1sQdWKtA4NThyr8qkAMGRm/lIRcy/+ytunLDm+e8jOW7xfcSayxDmzpA",
            "AAAABUpTWpkpIQZNJOhxYNo4fHw1td28kruB5B+oQEEFRI0gTgAAAAAABAIABQACAAkAAwI0AAIgTgAAA5DQAwAAAA",
            "AAAAFzd206T1JrSEZqVU5DQUVJRVFBTkQxY1NIUUZDV1Z4RVdSeEpFVUZYVXhkQlFsdEJYQT09EchRfKyhEwBA5Gx4",
            "r+QJ+/4Nt0gac4dqlzv9Vw+uYJtD/z4d4H3Xb1jkx/cdHr+dzwNpKCq3y6Zn+2jjACxSDw==",
        );
        // len=1318 mask=0xf sig=41tscx8bAjCmyhLFCSZz2xKJLFvjhgqRyEtVtEqcqPFt7XHhQjC27A9qsWRVcREK7K9NQ3tQPWjvENTHP6QB2And
        // fee paid 5290 = 5000 base + 290 priority; limit 222603; 1 ix
        pub const V1_OVER_1232_BYTES: &str = concat!(
            "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",
        );
    }

    fn v1(encoded: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap()
    }

    #[test]
    fn v1_header_fee_is_total_lamports_and_price_is_derived() {
        let info = decode_transaction(&v1(v1_fixtures::V1_FEE_AND_LIMIT)).unwrap();
        assert!(info.is_versioned);
        assert_eq!(info.requested_priority_fee_lamports, Some(324));
        assert_eq!(info.cu_limit, 24);
        assert!(!info.cu_limit_defaulted);
        assert_eq!(info.cu_price_micro_lamports, Some(13_500_000));
        assert_eq!(info.num_instructions, 1);
        assert_eq!(info.num_signatures, 1);

        // The heap-size bit adds a config word; it must not shift the others.
        let info = decode_transaction(&v1(v1_fixtures::V1_FEE_LIMIT_HEAP)).unwrap();
        assert_eq!(info.requested_priority_fee_lamports, Some(5));
        assert_eq!(info.cu_limit, 610);
        assert_eq!(info.cu_price_micro_lamports, Some(8_196));
    }

    /// ComputeBudget instructions inside a v1 transaction are ignored: this one
    /// sets a limit and a price by instruction, yet paid no priority fee.
    #[test]
    fn v1_ignores_compute_budget_instructions() {
        let info = decode_transaction(&v1(v1_fixtures::V1_NO_FEE_WITH_IGNORED_BUDGET_IXS)).unwrap();
        assert_eq!(info.requested_priority_fee_lamports, None);
        assert_eq!(info.cu_price_micro_lamports, None);
        assert_eq!(info.cu_limit, 20_000, "from the header");
        assert_eq!(info.num_instructions, 3);
    }

    #[test]
    fn v1_over_1232_bytes_decodes_through_the_request_path() {
        let raw = v1(v1_fixtures::V1_OVER_1232_BYTES);
        assert!(raw.len() > MAX_LEGACY_LEN);
        let body = serde_json::json!({
            "params": [v1_fixtures::V1_OVER_1232_BYTES, {"encoding": "base64"}]
        })
        .to_string();
        let info = decode_request(body.as_bytes()).unwrap();
        assert_eq!(info.requested_priority_fee_lamports, Some(290));
        assert_eq!(info.cu_limit, 222_603);
        assert_eq!(info.cu_price_micro_lamports, Some(1_302));
    }

    #[test]
    fn v1_malformed_and_unset_fields() {
        let good = v1(v1_fixtures::V1_FEE_AND_LIMIT);
        for end in [1, 8, 40, 42, good.len() - 1] {
            assert_eq!(
                decode_transaction(&good[..end]),
                Err(DecodeError::Unparsed),
                "truncation at {end}"
            );
        }
        let mut trailing = good.clone();
        trailing.push(0);
        assert_eq!(decode_transaction(&trailing), Err(DecodeError::Unparsed));

        // Only half of the u64 fee: clear bit 1 and drop its config word, which
        // sits second after the addresses, so the rest still lines up.
        let mut half = good.clone();
        half[4] &= !0b10;
        let config_start = 42 + 32 * good[41] as usize;
        half.drain(config_start + 4..config_start + 8);
        assert_eq!(decode_transaction(&half), Err(DecodeError::InvalidBudget));

        // No config at all: limit 0 (Agave's default for v1), no fee.
        let mut bare = vec![V1_PREFIX, 1, 0, 0, 0, 0, 0, 0];
        bare.extend([0u8; 32]);
        bare.extend([0, 0]); // no instructions, no addresses
        bare.extend([0u8; 64]);
        let info = decode_transaction(&bare).unwrap();
        assert_eq!(info.cu_limit, 0);
        assert!(info.cu_limit_defaulted);
        assert_eq!(info.requested_priority_fee_lamports, None);

        let mut future = bare.clone();
        future[0] = 0x82;
        assert_eq!(decode_transaction(&future), Err(DecodeError::Unsupported));
    }

    #[test]
    fn legacy_fixture_base58_default_encoding_decodes_limit_and_price() {
        let body = serde_json::json!({
            "params": [bs58::encode(fixtures::LEGACY_LIMIT_PRICE_TRANSFER).into_string()]
        })
        .to_string();
        let info = decode_request(body.as_bytes()).unwrap();
        assert_eq!(info.cu_limit, 300_000);
        assert!(!info.cu_limit_defaulted);
        assert_eq!(info.cu_price_micro_lamports, Some(5_000));
        assert_eq!(info.requested_priority_fee_lamports, Some(1_500));
        assert_eq!(info.num_instructions, 3);
        assert_eq!(info.num_signatures, 1);
        assert!(!info.is_versioned);
    }

    #[test]
    fn legacy_fixture_base64_explicit_encoding_limit_only() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(fixtures::LEGACY_LIMIT_ONLY);
        let body = serde_json::json!({"params": [encoded, {"encoding": "base64"}]}).to_string();
        let info = decode_request(body.as_bytes()).unwrap();
        assert_eq!(info.cu_limit, 300_000);
        assert!(!info.cu_limit_defaulted);
        assert_eq!(info.cu_price_micro_lamports, None);
        assert_eq!(info.requested_priority_fee_lamports, None);
        assert_eq!(info.num_instructions, 2);
        assert!(!info.is_versioned);
    }

    #[test]
    fn v0_fixture_without_alt_lookups_decodes_limit_and_price() {
        let info = decode_transaction(&fixtures::V0_NO_ALT).unwrap();
        assert!(info.is_versioned);
        assert_eq!(info.cu_limit, 300_000);
        assert_eq!(info.cu_price_micro_lamports, Some(5_000));
        assert_eq!(info.requested_priority_fee_lamports, Some(1_500));
        assert_eq!(info.num_instructions, 3);
    }

    #[test]
    fn v0_fixture_with_alt_lookups_decodes_limit_and_price() {
        let info = decode_transaction(&fixtures::V0_WITH_ALT).unwrap();
        assert!(info.is_versioned);
        assert_eq!(info.cu_limit, 300_000);
        assert_eq!(info.cu_price_micro_lamports, Some(5_000));
        assert_eq!(info.requested_priority_fee_lamports, Some(1_500));
        assert_eq!(info.num_instructions, 3);
    }
}
