use base64::Engine;

const MAX_ENCODED_LEN: usize = 1700;
const MAX_PACKET_LEN: usize = 1232;
const MAX_COMPUTE_UNITS: u64 = 1_400_000;
const DEFAULT_INSTRUCTION_COMPUTE_UNITS: u64 = 200_000;
const COMPUTE_BUDGET_ID: [u8; 32] = [
    3, 6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229, 187,
    197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0,
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
    if encoded.len() > MAX_ENCODED_LEN {
        return Err(DecodeError::Unparsed);
    }
    let encoding = params
        .get(1)
        .and_then(|v| v.get("encoding"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("base58");
    let bytes = match encoding {
        "base58" => bs58::decode(encoded)
            .into_vec()
            .map_err(|_| DecodeError::Unparsed)?,
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| DecodeError::Unparsed)?,
        _ => return Err(DecodeError::Unsupported),
    };
    if bytes.len() > MAX_PACKET_LEN {
        return Err(DecodeError::Unparsed);
    }
    decode_transaction(&bytes)
}

pub fn decode_transaction(bytes: &[u8]) -> Result<TransactionInfo, DecodeError> {
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
    let mut non_budget_instructions = 0u64;

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
        if program_key != COMPUTE_BUDGET_ID {
            non_budget_instructions = non_budget_instructions.saturating_add(1);
            continue;
        }
        let Some(tag) = data.first() else {
            return Err(DecodeError::InvalidBudget);
        };
        match *tag {
            2 => {
                if data.len() != 5 || explicit_limit.is_some() {
                    return Err(DecodeError::InvalidBudget);
                }
                explicit_limit = Some(u32::from_le_bytes(data[1..5].try_into().unwrap()) as u64);
            }
            3 => {
                if data.len() != 9 || price.is_some() {
                    return Err(DecodeError::InvalidBudget);
                }
                price = Some(u64::from_le_bytes(data[1..9].try_into().unwrap()));
            }
            _ => {}
        }
    }

    let (cu_limit, effective_cu_limit, cu_limit_defaulted) = match explicit_limit {
        Some(limit) => (limit, limit.min(MAX_COMPUTE_UNITS), false),
        None => (
            non_budget_instructions
                .saturating_mul(DEFAULT_INSTRUCTION_COMPUTE_UNITS)
                .min(MAX_COMPUTE_UNITS),
            non_budget_instructions
                .saturating_mul(DEFAULT_INSTRUCTION_COMPUTE_UNITS)
                .min(MAX_COMPUTE_UNITS),
            true,
        ),
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
        assert_eq!(info.cu_limit, 200_000);
        assert_eq!(info.requested_priority_fee_lamports, Some(1));

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
        assert_eq!(info.cu_limit, 200_000);

        let info =
            decode_transaction(&transaction(false, &[(&COMPUTE_BUDGET_ID, price(0))])).unwrap();
        assert_eq!(info.cu_price_micro_lamports, Some(0));
        assert_eq!(info.requested_priority_fee_lamports, Some(0));
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

        let oversized = "1".repeat(MAX_ENCODED_LEN + 1);
        let body = serde_json::json!({"params": [oversized]}).to_string();
        assert_eq!(decode_request(body.as_bytes()), Err(DecodeError::Unparsed));
        let oversized = base64::engine::general_purpose::STANDARD.encode([0u8; 1233]);
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
