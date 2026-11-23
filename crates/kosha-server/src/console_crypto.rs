//! Small cryptographic helpers for the console (#345): randomness, PKCE,
//! base64url, constant-time comparison, and sealing an organization's OIDC
//! client secret at rest. All on aws-lc-rs, rustls's crypto provider.

use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;

/// `n` random bytes from the OS.
pub(crate) fn random_bytes(n: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; n];
    aws_lc_rs::rand::fill(&mut bytes).expect("the OS random number generator failed");
    bytes
}

/// `n` random bytes as lowercase hex.
pub(crate) fn random_hex(n: usize) -> String {
    random_bytes(n).iter().map(|b| format!("{b:02x}")).collect()
}

/// `n` random bytes as base64url without padding (OAuth `state`, `nonce`
/// and PKCE verifiers: 32 bytes give 43 characters, within PKCE's 43-128).
pub(crate) fn random_url_token(n: usize) -> String {
    URL_SAFE_NO_PAD.encode(random_bytes(n))
}

pub(crate) fn b64url_decode(s: &str) -> Option<Vec<u8>> {
    URL_SAFE_NO_PAD.decode(s.trim_end_matches('=')).ok()
}

pub(crate) fn b64url(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub(crate) fn sha256(bytes: &[u8]) -> Vec<u8> {
    aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, bytes)
        .as_ref()
        .to_vec()
}

/// The PKCE `S256` code challenge of a verifier (RFC 7636 §4.2).
pub(crate) fn pkce_challenge(verifier: &str) -> String {
    b64url(&sha256(verifier.as_bytes()))
}

/// Equality that does not stop at the first difference.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    aws_lc_rs::constant_time::verify_slices_are_equal(a, b).is_ok()
}

/// The key that seals secrets stored in Postgres
/// (`KOSHA_CONSOLE_SECRET_KEY`): 32 bytes, given as 64 hex characters or
/// standard base64.
#[derive(Clone)]
pub(crate) struct SecretKey([u8; 32]);

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(..)")
    }
}

impl SecretKey {
    pub(crate) fn parse(value: &str) -> Result<SecretKey, String> {
        let value = value.trim();
        let bytes = if value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit()) {
            (0..32)
                .map(|i| u8::from_str_radix(&value[2 * i..2 * i + 2], 16).unwrap_or(0))
                .collect::<Vec<u8>>()
        } else {
            STANDARD
                .decode(value)
                .map_err(|_| "KOSHA_CONSOLE_SECRET_KEY must be 32 bytes as hex or base64")?
        };
        let key: [u8; 32] = bytes
            .try_into()
            .map_err(|_| "KOSHA_CONSOLE_SECRET_KEY must be exactly 32 bytes")?;
        Ok(SecretKey(key))
    }

    fn key(&self) -> LessSafeKey {
        LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &self.0).expect("32-byte AES key"))
    }

    /// `v1:<nonce>:<ciphertext and tag>` (base64), bound to `context` (the
    /// organization id) so a sealed value cannot be moved to another row.
    pub(crate) fn seal(&self, context: &str, plaintext: &str) -> String {
        let nonce_bytes: [u8; NONCE_LEN] = random_bytes(NONCE_LEN)
            .try_into()
            .expect("NONCE_LEN random bytes");
        let mut buf = plaintext.as_bytes().to_vec();
        self.key()
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce_bytes),
                Aad::from(context.as_bytes()),
                &mut buf,
            )
            .expect("AES-GCM seal");
        format!("v1:{}:{}", STANDARD.encode(nonce_bytes), STANDARD.encode(buf))
    }

    pub(crate) fn open(&self, context: &str, sealed: &str) -> Result<String, String> {
        let mut parts = sealed.splitn(3, ':');
        let (Some("v1"), Some(nonce), Some(ct)) = (parts.next(), parts.next(), parts.next()) else {
            return Err("unrecognized sealed secret".into());
        };
        let nonce: [u8; NONCE_LEN] = STANDARD
            .decode(nonce)
            .ok()
            .and_then(|n| n.try_into().ok())
            .ok_or("malformed sealed secret")?;
        let mut buf = STANDARD.decode(ct).map_err(|_| "malformed sealed secret")?;
        let plain = self
            .key()
            .open_in_place(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(context.as_bytes()),
                &mut buf,
            )
            .map_err(|_| "the secret does not open with KOSHA_CONSOLE_SECRET_KEY")?;
        String::from_utf8(plain.to_vec()).map_err(|_| "sealed secret is not UTF-8".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_secrets_round_trip_and_are_bound_to_their_row() {
        let key = SecretKey::parse(&"ab".repeat(32)).unwrap();
        let sealed = key.seal("org_1", "s3cret");
        assert!(sealed.starts_with("v1:") && !sealed.contains("s3cret"));
        assert_eq!(key.open("org_1", &sealed).unwrap(), "s3cret");
        assert!(key.open("org_2", &sealed).is_err());
        let other = SecretKey::parse(&STANDARD.encode([7u8; 32])).unwrap();
        assert!(other.open("org_1", &sealed).is_err());
        assert_ne!(sealed, key.seal("org_1", "s3cret"));
        assert!(SecretKey::parse("short").is_err());
    }

    #[test]
    fn pkce_matches_rfc7636_appendix_b() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
        assert_eq!(random_url_token(32).len(), 43);
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
