use aes_gcm_siv::{
    Aes256GcmSiv, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{AppError, AppResult};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptedValue {
    pub algorithm: String,
    pub nonce_b64: String,
    pub ciphertext_b64: String,
}

#[derive(Clone, Debug)]
pub struct CryptoContext {
    env_var_name: String,
    key_bytes: Option<[u8; 32]>,
}

impl CryptoContext {
    pub fn from_env(env_var_name: &str) -> Self {
        let key_bytes = std::env::var(env_var_name)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .map(|value| {
                let digest = Sha256::digest(value.as_bytes());
                let mut key = [0_u8; 32];
                key.copy_from_slice(&digest);
                key
            });

        Self {
            env_var_name: env_var_name.to_owned(),
            key_bytes,
        }
    }

    pub fn is_configured(&self) -> bool {
        self.key_bytes.is_some()
    }

    pub fn encrypt_string(&self, plain_text: &str) -> AppResult<EncryptedValue> {
        let cipher = self.cipher()?;
        let mut nonce_bytes = [0_u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let cipher_text = cipher
            .encrypt(nonce, plain_text.as_bytes())
            .map_err(|error| AppError::Crypto(format!("encrypt failed: {error:?}")))?;

        Ok(EncryptedValue {
            algorithm: "aes-256-gcm-siv".to_owned(),
            nonce_b64: STANDARD.encode(nonce_bytes),
            ciphertext_b64: STANDARD.encode(cipher_text),
        })
    }

    pub fn decrypt_string(&self, encrypted: &EncryptedValue) -> AppResult<String> {
        let cipher = self.cipher()?;
        let nonce_bytes = STANDARD
            .decode(&encrypted.nonce_b64)
            .map_err(|error| AppError::Crypto(format!("nonce decode failed: {error}")))?;
        let cipher_text = STANDARD
            .decode(&encrypted.ciphertext_b64)
            .map_err(|error| AppError::Crypto(format!("ciphertext decode failed: {error}")))?;

        let plain_text = cipher
            .decrypt(Nonce::from_slice(&nonce_bytes), cipher_text.as_ref())
            .map_err(|error| AppError::Crypto(format!("decrypt failed: {error:?}")))?;

        String::from_utf8(plain_text)
            .map_err(|error| AppError::Crypto(format!("plaintext is not valid UTF-8: {error}")))
    }

    fn cipher(&self) -> AppResult<Aes256GcmSiv> {
        let key_bytes = self.key_bytes.ok_or_else(|| {
            AppError::Crypto(format!(
                "set {} before creating or loading exchange credentials",
                self.env_var_name
            ))
        })?;

        Aes256GcmSiv::new_from_slice(&key_bytes)
            .map_err(|error| AppError::Crypto(format!("invalid cipher key: {error:?}")))
    }
}

pub fn mask_api_key(value: &str) -> String {
    if value.len() <= 6 {
        return "***".to_owned();
    }

    format!("{}***{}", &value[..3], &value[value.len() - 2..])
}
