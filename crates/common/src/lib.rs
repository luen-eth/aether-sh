use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::time::Duration;
use thiserror::Error;

pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetStandard {
    Erc20,
    Erc721,
    Erc1155,
}

impl AssetStandard {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Erc20 => "erc20",
            Self::Erc721 => "erc721",
            Self::Erc1155 => "erc1155",
        }
    }
}

impl std::fmt::Display for AssetStandard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for AssetStandard {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "erc20" => Ok(Self::Erc20),
            "erc721" => Ok(Self::Erc721),
            "erc1155" => Ok(Self::Erc1155),
            _ => Err(ConfigError::InvalidEnv {
                key: "ASSET_STANDARD".to_owned(),
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub chain: ChainConfig,
    pub database: DatabaseConfig,
    pub api: ApiConfig,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let primary_rpc = env_required("AETHER_RPC_HTTP_URL")?;
        if primary_rpc.trim().is_empty() {
            return Err(ConfigError::InvalidEnv {
                key: "AETHER_RPC_HTTP_URL".to_owned(),
                value: primary_rpc,
            });
        }

        let mut rpc_http_urls = vec![primary_rpc.trim().to_owned()];
        for key in ["AETHER_RPC_HTTP_URL_2", "AETHER_RPC_HTTP_URL_3"] {
            if let Some(url) = env_optional_nonempty(key) {
                if !rpc_http_urls.iter().any(|existing| existing == &url) {
                    rpc_http_urls.push(url);
                }
            }
        }

        let chain = ChainConfig {
            rpc_http_urls,
            chain_id: env_parse("AETHER_CHAIN_ID", 1)?,
            start_block: env_parse("AETHER_START_BLOCK", 0)?,
            confirmations: env_parse("AETHER_CONFIRMATIONS", 12)?,
            chunk_size: env_parse("AETHER_CHUNK_SIZE", 10_000)?,
            poll_interval_ms: env_parse("AETHER_POLL_INTERVAL_MS", 4_000)?,
            request_timeout_ms: env_parse("AETHER_RPC_TIMEOUT_MS", 20_000)?,
            rate_limit_backoff_ms: env_parse("AETHER_RPC_RATE_LIMIT_BACKOFF_MS", 1_000)?,
            enable_auto_metadata_fetch: env_parse("AETHER_ENABLE_AUTO_METADATA_FETCH", true)?,
        };

        let database = DatabaseConfig {
            url: env_required("AETHER_DATABASE_URL")?,
            max_connections: env_parse("AETHER_DB_MAX_CONNECTIONS", 20)?,
        };

        let api = ApiConfig {
            bind_addr: env_value_or("AETHER_API_BIND", "0.0.0.0:8090"),
            page_size_default: env_parse("AETHER_API_PAGE_SIZE_DEFAULT", 50)?,
            page_size_max: env_parse("AETHER_API_PAGE_SIZE_MAX", 200)?,
        };

        Ok(Self {
            chain,
            database,
            api,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainConfig {
    pub rpc_http_urls: Vec<String>,
    pub chain_id: u64,
    pub start_block: u64,
    pub confirmations: u64,
    pub chunk_size: u64,
    pub poll_interval_ms: u64,
    pub request_timeout_ms: u64,
    pub rate_limit_backoff_ms: u64,
    pub enable_auto_metadata_fetch: bool,
}

impl ChainConfig {
    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms.max(500))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub bind_addr: String,
    pub page_size_default: u32,
    pub page_size_max: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferEvent {
    pub chain_id: u64,
    pub block_number: u64,
    pub block_hash: String,
    pub tx_hash: String,
    pub log_index: u64,
    pub token_address: String,
    pub standard: AssetStandard,
    pub from_address: String,
    pub to_address: String,
    pub token_id: Option<String>,
    pub value: String,
    pub batch_index: Option<u32>,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserTokenBalance {
    pub token_address: String,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<i32>,
    pub balance: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserNftBalance {
    pub token_address: String,
    pub standard: AssetStandard,
    pub token_id: String,
    pub token_uri: Option<String>,
    pub balance: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssetSearchItem {
    pub token_address: String,
    pub standard: AssetStandard,
    pub name: Option<String>,
    pub symbol: Option<String>,
    pub decimals: Option<i32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HolderItem {
    pub holder_address: String,
    pub amount: String,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("missing required environment variable: {0}")]
    MissingEnv(String),
    #[error("invalid environment value for {key}: {value}")]
    InvalidEnv { key: String, value: String },
}

fn env_required(key: &str) -> Result<String, ConfigError> {
    env::var(key).map_err(|_| ConfigError::MissingEnv(key.to_owned()))
}

fn env_value_or(key: &str, fallback: &str) -> String {
    env::var(key).unwrap_or_else(|_| fallback.to_owned())
}

fn env_optional_nonempty(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_owned())
            }
        }
        Err(_) => None,
    }
}

fn env_parse<T>(key: &str, fallback: T) -> Result<T, ConfigError>
where
    T: std::str::FromStr,
{
    match env::var(key) {
        Ok(value) => value.parse::<T>().map_err(|_| ConfigError::InvalidEnv {
            key: key.to_owned(),
            value,
        }),
        Err(_) => Ok(fallback),
    }
}

pub fn normalize_address(address: &str) -> String {
    let trimmed = address.trim();
    let raw = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    let cleaned = raw.trim_start_matches('0');
    if cleaned.is_empty() {
        ZERO_ADDRESS.to_owned()
    } else {
        let padded = format!("{cleaned:0>40}");
        format!("0x{}", padded.to_ascii_lowercase())
    }
}

pub fn normalize_hash(hash: &str) -> String {
    let trimmed = hash.trim();
    let raw = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    format!("0x{}", raw.to_ascii_lowercase())
}

pub fn hex_to_u64(hex: &str) -> Result<u64, ConfigError> {
    let raw = hex
        .strip_prefix("0x")
        .or_else(|| hex.strip_prefix("0X"))
        .unwrap_or(hex);
    u64::from_str_radix(raw, 16).map_err(|_| ConfigError::InvalidEnv {
        key: "hex_u64".to_owned(),
        value: hex.to_owned(),
    })
}

pub fn hex_to_decimal_string(hex: &str) -> Result<String, ConfigError> {
    let raw = hex
        .strip_prefix("0x")
        .or_else(|| hex.strip_prefix("0X"))
        .unwrap_or(hex)
        .trim_start_matches('0');
    if raw.is_empty() {
        return Ok("0".to_owned());
    }

    let mut digits = vec![0u8];
    for ch in raw.bytes() {
        let nibble = match ch {
            b'0'..=b'9' => ch - b'0',
            b'a'..=b'f' => ch - b'a' + 10,
            b'A'..=b'F' => ch - b'A' + 10,
            _ => {
                return Err(ConfigError::InvalidEnv {
                    key: "hex_decimal".to_owned(),
                    value: hex.to_owned(),
                });
            }
        };

        let mut carry = nibble as u32;
        for digit in &mut digits {
            let value = (*digit as u32) * 16 + carry;
            *digit = (value % 10) as u8;
            carry = value / 10;
        }
        while carry > 0 {
            digits.push((carry % 10) as u8);
            carry /= 10;
        }
    }

    digits.reverse();
    let output = digits
        .into_iter()
        .map(|d| (b'0' + d) as char)
        .collect::<String>();
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::{hex_to_decimal_string, normalize_address};

    #[test]
    fn converts_big_hex_to_decimal() {
        let number = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
        let decimal = hex_to_decimal_string(number).unwrap();
        assert_eq!(
            decimal,
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn normalizes_short_address() {
        assert_eq!(
            normalize_address("0xabc"),
            "0x0000000000000000000000000000000000000abc"
        );
    }
}
