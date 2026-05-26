use common::{
    AssetStandard, ChainConfig, ConfigError, hex_to_u64, normalize_address, normalize_hash,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tracing::{debug, warn};

pub const ERC_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub const ERC1155_TRANSFER_SINGLE_TOPIC: &str =
    "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
pub const ERC1155_TRANSFER_BATCH_TOPIC: &str =
    "0x4a39dc06d4c0dbc64b70f7f1d58fca64d4d8105b8f476e2f70f11f3b9f7f62cb";
const ERC721_TOKEN_URI_SELECTOR: &str = "c87b56dd";
const ERC1155_URI_SELECTOR: &str = "0e89341c";
const NAME_SELECTOR: &str = "06fdde03";
const SYMBOL_SELECTOR: &str = "95d89b41";

#[derive(Debug, Clone)]
pub struct RpcClient {
    chain_id: u64,
    http_urls: Arc<Vec<String>>,
    http: Client,
    preferred_index: Arc<AtomicUsize>,
    rate_limit_backoff: Duration,
}

impl RpcClient {
    pub fn new(config: &ChainConfig) -> Result<Self, RpcError> {
        if config.rpc_http_urls.is_empty() {
            return Err(RpcError::NoEndpoints);
        }

        let http = Client::builder()
            .timeout(std::time::Duration::from_millis(
                config.request_timeout_ms.max(1_000),
            ))
            .build()
            .map_err(RpcError::HttpClientBuild)?;

        Ok(Self {
            chain_id: config.chain_id,
            http_urls: Arc::new(config.rpc_http_urls.clone()),
            http,
            preferred_index: Arc::new(AtomicUsize::new(0)),
            rate_limit_backoff: Duration::from_millis(config.rate_limit_backoff_ms.max(250)),
        })
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub async fn block_number(&self) -> Result<u64, RpcError> {
        let result: String = self.rpc_call("eth_blockNumber", json!([])).await?;
        hex_to_u64(&result).map_err(RpcError::ParseConfig)
    }

    pub async fn get_transfer_logs(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<Vec<RpcLog>, RpcError> {
        let params = json!([{
            "fromBlock": to_hex_block(from_block),
            "toBlock": to_hex_block(to_block),
            "topics": [[
                ERC_TRANSFER_TOPIC,
                ERC1155_TRANSFER_SINGLE_TOPIC,
                ERC1155_TRANSFER_BATCH_TOPIC
            ]]
        }]);

        let logs: Vec<RpcLogRaw> = self.rpc_call("eth_getLogs", params).await?;
        logs.into_iter().map(RpcLog::try_from).collect()
    }

    pub async fn token_uri(
        &self,
        standard: AssetStandard,
        token_address: &str,
        token_id: &str,
    ) -> Result<String, RpcError> {
        let selector = match standard {
            AssetStandard::Erc721 => ERC721_TOKEN_URI_SELECTOR,
            AssetStandard::Erc1155 => ERC1155_URI_SELECTOR,
            AssetStandard::Erc20 => {
                return Err(RpcError::UnsupportedTokenUriStandard(standard.as_str()));
            }
        };
        let encoded_token_id = decimal_to_u256_word(token_id)?;
        let call_data = format!("0x{selector}{encoded_token_id}");
        let params = json!([{
            "to": normalize_address(token_address),
            "data": call_data,
        }, "latest"]);

        let raw: String = self.rpc_call("eth_call", params).await?;
        decode_abi_string(&raw)
    }

    pub async fn contract_name(&self, token_address: &str) -> Result<String, RpcError> {
        self.eth_call_string(token_address, NAME_SELECTOR).await
    }

    pub async fn contract_symbol(&self, token_address: &str) -> Result<String, RpcError> {
        self.eth_call_string(token_address, SYMBOL_SELECTOR).await
    }

    async fn eth_call_string(
        &self,
        token_address: &str,
        selector: &str,
    ) -> Result<String, RpcError> {
        let params = json!([{
            "to": normalize_address(token_address),
            "data": format!("0x{selector}"),
        }, "latest"]);
        let raw: String = self.rpc_call("eth_call", params).await?;
        decode_abi_string(&raw)
    }

    async fn rpc_call<T>(&self, method: &str, params: serde_json::Value) -> Result<T, RpcError>
    where
        T: serde::de::DeserializeOwned,
    {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let total = self.http_urls.len();
        if total == 0 {
            return Err(RpcError::NoEndpoints);
        }

        let mut rate_limit_retries = 0u64;

        loop {
            let start = self.preferred_index.load(Ordering::Relaxed) % total;
            let mut last_error_message = String::new();
            let mut rate_limited_endpoints = 0usize;
            let mut rate_limit_delay = self.rate_limit_backoff;

            for attempt in 0..total {
                let index = (start + attempt) % total;
                let endpoint = &self.http_urls[index];
                match self.rpc_call_single(endpoint, &payload).await {
                    Ok(value) => {
                        if attempt > 0 {
                            warn!(
                                method,
                                endpoint,
                                failed_endpoints = attempt,
                                "rpc failover switched endpoint"
                            );
                        }
                        self.preferred_index.store(index, Ordering::Relaxed);
                        return Ok(value);
                    }
                    Err(err) => {
                        last_error_message = format!("{endpoint}: {err}");
                        if err.is_rate_limited() {
                            rate_limited_endpoints += 1;
                            let delay = err.suggested_rate_limit_delay(self.rate_limit_backoff);
                            if delay > rate_limit_delay {
                                rate_limit_delay = delay;
                            }
                            debug!(
                                method,
                                endpoint,
                                error = %err,
                                "rpc endpoint rate limited"
                            );
                        } else if method == "eth_getLogs" && err.is_log_result_limit_exceeded() {
                            debug!(
                                method,
                                endpoint,
                                error = %err,
                                "rpc log result limit reached"
                            );
                        } else if method == "eth_call" && err.is_expected_metadata_call_failure() {
                            debug!(
                                method,
                                endpoint,
                                error = %err,
                                "rpc eth_call metadata method unavailable/reverted"
                            );
                        } else {
                            warn!(
                                method,
                                endpoint,
                                error = %err,
                                "rpc endpoint failed"
                            );
                        }
                    }
                }
            }

            if rate_limited_endpoints == total {
                rate_limit_retries += 1;
                warn!(
                    method,
                    delay_ms = rate_limit_delay.as_millis(),
                    retry = rate_limit_retries,
                    "all rpc endpoints rate limited, waiting before retry"
                );
                tokio::time::sleep(rate_limit_delay).await;
                continue;
            }

            return Err(RpcError::AllEndpointsFailed(last_error_message));
        }
    }

    async fn rpc_call_single<T>(
        &self,
        endpoint: &str,
        payload: &serde_json::Value,
    ) -> Result<T, RpcError>
    where
        T: serde::de::DeserializeOwned,
    {
        let response = self
            .http
            .post(endpoint)
            .json(payload)
            .send()
            .await
            .map_err(RpcError::HttpRequest)?;

        let status = response.status();
        let body = response.text().await.map_err(RpcError::HttpBody)?;
        if !status.is_success() {
            return Err(RpcError::RpcStatus(status.as_u16(), body));
        }

        let rpc_response: RpcEnvelope<T> =
            serde_json::from_str(&body).map_err(|e| RpcError::RpcDecode(e, body.clone()))?;
        if let Some(error) = rpc_response.error {
            return Err(RpcError::Rpc(error.code, error.message));
        }

        rpc_response
            .result
            .ok_or_else(|| RpcError::RpcDecodeMissingResult(body))
    }
}

#[derive(Debug, Clone, Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
    error: Option<RpcEnvelopeError>,
}

#[derive(Debug, Clone, Deserialize)]
struct RpcEnvelopeError {
    code: i64,
    message: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct RpcLogRaw {
    address: String,
    topics: Vec<String>,
    data: String,
    block_number: String,
    block_hash: String,
    transaction_hash: String,
    log_index: String,
    removed: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub log_index: u64,
    pub removed: bool,
}

impl TryFrom<RpcLogRaw> for RpcLog {
    type Error = RpcError;

    fn try_from(raw: RpcLogRaw) -> Result<Self, Self::Error> {
        Ok(Self {
            address: normalize_address(&raw.address),
            topics: raw.topics.into_iter().map(|t| normalize_hash(&t)).collect(),
            data: normalize_hash(&raw.data),
            block_number: hex_to_u64(&raw.block_number).map_err(RpcError::ParseConfig)?,
            block_hash: normalize_hash(&raw.block_hash),
            transaction_hash: normalize_hash(&raw.transaction_hash),
            log_index: hex_to_u64(&raw.log_index).map_err(RpcError::ParseConfig)?,
            removed: raw.removed.unwrap_or(false),
        })
    }
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("no rpc endpoints configured")]
    NoEndpoints,
    #[error("http client build error: {0}")]
    HttpClientBuild(reqwest::Error),
    #[error("http request error: {0}")]
    HttpRequest(reqwest::Error),
    #[error("http body read error: {0}")]
    HttpBody(reqwest::Error),
    #[error("non-success rpc status {0}: {1}")]
    RpcStatus(u16, String),
    #[error("rpc response decode error: {0}; body: {1}")]
    RpcDecode(serde_json::Error, String),
    #[error("rpc response missing result: {0}")]
    RpcDecodeMissingResult(String),
    #[error("rpc error {0}: {1}")]
    Rpc(i64, String),
    #[error("parse error: {0}")]
    ParseConfig(ConfigError),
    #[error("all rpc endpoints failed; last error: {0}")]
    AllEndpointsFailed(String),
    #[error("token URI is not supported for {0}")]
    UnsupportedTokenUriStandard(&'static str),
    #[error("invalid decimal token id: {0}")]
    InvalidTokenId(String),
    #[error("ABI string decode error: {0}")]
    AbiStringDecode(String),
}

impl RpcError {
    pub fn is_rate_limited(&self) -> bool {
        match self {
            Self::RpcStatus(429, _) => true,
            Self::Rpc(code, message) => {
                matches!(*code, -32007 | -32008) || is_rate_limit_message(message)
            }
            Self::AllEndpointsFailed(message) => is_rate_limit_message(message),
            _ => false,
        }
    }

    pub fn suggested_rate_limit_delay(&self, fallback: Duration) -> Duration {
        let Some(message) = self.message_for_delay_hint() else {
            return fallback;
        };

        let normalized = message.to_ascii_lowercase();
        if normalized.contains("/minute") || normalized.contains("per minute") {
            Duration::from_secs(60)
        } else if normalized.contains("/second") || normalized.contains("per second") {
            Duration::from_secs(1)
        } else {
            fallback
        }
    }

    pub fn is_log_result_limit_exceeded(&self) -> bool {
        match self {
            Self::Rpc(code, message) => *code == -32602 && is_log_result_limit_message(message),
            Self::AllEndpointsFailed(message) => is_log_result_limit_message(message),
            _ => false,
        }
    }

    pub fn suggested_log_retry_range(&self) -> Option<(u64, u64)> {
        match self {
            Self::Rpc(_, message) | Self::AllEndpointsFailed(message) => {
                parse_log_retry_range(message)
            }
            _ => None,
        }
    }

    pub fn is_expected_metadata_call_failure(&self) -> bool {
        let Some(message) = self.message_for_delay_hint() else {
            return false;
        };
        is_expected_metadata_call_failure_message(message)
    }

    fn message_for_delay_hint(&self) -> Option<&str> {
        match self {
            Self::RpcStatus(_, message)
            | Self::Rpc(_, message)
            | Self::AllEndpointsFailed(message) => Some(message),
            _ => None,
        }
    }
}

fn is_rate_limit_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("rate limit") || normalized.contains("request limit reached")
}

fn is_log_result_limit_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("query exceeds max results")
        || normalized.contains("response size should not exceed")
        || normalized.contains("too many results")
        || normalized.contains("more than")
            && normalized.contains("results")
            && normalized.contains("eth_getlogs")
}

fn is_expected_metadata_call_failure_message(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();
    normalized.contains("invalidjump")
        || normalized.contains("invalid opcode")
        || normalized.contains("execution reverted")
        || normalized.contains("function selector was not recognized")
        || normalized.contains("no fallback function")
        || normalized.contains("evm error")
}

fn parse_log_retry_range(message: &str) -> Option<(u64, u64)> {
    let retry_pos = message.to_ascii_lowercase().find("retry with the range")?;
    let tail = &message[retry_pos..];
    let mut numbers = tail
        .split(|ch: char| !ch.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .filter_map(|part| part.parse::<u64>().ok());

    let from = numbers.next()?;
    let to = numbers.next()?;
    if from <= to { Some((from, to)) } else { None }
}

fn to_hex_block(value: u64) -> String {
    format!("0x{value:x}")
}

fn decimal_to_u256_word(decimal: &str) -> Result<String, RpcError> {
    let trimmed = decimal.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return Err(RpcError::InvalidTokenId(decimal.to_owned()));
    }

    let mut nibbles = vec![0u8];
    for ch in trimmed.bytes() {
        let digit = ch - b'0';
        let mut carry = digit as u16;
        for nibble in &mut nibbles {
            let value = (*nibble as u16) * 10 + carry;
            *nibble = (value % 16) as u8;
            carry = value / 16;
        }
        while carry > 0 {
            nibbles.push((carry % 16) as u8);
            carry /= 16;
        }
    }

    while nibbles.len() > 1 && nibbles.last() == Some(&0) {
        nibbles.pop();
    }
    if nibbles.len() > 64 {
        return Err(RpcError::InvalidTokenId(decimal.to_owned()));
    }

    let mut raw = nibbles
        .into_iter()
        .rev()
        .map(|n| char::from_digit(n as u32, 16).unwrap_or('0'))
        .collect::<String>();
    if raw.is_empty() {
        raw.push('0');
    }
    Ok(format!("{raw:0>64}"))
}

fn decode_abi_string(value: &str) -> Result<String, RpcError> {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    if raw.is_empty() {
        return Err(RpcError::AbiStringDecode(
            "empty eth_call result".to_owned(),
        ));
    }

    let bytes =
        hex::decode(raw).map_err(|err| RpcError::AbiStringDecode(format!("invalid hex: {err}")))?;
    if bytes.len() >= 64 {
        let offset = word_to_usize(&bytes[0..32])?;
        if offset + 32 <= bytes.len() {
            let len = word_to_usize(&bytes[offset..offset + 32])?;
            let begin = offset + 32;
            let end = begin.saturating_add(len);
            if end <= bytes.len() {
                return String::from_utf8(bytes[begin..end].to_vec())
                    .map_err(|err| RpcError::AbiStringDecode(format!("invalid utf-8: {err}")));
            }
        }
    }

    if bytes.len() == 32 {
        let trimmed = bytes
            .into_iter()
            .take_while(|b| *b != 0)
            .collect::<Vec<u8>>();
        if !trimmed.is_empty() {
            return String::from_utf8(trimmed)
                .map_err(|err| RpcError::AbiStringDecode(format!("invalid bytes32 utf-8: {err}")));
        }
    }

    Err(RpcError::AbiStringDecode(
        "unsupported ABI string return encoding".to_owned(),
    ))
}

fn word_to_usize(word: &[u8]) -> Result<usize, RpcError> {
    if word.len() != 32 {
        return Err(RpcError::AbiStringDecode(
            "invalid ABI word length".to_owned(),
        ));
    }

    let mut value = 0usize;
    for byte in word {
        value = value
            .checked_mul(256)
            .and_then(|v| v.checked_add(*byte as usize))
            .ok_or_else(|| RpcError::AbiStringDecode("ABI word overflows usize".to_owned()))?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{RpcError, decimal_to_u256_word, decode_abi_string};
    use std::time::Duration;

    #[test]
    fn encodes_decimal_token_id() {
        assert_eq!(
            decimal_to_u256_word("123").unwrap(),
            "000000000000000000000000000000000000000000000000000000000000007b"
        );
    }

    #[test]
    fn decodes_dynamic_abi_string() {
        let value = "0x0000000000000000000000000000000000000000000000000000000000000020\
0000000000000000000000000000000000000000000000000000000000000018\
697066733a2f2f636f6c6c656374696f6e2f312e6a736f6e0000000000000000000000";
        assert_eq!(
            decode_abi_string(value).unwrap(),
            "ipfs://collection/1.json"
        );
    }

    #[test]
    fn detects_log_result_limit_errors() {
        let err = RpcError::AllEndpointsFailed(
            "https://rpc.testnet.arc.network: rpc error -32602: query exceeds max results 20000, retry with the range 6326001-6326937"
                .to_owned(),
        );

        assert!(err.is_log_result_limit_exceeded());
        assert_eq!(
            err.suggested_log_retry_range(),
            Some((6_326_001, 6_326_937))
        );
    }

    #[test]
    fn detects_http_rate_limit_errors() {
        let err = RpcError::RpcStatus(
            429,
            r#"{"code":-32007,"message":"100/second request limit reached"}"#.to_owned(),
        );

        assert!(err.is_rate_limited());
        assert_eq!(
            err.suggested_rate_limit_delay(Duration::from_millis(250)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn uses_minute_delay_for_minute_rate_limits() {
        let err = RpcError::RpcStatus(
            429,
            r#"{"code":-32008,"message":"3000/minute request limit reached"}"#.to_owned(),
        );

        assert_eq!(
            err.suggested_rate_limit_delay(Duration::from_secs(1)),
            Duration::from_secs(60)
        );
    }
}
