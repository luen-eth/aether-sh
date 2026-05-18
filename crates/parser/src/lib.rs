use chrono::Utc;
use common::{AssetStandard, ConfigError, TransferEvent, hex_to_decimal_string, normalize_address};
use rpc::{
    ERC_TRANSFER_TOPIC, ERC1155_TRANSFER_BATCH_TOPIC, ERC1155_TRANSFER_SINGLE_TOPIC, RpcLog,
};
use thiserror::Error;

pub fn parse_transfer_logs(
    chain_id: u64,
    logs: &[RpcLog],
) -> Result<Vec<TransferEvent>, ParseError> {
    let mut out = Vec::new();
    for log in logs {
        if log.removed {
            continue;
        }

        if let Some(topic0) = log.topics.first() {
            match topic0.as_str() {
                ERC_TRANSFER_TOPIC => parse_erc20_or_erc721(chain_id, log, &mut out)?,
                ERC1155_TRANSFER_SINGLE_TOPIC => parse_erc1155_single(chain_id, log, &mut out)?,
                ERC1155_TRANSFER_BATCH_TOPIC => parse_erc1155_batch(chain_id, log, &mut out)?,
                _ => {}
            }
        }
    }
    Ok(out)
}

fn parse_erc20_or_erc721(
    chain_id: u64,
    log: &RpcLog,
    out: &mut Vec<TransferEvent>,
) -> Result<(), ParseError> {
    if log.topics.len() < 3 {
        return Ok(());
    }

    let from = topic_to_address(&log.topics[1])?;
    let to = topic_to_address(&log.topics[2])?;

    if log.topics.len() >= 4 {
        let token_id = topic_to_decimal_word(&log.topics[3])?;
        out.push(TransferEvent {
            chain_id,
            block_number: log.block_number,
            block_hash: log.block_hash.clone(),
            tx_hash: log.transaction_hash.clone(),
            log_index: log.log_index,
            token_address: log.address.clone(),
            standard: AssetStandard::Erc721,
            from_address: from,
            to_address: to,
            token_id: Some(token_id),
            value: "1".to_owned(),
            batch_index: None,
            indexed_at: Utc::now(),
        });
        return Ok(());
    }

    let value = topic_to_decimal_word(&log.data)?;
    out.push(TransferEvent {
        chain_id,
        block_number: log.block_number,
        block_hash: log.block_hash.clone(),
        tx_hash: log.transaction_hash.clone(),
        log_index: log.log_index,
        token_address: log.address.clone(),
        standard: AssetStandard::Erc20,
        from_address: from,
        to_address: to,
        token_id: None,
        value,
        batch_index: None,
        indexed_at: Utc::now(),
    });
    Ok(())
}

fn parse_erc1155_single(
    chain_id: u64,
    log: &RpcLog,
    out: &mut Vec<TransferEvent>,
) -> Result<(), ParseError> {
    if log.topics.len() < 4 {
        return Ok(());
    }

    let from = topic_to_address(&log.topics[2])?;
    let to = topic_to_address(&log.topics[3])?;
    let words = data_words(&log.data)?;
    if words.len() < 2 {
        return Ok(());
    }

    let token_id = hex_to_decimal_string(&prefixed(&words[0])).map_err(ParseError::ParseConfig)?;
    let value = hex_to_decimal_string(&prefixed(&words[1])).map_err(ParseError::ParseConfig)?;
    out.push(TransferEvent {
        chain_id,
        block_number: log.block_number,
        block_hash: log.block_hash.clone(),
        tx_hash: log.transaction_hash.clone(),
        log_index: log.log_index,
        token_address: log.address.clone(),
        standard: AssetStandard::Erc1155,
        from_address: from,
        to_address: to,
        token_id: Some(token_id),
        value,
        batch_index: Some(0),
        indexed_at: Utc::now(),
    });
    Ok(())
}

fn parse_erc1155_batch(
    chain_id: u64,
    log: &RpcLog,
    out: &mut Vec<TransferEvent>,
) -> Result<(), ParseError> {
    if log.topics.len() < 4 {
        return Ok(());
    }

    let from = topic_to_address(&log.topics[2])?;
    let to = topic_to_address(&log.topics[3])?;
    let words = data_words(&log.data)?;
    if words.len() < 2 {
        return Ok(());
    }

    let ids_offset_words = word_to_usize(&words[0])? / 32;
    let values_offset_words = word_to_usize(&words[1])? / 32;
    if ids_offset_words >= words.len() || values_offset_words >= words.len() {
        return Ok(());
    }

    let ids_len = word_to_usize(&words[ids_offset_words])?;
    let values_len = word_to_usize(&words[values_offset_words])?;
    if ids_len == 0 || ids_len != values_len {
        return Ok(());
    }

    let ids_begin = ids_offset_words + 1;
    let values_begin = values_offset_words + 1;
    if ids_begin + ids_len > words.len() || values_begin + values_len > words.len() {
        return Ok(());
    }

    for index in 0..ids_len {
        let token_id = hex_to_decimal_string(&prefixed(&words[ids_begin + index]))
            .map_err(ParseError::ParseConfig)?;
        let value = hex_to_decimal_string(&prefixed(&words[values_begin + index]))
            .map_err(ParseError::ParseConfig)?;
        out.push(TransferEvent {
            chain_id,
            block_number: log.block_number,
            block_hash: log.block_hash.clone(),
            tx_hash: log.transaction_hash.clone(),
            log_index: log.log_index,
            token_address: log.address.clone(),
            standard: AssetStandard::Erc1155,
            from_address: from.clone(),
            to_address: to.clone(),
            token_id: Some(token_id),
            value,
            batch_index: Some(index as u32),
            indexed_at: Utc::now(),
        });
    }

    Ok(())
}

fn topic_to_address(word: &str) -> Result<String, ParseError> {
    let raw = strip_0x(word);
    if raw.len() < 40 {
        return Err(ParseError::InvalidAddressWord(word.to_owned()));
    }
    let address = &raw[raw.len() - 40..];
    Ok(normalize_address(address))
}

fn topic_to_decimal_word(word: &str) -> Result<String, ParseError> {
    hex_to_decimal_string(word).map_err(ParseError::ParseConfig)
}

fn data_words(data: &str) -> Result<Vec<String>, ParseError> {
    let raw = strip_0x(data);
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    if raw.len() % 64 != 0 {
        return Err(ParseError::InvalidData(data.to_owned()));
    }

    let mut words = Vec::with_capacity(raw.len() / 64);
    for chunk in raw.as_bytes().chunks(64) {
        words.push(std::str::from_utf8(chunk).unwrap_or_default().to_owned());
    }
    Ok(words)
}

fn word_to_usize(word: &str) -> Result<usize, ParseError> {
    let raw = word.trim_start_matches('0');
    if raw.is_empty() {
        return Ok(0);
    }
    usize::from_str_radix(raw, 16).map_err(|_| ParseError::InvalidData(word.to_owned()))
}

fn prefixed(word: &str) -> String {
    format!("0x{word}")
}

fn strip_0x(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid indexed address word: {0}")]
    InvalidAddressWord(String),
    #[error("invalid data payload: {0}")]
    InvalidData(String),
    #[error("parse error: {0}")]
    ParseConfig(ConfigError),
}

#[cfg(test)]
mod tests {
    use super::data_words;

    #[test]
    fn parses_words() {
        let words =
            data_words("0x0000000000000000000000000000000000000000000000000000000000000001")
                .unwrap();
        assert_eq!(words.len(), 1);
    }
}
