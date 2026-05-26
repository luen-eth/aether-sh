use common::{AssetStandard, ChainConfig};
use parser::{ParseError, parse_transfer_logs};
use rpc::{RpcClient, RpcError};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use storage::{AssetKey, NftTokenKey, StorageError, Store};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct IndexerService {
    config: ChainConfig,
    rpc: RpcClient,
    store: Store,
}

impl IndexerService {
    pub fn new(config: ChainConfig, rpc: RpcClient, store: Store) -> Self {
        Self { config, rpc, store }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<(), IndexerError> {
        let last_db_block = self.store.last_scanned_block().await?;
        let mut cursor = if last_db_block < self.config.start_block {
            self.config.start_block.saturating_sub(1)
        } else {
            last_db_block
        };

        info!(
            chain_id = self.rpc.chain_id(),
            start_block = self.config.start_block,
            db_block = last_db_block,
            "indexer started"
        );

        loop {
            if *shutdown.borrow() {
                info!("indexer received shutdown signal");
                return Ok(());
            }

            match self.tick(cursor).await {
                Ok(Some(new_cursor)) => cursor = new_cursor,
                Ok(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.poll_interval()) => {}
                        _ = shutdown.changed() => {}
                    }
                }
                Err(err) => {
                    warn!(error = %err, "indexer tick failed, retrying");
                    match self.store.last_scanned_block().await {
                        Ok(last_db_block) => cursor = cursor.max(last_db_block),
                        Err(store_err) => warn!(
                            error = %store_err,
                            "failed to reload indexer cursor after tick error"
                        ),
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.poll_interval()) => {}
                        _ = shutdown.changed() => {}
                    }
                }
            }
        }
    }

    async fn tick(&self, cursor: u64) -> Result<Option<u64>, IndexerError> {
        let head = self.rpc.block_number().await?;
        let safe_head = head.saturating_sub(self.config.confirmations);
        if safe_head <= cursor {
            debug!(head, safe_head, cursor, "no new finalized blocks");
            return Ok(None);
        }

        let from_block = cursor.saturating_add(1);
        let to_block = std::cmp::min(
            safe_head,
            from_block.saturating_add(self.config.chunk_size.saturating_sub(1)),
        );

        let indexed_to = self
            .index_block_range_adaptive(from_block, to_block)
            .await?;
        Ok(Some(indexed_to))
    }

    async fn index_block_range_adaptive(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<u64, IndexerError> {
        let mut ranges = VecDeque::from([(from_block, to_block)]);
        let mut indexed_to = from_block.saturating_sub(1);

        while let Some((range_from, range_to)) = ranges.pop_front() {
            match self.index_block_range(range_from, range_to).await {
                Ok(()) => indexed_to = range_to,
                Err(IndexerError::Rpc(err))
                    if err.is_log_result_limit_exceeded() && range_from < range_to =>
                {
                    let split_to = suggested_split_to(&err, range_from, range_to)
                        .unwrap_or_else(|| range_from + (range_to - range_from) / 2);

                    debug!(
                        from_block = range_from,
                        to_block = range_to,
                        first_to_block = split_to,
                        second_from_block = split_to.saturating_add(1),
                        "rpc log result limit exceeded, splitting block range"
                    );

                    ranges.push_front((split_to.saturating_add(1), range_to));
                    ranges.push_front((range_from, split_to));
                }
                Err(err) => return Err(err),
            }
        }

        Ok(indexed_to)
    }

    async fn index_block_range(&self, from_block: u64, to_block: u64) -> Result<(), IndexerError> {
        let logs = self.rpc.get_transfer_logs(from_block, to_block).await?;
        let transfers = parse_transfer_logs(self.config.chain_id, &logs)?;
        let summary = self.store.ingest_transfers(&transfers).await?;
        self.fetch_new_asset_metadata(&summary.new_assets).await;
        if self.config.enable_auto_metadata_fetch {
            self.fetch_detected_nft_token_uris(&summary.nft_tokens_to_fetch)
                .await;
        }
        self.store.set_last_scanned_block(to_block).await?;

        info!(
            from_block,
            to_block,
            rpc_logs = logs.len(),
            parsed_transfers = transfers.len(),
            inserted_transfers = summary.inserted_transfers,
            discovered_assets = summary.new_assets.len(),
            detected_nft_tokens = summary.nft_tokens_to_fetch.len(),
            "indexed block range"
        );
        Ok(())
    }

    async fn fetch_new_asset_metadata(&self, assets: &[AssetKey]) {
        if assets.is_empty() {
            return;
        }

        let mut discovered = HashMap::new();
        for asset in assets {
            if matches!(asset.standard, AssetStandard::Erc20 | AssetStandard::Erc721) {
                discovered
                    .entry(asset.token_address.clone())
                    .or_insert(asset.standard);
            }
        }
        if discovered.is_empty() {
            return;
        }

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(10));

        for (token_address, standard) in discovered {
            let rpc = self.rpc.clone();
            let store = self.store.clone();
            let sem = semaphore.clone();

            join_set.spawn(async move {
                let Ok(_permit) = sem.acquire().await else {
                    return;
                };

                let name_result = rpc.contract_name(&token_address).await;
                let symbol_result = rpc.contract_symbol(&token_address).await;

                let name = name_result
                    .as_ref()
                    .ok()
                    .and_then(|value| normalize_metadata_text(value));
                let symbol = symbol_result
                    .as_ref()
                    .ok()
                    .and_then(|value| normalize_metadata_text(value));

                let mut failed_fields = Vec::new();
                let mut expected_failures = Vec::new();
                let mut unexpected_failures = Vec::new();

                if let Err(err) = &name_result {
                    failed_fields.push("name");
                    if err.is_expected_metadata_call_failure() {
                        expected_failures.push(format!("name: {err}"));
                    } else {
                        unexpected_failures.push(format!("name: {err}"));
                    }
                }
                if let Err(err) = &symbol_result {
                    failed_fields.push("symbol");
                    if err.is_expected_metadata_call_failure() {
                        expected_failures.push(format!("symbol: {err}"));
                    } else {
                        unexpected_failures.push(format!("symbol: {err}"));
                    }
                }

                if !unexpected_failures.is_empty() {
                    warn!(
                        token_address = %token_address,
                        standard = %standard,
                        failed_fields = %failed_fields.join(","),
                        errors = %unexpected_failures.join(" | "),
                        "failed to fetch token metadata"
                    );
                } else if !expected_failures.is_empty() {
                    debug!(
                        token_address = %token_address,
                        standard = %standard,
                        failed_fields = %failed_fields.join(","),
                        errors = %expected_failures.join(" | "),
                        "token metadata method unavailable/reverted"
                    );
                }

                if name.is_none() && symbol.is_none() {
                    return;
                }

                if let Err(err) = store
                    .set_asset_metadata(&token_address, name.as_deref(), symbol.as_deref())
                    .await
                {
                    warn!(
                        token_address = %token_address,
                        standard = %standard,
                        error = %err,
                        "failed to persist token metadata"
                    );
                    return;
                }

                match (&name, &symbol) {
                    (Some(name), Some(symbol)) => {
                        info!(
                            token_address = %token_address,
                            standard = %standard,
                            token_name = %name,
                            token_symbol = %symbol,
                            "şu isim ve sembolde token bulundu"
                        );
                    }
                    _ => {
                        info!(
                            token_address = %token_address,
                            standard = %standard,
                            token_name = name.as_deref().unwrap_or(""),
                            token_symbol = symbol.as_deref().unwrap_or(""),
                            "token metadata bulundu (kismi)"
                        );
                    }
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(err) = result {
                warn!(error = %err, "token metadata worker failed");
            }
        }
    }

    async fn fetch_detected_nft_token_uris(&self, tokens: &[NftTokenKey]) {
        let pending_tokens = match self.store.nft_tokens_for_initial_uri_fetch(tokens).await {
            Ok(pending) => pending,
            Err(err) => {
                warn!(error = %err, "failed to inspect NFT token URI fetch states");
                return;
            }
        };

        if pending_tokens.is_empty() {
            return;
        }

        // Group pending tokens by token_address (collection)
        let mut collections: HashMap<String, Vec<NftTokenKey>> = HashMap::new();
        for pending in pending_tokens {
            collections
                .entry(pending.token_address.clone())
                .or_default()
                .push(pending);
        }

        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(tokio::sync::Semaphore::new(10)); // limit concurrency to 10 collections at once

        for (token_address, pending_list) in collections {
            let rpc = self.rpc.clone();
            let store = self.store.clone();
            let sem = semaphore.clone();

            join_set.spawn(async move {
                let _permit = sem.acquire().await;

                // 1. Try to fetch an existing example from DB
                let mut inferred = None;
                match store.example_nft_token_uri(&token_address).await {
                    Ok(Some((example_id, example_uri))) => {
                        inferred = infer_prefix_suffix(&example_uri, &example_id);
                    }
                    Err(err) => {
                        warn!(
                            token_address = %token_address,
                            error = %err,
                            "failed to look up example token URI from database"
                        );
                    }
                    _ => {}
                }

                // If not inferred from DB, we fetch exactly ONE token URI from RPC
                let mut fetched_first_item = None;
                if inferred.is_none() && !pending_list.is_empty() {
                    let first = &pending_list[0];
                    match rpc
                        .token_uri(first.standard, &first.token_address, &first.token_id)
                        .await
                    {
                        Ok(token_uri) => {
                            inferred = infer_prefix_suffix(&token_uri, &first.token_id);
                            fetched_first_item = Some((first.token_id.clone(), token_uri));
                        }
                        Err(err) => {
                            let error_str = err.to_string();
                            let items_to_fail: Vec<_> = pending_list
                                .iter()
                                .map(|t| {
                                    (
                                        t.token_address.as_str(),
                                        t.token_id.as_str(),
                                        error_str.as_str(),
                                    )
                                })
                                .collect();
                            let _ = store.mark_nft_token_uris_fetch_failed(&items_to_fail).await;
                            warn!(
                                token_address = %token_address,
                                error = %err,
                                "failed to fetch first NFT token URI, marked collection as failed"
                            );
                            return;
                        }
                    }
                }

                // 2. Perform propagation if inferred
                if let Some((prefix, suffix)) = inferred {
                    let standard = pending_list[0].standard;
                    let mut calculated_uris = Vec::new();
                    for item in &pending_list {
                        if fetched_first_item.as_ref().map(|f| &f.0) == Some(&item.token_id) {
                            continue;
                        }
                        let calculated_uri = format!("{}{}{}", prefix, item.token_id, suffix);
                        calculated_uris.push(calculated_uri);
                    }

                    let mut items_to_set = Vec::new();
                    if let Some((first_id, first_uri)) = &fetched_first_item {
                        items_to_set.push((first_id.as_str(), first_uri.as_str()));
                    }

                    let mut calc_iter = calculated_uris.iter();
                    for item in &pending_list {
                        if fetched_first_item.as_ref().map(|f| &f.0) == Some(&item.token_id) {
                            continue;
                        }
                        if let Some(uri) = calc_iter.next() {
                            items_to_set.push((item.token_id.as_str(), uri.as_str()));
                        }
                    }

                    if let Err(err) = store
                        .set_nft_token_uris(&token_address, standard, &items_to_set)
                        .await
                    {
                        warn!(
                            token_address = %token_address,
                            error = %err,
                            "failed to persist bulk NFT token URIs"
                        );
                    }
                } else {
                    // 3. Fallback: fetch individually if custom URIs are used
                    let mut individual_join_set = JoinSet::new();
                    let ind_sem = Arc::new(tokio::sync::Semaphore::new(5));

                    for item in pending_list {
                        let item_rpc = rpc.clone();
                        let item_store = store.clone();
                        let item_sem = ind_sem.clone();

                        individual_join_set.spawn(async move {
                            let _permit = item_sem.acquire().await;
                            match item_rpc
                                .token_uri(item.standard, &item.token_address, &item.token_id)
                                .await
                            {
                                Ok(token_uri) => {
                                    let _ = item_store
                                        .set_nft_token_uri(
                                            &item.token_address,
                                            &item.token_id,
                                            item.standard,
                                            &token_uri,
                                        )
                                        .await;
                                }
                                Err(err) => {
                                    let _ = item_store
                                        .mark_nft_token_uri_fetch_failed(
                                            &item.token_address,
                                            &item.token_id,
                                            &err.to_string(),
                                        )
                                        .await;
                                }
                            }
                        });
                    }

                    while individual_join_set.join_next().await.is_some() {}
                }
            });
        }

        while join_set.join_next().await.is_some() {}
    }
}

fn decimal_to_erc1155_hex_id(token_id: &str) -> String {
    let trimmed = token_id.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|b| b.is_ascii_digit()) {
        return String::new();
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

    let mut raw = nibbles
        .into_iter()
        .rev()
        .map(|n| char::from_digit(n as u32, 16).unwrap_or('0'))
        .collect::<String>();
    if raw.is_empty() {
        raw.push('0');
    }
    format!("{raw:0>64}")
}

fn infer_prefix_suffix(uri: &str, token_id: &str) -> Option<(String, String)> {
    let raw_token_id = token_id.trim();
    if raw_token_id.is_empty() {
        return None;
    }

    let markers = [
        raw_token_id.to_owned(),
        "{id}".to_owned(),
        decimal_to_erc1155_hex_id(raw_token_id),
    ];

    for marker in markers.iter().filter(|m| !m.is_empty()) {
        if let Some(idx) = uri.rfind(marker) {
            let prefix = uri[..idx].to_owned();
            let suffix = uri[idx + marker.len()..].to_owned();
            if !prefix.is_empty() {
                return Some((prefix, suffix));
            }
        }
    }
    None
}

fn suggested_split_to(err: &RpcError, range_from: u64, range_to: u64) -> Option<u64> {
    let (_, suggested_to) = err.suggested_log_retry_range()?;
    if (range_from..range_to).contains(&suggested_to) {
        Some(suggested_to)
    } else {
        None
    }
}

fn normalize_metadata_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

#[derive(Debug, Error)]
pub enum IndexerError {
    #[error("rpc error: {0}")]
    Rpc(#[from] RpcError),
    #[error("parse error: {0}")]
    Parse(#[from] ParseError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
}

#[cfg(test)]
mod tests {
    use super::suggested_split_to;
    use rpc::RpcError;

    #[test]
    fn uses_rpc_suggested_log_range_when_inside_requested_range() {
        let err = RpcError::AllEndpointsFailed(
            "rpc error -32602: query exceeds max results 20000, retry with the range 6326001-6326937"
                .to_owned(),
        );

        assert_eq!(
            suggested_split_to(&err, 6_326_001, 6_327_000),
            Some(6_326_937)
        );
    }

    #[test]
    fn ignores_rpc_suggested_log_range_when_outside_requested_range() {
        let err = RpcError::AllEndpointsFailed(
            "rpc error -32602: query exceeds max results 20000, retry with the range 6326001-6326937"
                .to_owned(),
        );

        assert_eq!(suggested_split_to(&err, 6_326_938, 6_327_000), None);
    }
}
