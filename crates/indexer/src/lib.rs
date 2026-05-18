use common::ChainConfig;
use parser::{ParseError, parse_transfer_logs};
use rpc::{RpcClient, RpcError};
use std::collections::VecDeque;
use storage::{NftTokenKey, StorageError, Store};
use thiserror::Error;
use tokio::sync::watch;
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

                    warn!(
                        from_block = range_from,
                        to_block = range_to,
                        first_to_block = split_to,
                        second_from_block = split_to.saturating_add(1),
                        error = %err,
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
        self.fetch_detected_nft_token_uris(&summary.nft_tokens_to_fetch)
            .await;
        self.store.set_last_scanned_block(to_block).await?;

        info!(
            from_block,
            to_block,
            rpc_logs = logs.len(),
            parsed_transfers = transfers.len(),
            inserted_transfers = summary.inserted_transfers,
            detected_nft_tokens = summary.nft_tokens_to_fetch.len(),
            "indexed block range"
        );
        Ok(())
    }

    async fn fetch_detected_nft_token_uris(&self, tokens: &[NftTokenKey]) {
        for token in tokens {
            let pending = match self
                .store
                .nft_token_for_initial_uri_fetch(&token.token_address, &token.token_id)
                .await
            {
                Ok(Some(pending)) => pending,
                Ok(None) => continue,
                Err(err) => {
                    warn!(
                        token_address = %token.token_address,
                        token_id = %token.token_id,
                        error = %err,
                        "failed to inspect NFT token URI fetch state"
                    );
                    continue;
                }
            };

            match self
                .rpc
                .token_uri(pending.standard, &pending.token_address, &pending.token_id)
                .await
            {
                Ok(token_uri) => {
                    if let Err(err) = self
                        .store
                        .set_nft_token_uri(
                            &pending.token_address,
                            &pending.token_id,
                            pending.standard,
                            &token_uri,
                        )
                        .await
                    {
                        warn!(
                            token_address = %pending.token_address,
                            token_id = %pending.token_id,
                            error = %err,
                            "failed to persist NFT token URI"
                        );
                    }
                }
                Err(err) => {
                    let _ = self
                        .store
                        .mark_nft_token_uri_fetch_failed(
                            &pending.token_address,
                            &pending.token_id,
                            &err.to_string(),
                        )
                        .await;
                    warn!(
                        token_address = %pending.token_address,
                        token_id = %pending.token_id,
                        error = %err,
                        "failed to fetch NFT token URI"
                    );
                }
            }
        }
    }
}

fn suggested_split_to(err: &RpcError, range_from: u64, range_to: u64) -> Option<u64> {
    let (_, suggested_to) = err.suggested_log_retry_range()?;
    if (range_from..range_to).contains(&suggested_to) {
        Some(suggested_to)
    } else {
        None
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
