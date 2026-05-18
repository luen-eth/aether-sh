use common::{
    AssetSearchItem, AssetStandard, ConfigError, DatabaseConfig, HolderItem, TransferEvent,
    UserNftBalance, UserTokenBalance, ZERO_ADDRESS, normalize_address,
};
use sqlx::{FromRow, PgPool, QueryBuilder, Row, postgres::PgPoolOptions};
use std::collections::HashSet;
use thiserror::Error;
use tracing::info;
use url::Url;

const MIGRATION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS sync_state (
  id SMALLINT PRIMARY KEY CHECK (id = 1),
  last_scanned_block BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO sync_state (id, last_scanned_block)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS assets (
  address TEXT PRIMARY KEY,
  standard TEXT NOT NULL,
  name TEXT NULL,
  symbol TEXT NULL,
  decimals INTEGER NULL,
  first_seen_block BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS assets_symbol_idx ON assets (LOWER(symbol));
CREATE INDEX IF NOT EXISTS assets_name_idx ON assets (LOWER(name));

CREATE TABLE IF NOT EXISTS transfers (
  id BIGSERIAL PRIMARY KEY,
  chain_id BIGINT NOT NULL,
  block_number BIGINT NOT NULL,
  block_hash TEXT NOT NULL,
  tx_hash TEXT NOT NULL,
  log_index BIGINT NOT NULL,
  batch_index INTEGER NOT NULL DEFAULT -1,
  token_address TEXT NOT NULL REFERENCES assets(address),
  standard TEXT NOT NULL,
  from_address TEXT NOT NULL,
  to_address TEXT NOT NULL,
  token_id TEXT NULL,
  value_numeric NUMERIC(78, 0) NOT NULL,
  indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (chain_id, tx_hash, log_index, batch_index)
);
CREATE INDEX IF NOT EXISTS transfers_block_idx ON transfers (block_number);
CREATE INDEX IF NOT EXISTS transfers_token_idx ON transfers (token_address);
CREATE INDEX IF NOT EXISTS transfers_from_idx ON transfers (from_address);
CREATE INDEX IF NOT EXISTS transfers_to_idx ON transfers (to_address);

CREATE TABLE IF NOT EXISTS token_balances (
  token_address TEXT NOT NULL REFERENCES assets(address),
  holder_address TEXT NOT NULL,
  balance_numeric NUMERIC(78, 0) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (token_address, holder_address)
);
CREATE INDEX IF NOT EXISTS token_balances_holder_idx ON token_balances (holder_address);

CREATE TABLE IF NOT EXISTS nft_ownerships (
  token_address TEXT NOT NULL REFERENCES assets(address),
  token_id TEXT NOT NULL,
  owner_address TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (token_address, token_id)
);
CREATE INDEX IF NOT EXISTS nft_ownership_owner_idx ON nft_ownerships (owner_address);

CREATE TABLE IF NOT EXISTS nft_balances (
  token_address TEXT NOT NULL REFERENCES assets(address),
  token_id TEXT NOT NULL,
  holder_address TEXT NOT NULL,
  balance_numeric NUMERIC(78, 0) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (token_address, token_id, holder_address)
);
CREATE INDEX IF NOT EXISTS nft_balances_holder_idx ON nft_balances (holder_address);

CREATE TABLE IF NOT EXISTS nft_tokens (
  token_address TEXT NOT NULL REFERENCES assets(address),
  token_id TEXT NOT NULL,
  standard TEXT NOT NULL,
  token_uri TEXT NULL,
  uri_status TEXT NOT NULL DEFAULT 'pending',
  last_uri_fetch_error TEXT NULL,
  last_uri_fetch_at TIMESTAMPTZ NULL,
  last_refresh_requested_at TIMESTAMPTZ NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (token_address, token_id)
);
CREATE INDEX IF NOT EXISTS nft_tokens_collection_idx ON nft_tokens (token_address);
CREATE INDEX IF NOT EXISTS nft_tokens_uri_status_idx ON nft_tokens (uri_status);

CREATE TABLE IF NOT EXISTS nft_collection_metadata (
  token_address TEXT PRIMARY KEY REFERENCES assets(address),
  base_uri TEXT NULL,
  last_base_uri_update_at TIMESTAMPTZ NULL,
  updated_from_token_id TEXT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"#;

#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NftTokenKey {
    pub token_address: String,
    pub token_id: String,
    pub standard: AssetStandard,
}

#[derive(Debug, Clone)]
pub struct IngestSummary {
    pub inserted_transfers: usize,
    pub nft_tokens_to_fetch: Vec<NftTokenKey>,
}

#[derive(Debug, Clone)]
pub enum NftRefreshStart {
    Allowed {
        key: NftTokenKey,
        previous_token_uri: Option<String>,
    },
    RateLimited {
        retry_after_seconds: u64,
        token_uri: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub struct NftRefreshComplete {
    pub previous_token_uri: Option<String>,
    pub token_uri: String,
    pub token_uri_changed: bool,
    pub collection_base_updated: bool,
    pub collection_tokens_updated: u64,
}

impl Store {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, StorageError> {
        let pool = match connect_pool(config).await {
            Ok(pool) => pool,
            Err(err) if is_missing_database_error(&err) => {
                create_database(&config.url).await?;
                connect_pool(config).await.map_err(StorageError::Connect)?
            }
            Err(err) => return Err(StorageError::Connect(err)),
        };

        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<(), StorageError> {
        for statement in MIGRATION_SQL
            .split(';')
            .map(str::trim)
            .filter(|stmt| !stmt.is_empty())
        {
            sqlx::query(statement)
                .execute(&self.pool)
                .await
                .map_err(StorageError::Migrate)?;
        }
        Ok(())
    }

    pub async fn last_scanned_block(&self) -> Result<u64, StorageError> {
        let row = sqlx::query("SELECT last_scanned_block FROM sync_state WHERE id = 1")
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::Query)?;
        let value: i64 = row.get("last_scanned_block");
        Ok(value.max(0) as u64)
    }

    pub async fn set_last_scanned_block(&self, block_number: u64) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO sync_state (id, last_scanned_block, updated_at)
            VALUES (1, $1, NOW())
            ON CONFLICT (id)
            DO UPDATE SET last_scanned_block = EXCLUDED.last_scanned_block, updated_at = NOW()
            "#,
        )
        .bind(block_number as i64)
        .execute(&self.pool)
        .await
        .map_err(StorageError::Query)?;
        Ok(())
    }

    pub async fn ingest_transfers(
        &self,
        transfers: &[TransferEvent],
    ) -> Result<IngestSummary, StorageError> {
        if transfers.is_empty() {
            return Ok(IngestSummary {
                inserted_transfers: 0,
                nft_tokens_to_fetch: Vec::new(),
            });
        }

        let mut tx = self.pool.begin().await.map_err(StorageError::Query)?;
        let mut inserted = 0usize;
        let mut nft_tokens_to_fetch = Vec::new();
        let mut seen_nft_tokens = HashSet::new();

        for transfer in transfers {
            self.upsert_asset(&mut tx, transfer).await?;
            let changed = self.insert_transfer(&mut tx, transfer).await?;
            if changed == 0 {
                continue;
            }
            inserted += 1;
            self.apply_balance(&mut tx, transfer).await?;
            if let Some(key) = self.ensure_nft_token(&mut tx, transfer).await? {
                let dedupe_key = (key.token_address.clone(), key.token_id.clone());
                if seen_nft_tokens.insert(dedupe_key) {
                    nft_tokens_to_fetch.push(key);
                }
            }
        }

        tx.commit().await.map_err(StorageError::Query)?;
        Ok(IngestSummary {
            inserted_transfers: inserted,
            nft_tokens_to_fetch,
        })
    }

    pub async fn user_token_balances(
        &self,
        owner_address: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<UserTokenBalance>, StorageError> {
        let owner = normalize_address(owner_address);
        let rows: Vec<TokenBalanceRow> = sqlx::query_as(
            r#"
            SELECT
              tb.token_address,
              a.name,
              a.symbol,
              a.decimals,
              tb.balance_numeric::TEXT AS balance
            FROM token_balances tb
            JOIN assets a ON a.address = tb.token_address
            WHERE tb.holder_address = $1
              AND a.standard = 'erc20'
              AND tb.balance_numeric > 0
            ORDER BY tb.balance_numeric DESC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(owner)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        Ok(rows
            .into_iter()
            .map(|row| UserTokenBalance {
                token_address: row.token_address,
                name: row.name,
                symbol: row.symbol,
                decimals: row.decimals,
                balance: row.balance,
            })
            .collect())
    }

    pub async fn user_nft_balances(
        &self,
        owner_address: &str,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<UserNftBalance>, StorageError> {
        let owner = normalize_address(owner_address);
        let rows: Vec<UserNftRow> = sqlx::query_as(
            r#"
            SELECT
              o.token_address,
              'erc721'::TEXT AS standard,
              o.token_id,
              nt.token_uri,
              '1'::TEXT AS balance
            FROM nft_ownerships o
            LEFT JOIN nft_tokens nt ON nt.token_address = o.token_address AND nt.token_id = o.token_id
            WHERE o.owner_address = $1
            UNION ALL
            SELECT
              b.token_address,
              'erc1155'::TEXT AS standard,
              b.token_id,
              nt.token_uri,
              b.balance_numeric::TEXT AS balance
            FROM nft_balances b
            LEFT JOIN nft_tokens nt ON nt.token_address = b.token_address AND nt.token_id = b.token_id
            WHERE b.holder_address = $1
              AND b.balance_numeric > 0
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(owner)
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        rows.into_iter()
            .map(|row| {
                Ok(UserNftBalance {
                    token_address: row.token_address,
                    standard: parse_standard(&row.standard)?,
                    token_id: row.token_id,
                    token_uri: row.token_uri,
                    balance: row.balance,
                })
            })
            .collect()
    }

    pub async fn search_assets(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<Vec<AssetSearchItem>, StorageError> {
        let q = query.trim();
        let pattern = format!("%{}%", q.to_ascii_lowercase());
        let rows: Vec<AssetRow> = sqlx::query_as(
            r#"
            SELECT address, standard, name, symbol, decimals
            FROM assets
            WHERE
              ($1 = '')
              OR LOWER(address) LIKE $2
              OR LOWER(COALESCE(symbol, '')) LIKE $2
              OR LOWER(COALESCE(name, '')) LIKE $2
            ORDER BY updated_at DESC
            LIMIT $3
            "#,
        )
        .bind(q)
        .bind(pattern)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        rows.into_iter()
            .map(|row| {
                Ok(AssetSearchItem {
                    token_address: row.address,
                    standard: parse_standard(&row.standard)?,
                    name: row.name,
                    symbol: row.symbol,
                    decimals: row.decimals,
                })
            })
            .collect()
    }

    pub async fn nft_token_for_initial_uri_fetch(
        &self,
        token_address: &str,
        token_id: &str,
    ) -> Result<Option<NftTokenKey>, StorageError> {
        let token = normalize_address(token_address);
        let row = sqlx::query(
            r#"
            SELECT token_address, token_id, standard
            FROM nft_tokens
            WHERE token_address = $1
              AND token_id = $2
              AND token_uri IS NULL
              AND uri_status = 'pending'
            "#,
        )
        .bind(token)
        .bind(token_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        row.map(|row| {
            Ok(NftTokenKey {
                token_address: row.get("token_address"),
                token_id: row.get("token_id"),
                standard: parse_standard(row.get::<String, _>("standard").as_str())?,
            })
        })
        .transpose()
    }

    pub async fn set_nft_token_uri(
        &self,
        token_address: &str,
        token_id: &str,
        standard: AssetStandard,
        token_uri: &str,
    ) -> Result<(), StorageError> {
        let token = normalize_address(token_address);
        sqlx::query(
            r#"
            INSERT INTO nft_tokens (
              token_address, token_id, standard, token_uri, uri_status,
              last_uri_fetch_error, last_uri_fetch_at, updated_at
            )
            VALUES ($1, $2, $3, $4, 'fetched', NULL, NOW(), NOW())
            ON CONFLICT (token_address, token_id)
            DO UPDATE SET
              standard = EXCLUDED.standard,
              token_uri = EXCLUDED.token_uri,
              uri_status = 'fetched',
              last_uri_fetch_error = NULL,
              last_uri_fetch_at = NOW(),
              updated_at = NOW()
            "#,
        )
        .bind(token)
        .bind(token_id)
        .bind(standard.as_str())
        .bind(token_uri)
        .execute(&self.pool)
        .await
        .map_err(StorageError::Query)?;
        Ok(())
    }

    pub async fn mark_nft_token_uri_fetch_failed(
        &self,
        token_address: &str,
        token_id: &str,
        error: &str,
    ) -> Result<(), StorageError> {
        let token = normalize_address(token_address);
        sqlx::query(
            r#"
            UPDATE nft_tokens
            SET uri_status = 'failed',
                last_uri_fetch_error = $3,
                last_uri_fetch_at = NOW(),
                updated_at = NOW()
            WHERE token_address = $1 AND token_id = $2
            "#,
        )
        .bind(token)
        .bind(token_id)
        .bind(error.chars().take(1_000).collect::<String>())
        .execute(&self.pool)
        .await
        .map_err(StorageError::Query)?;
        Ok(())
    }

    pub async fn start_nft_metadata_refresh(
        &self,
        token_address: &str,
        token_id: &str,
        cooldown_seconds: u64,
    ) -> Result<NftRefreshStart, StorageError> {
        let token = normalize_address(token_address);
        let standard = self.asset_standard(&token).await?;
        if !matches!(standard, AssetStandard::Erc721 | AssetStandard::Erc1155) {
            return Err(StorageError::Data(format!(
                "asset {token} is not an NFT collection"
            )));
        }

        sqlx::query(
            r#"
            INSERT INTO nft_tokens (token_address, token_id, standard, uri_status, updated_at)
            VALUES ($1, $2, $3, 'pending', NOW())
            ON CONFLICT (token_address, token_id) DO NOTHING
            "#,
        )
        .bind(&token)
        .bind(token_id)
        .bind(standard.as_str())
        .execute(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        let cooldown = cooldown_seconds as i32;
        let row = sqlx::query(
            r#"
            UPDATE nft_tokens
            SET last_refresh_requested_at = NOW(), updated_at = NOW()
            WHERE token_address = $1
              AND token_id = $2
              AND (
                last_refresh_requested_at IS NULL
                OR last_refresh_requested_at <= NOW() - ($3::INT * INTERVAL '1 second')
              )
            RETURNING token_uri
            "#,
        )
        .bind(&token)
        .bind(token_id)
        .bind(cooldown)
        .fetch_optional(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        if let Some(row) = row {
            return Ok(NftRefreshStart::Allowed {
                key: NftTokenKey {
                    token_address: token,
                    token_id: token_id.to_owned(),
                    standard,
                },
                previous_token_uri: row.get("token_uri"),
            });
        }

        let row = sqlx::query(
            r#"
            SELECT
              token_uri,
              CEIL(GREATEST(
                0,
                EXTRACT(EPOCH FROM (
                  last_refresh_requested_at + ($3::INT * INTERVAL '1 second') - NOW()
                ))
              ))::BIGINT AS retry_after_seconds
            FROM nft_tokens
            WHERE token_address = $1 AND token_id = $2
            "#,
        )
        .bind(&token)
        .bind(token_id)
        .bind(cooldown)
        .fetch_one(&self.pool)
        .await
        .map_err(StorageError::Query)?;

        Ok(NftRefreshStart::RateLimited {
            retry_after_seconds: row.get::<i64, _>("retry_after_seconds").max(0) as u64,
            token_uri: row.get("token_uri"),
        })
    }

    pub async fn complete_nft_metadata_refresh(
        &self,
        token_address: &str,
        token_id: &str,
        standard: AssetStandard,
        token_uri: &str,
    ) -> Result<NftRefreshComplete, StorageError> {
        let token = normalize_address(token_address);
        let mut tx = self.pool.begin().await.map_err(StorageError::Query)?;

        let row = sqlx::query(
            r#"
            SELECT token_uri
            FROM nft_tokens
            WHERE token_address = $1 AND token_id = $2
            FOR UPDATE
            "#,
        )
        .bind(&token)
        .bind(token_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(StorageError::Query)?;

        let previous_token_uri = row.and_then(|row| row.get("token_uri"));

        sqlx::query(
            r#"
            INSERT INTO nft_tokens (
              token_address, token_id, standard, token_uri, uri_status,
              last_uri_fetch_error, last_uri_fetch_at, updated_at
            )
            VALUES ($1, $2, $3, $4, 'fetched', NULL, NOW(), NOW())
            ON CONFLICT (token_address, token_id)
            DO UPDATE SET
              standard = EXCLUDED.standard,
              token_uri = EXCLUDED.token_uri,
              uri_status = 'fetched',
              last_uri_fetch_error = NULL,
              last_uri_fetch_at = NOW(),
              updated_at = NOW()
            "#,
        )
        .bind(&token)
        .bind(token_id)
        .bind(standard.as_str())
        .bind(token_uri)
        .execute(&mut *tx)
        .await
        .map_err(StorageError::Query)?;

        let mut collection_base_updated = false;
        let mut collection_tokens_updated = 0;
        let token_uri_changed = previous_token_uri.as_deref() != Some(token_uri);

        if let Some(previous) = previous_token_uri.as_deref()
            && token_uri_changed
            && let Some(base_update) = infer_base_uri_update(previous, token_uri, token_id)
        {
            let rows = sqlx::query(
                r#"
                UPDATE nft_tokens
                SET token_uri = $3 || SUBSTRING(token_uri FROM $4),
                    uri_status = 'fetched',
                    last_uri_fetch_error = NULL,
                    last_uri_fetch_at = NOW(),
                    updated_at = NOW()
                WHERE token_address = $1
                  AND token_uri IS NOT NULL
                  AND LEFT(token_uri, $2) = $5
                "#,
            )
            .bind(&token)
            .bind(base_update.old_base_uri.len() as i32)
            .bind(&base_update.new_base_uri)
            .bind((base_update.old_base_uri.len() + 1) as i32)
            .bind(&base_update.old_base_uri)
            .execute(&mut *tx)
            .await
            .map_err(StorageError::Query)?
            .rows_affected();

            sqlx::query(
                r#"
                INSERT INTO nft_collection_metadata (
                  token_address, base_uri, last_base_uri_update_at,
                  updated_from_token_id, updated_at
                )
                VALUES ($1, $2, NOW(), $3, NOW())
                ON CONFLICT (token_address)
                DO UPDATE SET
                  base_uri = EXCLUDED.base_uri,
                  last_base_uri_update_at = NOW(),
                  updated_from_token_id = EXCLUDED.updated_from_token_id,
                  updated_at = NOW()
                "#,
            )
            .bind(&token)
            .bind(&base_update.new_base_uri)
            .bind(token_id)
            .execute(&mut *tx)
            .await
            .map_err(StorageError::Query)?;

            collection_base_updated =
                rows > 0 || base_update.old_base_uri != base_update.new_base_uri;
            collection_tokens_updated = rows;
        }

        tx.commit().await.map_err(StorageError::Query)?;

        Ok(NftRefreshComplete {
            previous_token_uri,
            token_uri: token_uri.to_owned(),
            token_uri_changed,
            collection_base_updated,
            collection_tokens_updated,
        })
    }

    pub async fn holders(
        &self,
        token_address: &str,
        token_id: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<HolderItem>, StorageError> {
        let token = normalize_address(token_address);
        let standard = self.asset_standard(&token).await?;

        match standard {
            AssetStandard::Erc20 => {
                let rows: Vec<HolderRow> = sqlx::query_as(
                    r#"
                    SELECT holder_address, balance_numeric::TEXT AS amount
                    FROM token_balances
                    WHERE token_address = $1
                      AND balance_numeric > 0
                    ORDER BY balance_numeric DESC
                    LIMIT $2 OFFSET $3
                    "#,
                )
                .bind(token)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(&self.pool)
                .await
                .map_err(StorageError::Query)?;
                Ok(rows
                    .into_iter()
                    .map(|row| HolderItem {
                        holder_address: row.holder_address,
                        amount: row.amount,
                    })
                    .collect())
            }
            AssetStandard::Erc721 => {
                let rows: Vec<HolderRow> = if let Some(id) = token_id {
                    sqlx::query_as(
                        r#"
                        SELECT owner_address AS holder_address, '1'::TEXT AS amount
                        FROM nft_ownerships
                        WHERE token_address = $1
                          AND token_id = $2
                        LIMIT $3 OFFSET $4
                        "#,
                    )
                    .bind(token)
                    .bind(id)
                    .bind(limit as i64)
                    .bind(offset as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(StorageError::Query)?
                } else {
                    sqlx::query_as(
                        r#"
                        SELECT owner_address AS holder_address, COUNT(*)::TEXT AS amount
                        FROM nft_ownerships
                        WHERE token_address = $1
                        GROUP BY owner_address
                        ORDER BY COUNT(*) DESC
                        LIMIT $2 OFFSET $3
                        "#,
                    )
                    .bind(token)
                    .bind(limit as i64)
                    .bind(offset as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(StorageError::Query)?
                };

                Ok(rows
                    .into_iter()
                    .map(|row| HolderItem {
                        holder_address: row.holder_address,
                        amount: row.amount,
                    })
                    .collect())
            }
            AssetStandard::Erc1155 => {
                let rows: Vec<HolderRow> = if let Some(id) = token_id {
                    sqlx::query_as(
                        r#"
                        SELECT holder_address, balance_numeric::TEXT AS amount
                        FROM nft_balances
                        WHERE token_address = $1
                          AND token_id = $2
                          AND balance_numeric > 0
                        ORDER BY balance_numeric DESC
                        LIMIT $3 OFFSET $4
                        "#,
                    )
                    .bind(token)
                    .bind(id)
                    .bind(limit as i64)
                    .bind(offset as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(StorageError::Query)?
                } else {
                    sqlx::query_as(
                        r#"
                        SELECT holder_address, SUM(balance_numeric)::TEXT AS amount
                        FROM nft_balances
                        WHERE token_address = $1
                          AND balance_numeric > 0
                        GROUP BY holder_address
                        ORDER BY SUM(balance_numeric) DESC
                        LIMIT $2 OFFSET $3
                        "#,
                    )
                    .bind(token)
                    .bind(limit as i64)
                    .bind(offset as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(StorageError::Query)?
                };

                Ok(rows
                    .into_iter()
                    .map(|row| HolderItem {
                        holder_address: row.holder_address,
                        amount: row.amount,
                    })
                    .collect())
            }
        }
    }

    pub async fn recent_transfers(
        &self,
        token_address: Option<&str>,
        account_address: Option<&str>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<TransferEvent>, StorageError> {
        let mut builder = QueryBuilder::new(
            r#"
            SELECT
              chain_id,
              block_number,
              block_hash,
              tx_hash,
              log_index,
              token_address,
              standard,
              from_address,
              to_address,
              token_id,
              value_numeric::TEXT AS value,
              batch_index,
              indexed_at
            FROM transfers
            WHERE TRUE
            "#,
        );

        if let Some(token) = token_address {
            builder.push(" AND token_address = ");
            builder.push_bind(normalize_address(token));
        }

        if let Some(account) = account_address {
            let normalized = normalize_address(account);
            builder.push(" AND (from_address = ");
            builder.push_bind(normalized.clone());
            builder.push(" OR to_address = ");
            builder.push_bind(normalized);
            builder.push(")");
        }

        builder.push(" ORDER BY block_number DESC, log_index DESC, batch_index DESC");
        builder.push(" LIMIT ");
        builder.push_bind(limit as i64);
        builder.push(" OFFSET ");
        builder.push_bind(offset as i64);

        let rows = builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(StorageError::Query)?;

        rows.into_iter()
            .map(|row| {
                let standard_text: String = row.get("standard");
                let standard = parse_standard(&standard_text)?;
                let batch_index: i32 = row.get("batch_index");
                Ok(TransferEvent {
                    chain_id: row.get::<i64, _>("chain_id") as u64,
                    block_number: row.get::<i64, _>("block_number") as u64,
                    block_hash: row.get("block_hash"),
                    tx_hash: row.get("tx_hash"),
                    log_index: row.get::<i64, _>("log_index") as u64,
                    token_address: row.get("token_address"),
                    standard,
                    from_address: row.get("from_address"),
                    to_address: row.get("to_address"),
                    token_id: row.get("token_id"),
                    value: row.get("value"),
                    batch_index: if batch_index < 0 {
                        None
                    } else {
                        Some(batch_index as u32)
                    },
                    indexed_at: row.get("indexed_at"),
                })
            })
            .collect()
    }

    async fn asset_standard(&self, token_address: &str) -> Result<AssetStandard, StorageError> {
        let standard: String = sqlx::query_scalar("SELECT standard FROM assets WHERE address = $1")
            .bind(token_address)
            .fetch_one(&self.pool)
            .await
            .map_err(StorageError::Query)?;
        parse_standard(&standard)
    }

    async fn upsert_asset(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO assets (address, standard, first_seen_block, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (address)
            DO UPDATE SET standard = EXCLUDED.standard, updated_at = NOW()
            "#,
        )
        .bind(&transfer.token_address)
        .bind(transfer.standard.as_str())
        .bind(transfer.block_number as i64)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;
        Ok(())
    }

    async fn insert_transfer(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<u64, StorageError> {
        let batch_index = transfer.batch_index.map(|v| v as i32).unwrap_or(-1);
        let rows = sqlx::query(
            r#"
            INSERT INTO transfers (
                chain_id, block_number, block_hash, tx_hash, log_index, batch_index, token_address,
                standard, from_address, to_address, token_id, value_numeric, indexed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CAST($12 AS NUMERIC(78,0)), $13)
            ON CONFLICT (chain_id, tx_hash, log_index, batch_index) DO NOTHING
            "#,
        )
        .bind(transfer.chain_id as i64)
        .bind(transfer.block_number as i64)
        .bind(&transfer.block_hash)
        .bind(&transfer.tx_hash)
        .bind(transfer.log_index as i64)
        .bind(batch_index)
        .bind(&transfer.token_address)
        .bind(transfer.standard.as_str())
        .bind(&transfer.from_address)
        .bind(&transfer.to_address)
        .bind(transfer.token_id.as_deref())
        .bind(&transfer.value)
        .bind(transfer.indexed_at)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?
        .rows_affected();
        Ok(rows)
    }

    async fn apply_balance(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<(), StorageError> {
        match transfer.standard {
            AssetStandard::Erc20 => self.apply_erc20(tx, transfer).await,
            AssetStandard::Erc721 => self.apply_erc721(tx, transfer).await,
            AssetStandard::Erc1155 => self.apply_erc1155(tx, transfer).await,
        }
    }

    async fn apply_erc20(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<(), StorageError> {
        if transfer.from_address != ZERO_ADDRESS {
            self.debit_token_balance(
                tx,
                &transfer.token_address,
                &transfer.from_address,
                &transfer.value,
            )
            .await?;
        }
        if transfer.to_address != ZERO_ADDRESS {
            self.credit_token_balance(
                tx,
                &transfer.token_address,
                &transfer.to_address,
                &transfer.value,
            )
            .await?;
        }
        Ok(())
    }

    async fn apply_erc721(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<(), StorageError> {
        let token_id = transfer
            .token_id
            .as_ref()
            .ok_or_else(|| StorageError::Data("erc721 transfer missing token_id".to_owned()))?;

        if transfer.to_address == ZERO_ADDRESS {
            sqlx::query("DELETE FROM nft_ownerships WHERE token_address = $1 AND token_id = $2")
                .bind(&transfer.token_address)
                .bind(token_id)
                .execute(&mut **tx)
                .await
                .map_err(StorageError::Query)?;
        } else {
            sqlx::query(
                r#"
                INSERT INTO nft_ownerships (token_address, token_id, owner_address, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (token_address, token_id)
                DO UPDATE SET owner_address = EXCLUDED.owner_address, updated_at = NOW()
                "#,
            )
            .bind(&transfer.token_address)
            .bind(token_id)
            .bind(&transfer.to_address)
            .execute(&mut **tx)
            .await
            .map_err(StorageError::Query)?;
        }
        Ok(())
    }

    async fn apply_erc1155(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<(), StorageError> {
        let token_id = transfer
            .token_id
            .as_ref()
            .ok_or_else(|| StorageError::Data("erc1155 transfer missing token_id".to_owned()))?;

        if transfer.from_address != ZERO_ADDRESS {
            self.debit_nft_balance(
                tx,
                &transfer.token_address,
                token_id,
                &transfer.from_address,
                &transfer.value,
            )
            .await?;
        }

        if transfer.to_address != ZERO_ADDRESS {
            self.credit_nft_balance(
                tx,
                &transfer.token_address,
                token_id,
                &transfer.to_address,
                &transfer.value,
            )
            .await?;
        }
        Ok(())
    }

    async fn ensure_nft_token(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        transfer: &TransferEvent,
    ) -> Result<Option<NftTokenKey>, StorageError> {
        if !matches!(
            transfer.standard,
            AssetStandard::Erc721 | AssetStandard::Erc1155
        ) {
            return Ok(None);
        }
        let Some(token_id) = transfer.token_id.as_ref() else {
            return Ok(None);
        };

        sqlx::query(
            r#"
            INSERT INTO nft_tokens (token_address, token_id, standard, uri_status, updated_at)
            VALUES ($1, $2, $3, 'pending', NOW())
            ON CONFLICT (token_address, token_id)
            DO UPDATE SET standard = EXCLUDED.standard, updated_at = NOW()
            "#,
        )
        .bind(&transfer.token_address)
        .bind(token_id)
        .bind(transfer.standard.as_str())
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        Ok(Some(NftTokenKey {
            token_address: transfer.token_address.clone(),
            token_id: token_id.clone(),
            standard: transfer.standard,
        }))
    }

    async fn credit_token_balance(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        token_address: &str,
        holder_address: &str,
        amount: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO token_balances (token_address, holder_address, balance_numeric, updated_at)
            VALUES ($1, $2, CAST($3 AS NUMERIC(78,0)), NOW())
            ON CONFLICT (token_address, holder_address)
            DO UPDATE SET
              balance_numeric = token_balances.balance_numeric + EXCLUDED.balance_numeric,
              updated_at = NOW()
            "#,
        )
        .bind(token_address)
        .bind(holder_address)
        .bind(amount)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;
        Ok(())
    }

    async fn debit_token_balance(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        token_address: &str,
        holder_address: &str,
        amount: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO token_balances (token_address, holder_address, balance_numeric, updated_at)
            VALUES ($1, $2, 0, NOW())
            ON CONFLICT (token_address, holder_address) DO NOTHING
            "#,
        )
        .bind(token_address)
        .bind(holder_address)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        sqlx::query(
            r#"
            UPDATE token_balances
            SET balance_numeric = GREATEST(balance_numeric - CAST($3 AS NUMERIC(78,0)), 0), updated_at = NOW()
            WHERE token_address = $1 AND holder_address = $2
            "#,
        )
        .bind(token_address)
        .bind(holder_address)
        .bind(amount)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        sqlx::query(
            "DELETE FROM token_balances WHERE token_address = $1 AND holder_address = $2 AND balance_numeric = 0",
        )
        .bind(token_address)
        .bind(holder_address)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        Ok(())
    }

    async fn credit_nft_balance(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        token_address: &str,
        token_id: &str,
        holder_address: &str,
        amount: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO nft_balances (token_address, token_id, holder_address, balance_numeric, updated_at)
            VALUES ($1, $2, $3, CAST($4 AS NUMERIC(78,0)), NOW())
            ON CONFLICT (token_address, token_id, holder_address)
            DO UPDATE SET
              balance_numeric = nft_balances.balance_numeric + EXCLUDED.balance_numeric,
              updated_at = NOW()
            "#,
        )
        .bind(token_address)
        .bind(token_id)
        .bind(holder_address)
        .bind(amount)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;
        Ok(())
    }

    async fn debit_nft_balance(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        token_address: &str,
        token_id: &str,
        holder_address: &str,
        amount: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO nft_balances (token_address, token_id, holder_address, balance_numeric, updated_at)
            VALUES ($1, $2, $3, 0, NOW())
            ON CONFLICT (token_address, token_id, holder_address) DO NOTHING
            "#,
        )
        .bind(token_address)
        .bind(token_id)
        .bind(holder_address)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        sqlx::query(
            r#"
            UPDATE nft_balances
            SET balance_numeric = GREATEST(balance_numeric - CAST($4 AS NUMERIC(78,0)), 0), updated_at = NOW()
            WHERE token_address = $1 AND token_id = $2 AND holder_address = $3
            "#,
        )
        .bind(token_address)
        .bind(token_id)
        .bind(holder_address)
        .bind(amount)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        sqlx::query(
            "DELETE FROM nft_balances WHERE token_address = $1 AND token_id = $2 AND holder_address = $3 AND balance_numeric = 0",
        )
        .bind(token_address)
        .bind(token_id)
        .bind(holder_address)
        .execute(&mut **tx)
        .await
        .map_err(StorageError::Query)?;

        Ok(())
    }
}

async fn connect_pool(config: &DatabaseConfig) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(config.max_connections)
        .connect(&config.url)
        .await
}

async fn create_database(database_url: &str) -> Result<(), StorageError> {
    let database_name = database_name_from_url(database_url)?;
    let admin_url = maintenance_database_url(database_url)?;
    let admin_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin_url)
        .await
        .map_err(StorageError::AdminConnect)?;

    let statement = format!("CREATE DATABASE {}", quote_identifier(&database_name));
    match sqlx::query(&statement).execute(&admin_pool).await {
        Ok(_) => {
            info!(database = %database_name, "created missing postgres database");
            Ok(())
        }
        Err(err) if is_duplicate_database_error(&err) => Ok(()),
        Err(err) => Err(StorageError::CreateDatabase(err)),
    }
}

fn maintenance_database_url(database_url: &str) -> Result<String, StorageError> {
    let mut parsed = Url::parse(database_url).map_err(StorageError::DatabaseUrl)?;
    parsed.set_path("/postgres");
    Ok(parsed.to_string())
}

fn database_name_from_url(database_url: &str) -> Result<String, StorageError> {
    let parsed = Url::parse(database_url).map_err(StorageError::DatabaseUrl)?;
    let database_name = parsed
        .path_segments()
        .and_then(|mut segments| segments.next())
        .unwrap_or_default()
        .trim();

    if database_name.is_empty() || database_name.contains('\0') {
        return Err(StorageError::InvalidDatabaseName(database_name.to_owned()));
    }

    Ok(database_name.to_owned())
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn is_missing_database_error(err: &sqlx::Error) -> bool {
    database_error_code(err).as_deref() == Some("3D000")
}

fn is_duplicate_database_error(err: &sqlx::Error) -> bool {
    database_error_code(err).as_deref() == Some("42P04")
}

fn database_error_code(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(database_error) => {
            database_error.code().map(|code| code.into_owned())
        }
        _ => None,
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database connect failed: {0}")]
    Connect(sqlx::Error),
    #[error("database admin connect failed: {0}")]
    AdminConnect(sqlx::Error),
    #[error("database create failed: {0}")]
    CreateDatabase(sqlx::Error),
    #[error("invalid database url: {0}")]
    DatabaseUrl(url::ParseError),
    #[error("invalid database name: {0}")]
    InvalidDatabaseName(String),
    #[error("database migration failed: {0}")]
    Migrate(sqlx::Error),
    #[error("database query failed: {0}")]
    Query(sqlx::Error),
    #[error("invalid data: {0}")]
    Data(String),
    #[error("parse error: {0}")]
    Parse(#[from] ConfigError),
}

fn parse_standard(value: &str) -> Result<AssetStandard, StorageError> {
    match value {
        "erc20" => Ok(AssetStandard::Erc20),
        "erc721" => Ok(AssetStandard::Erc721),
        "erc1155" => Ok(AssetStandard::Erc1155),
        _ => Err(StorageError::Data(format!(
            "unknown asset standard: {value}"
        ))),
    }
}

#[derive(Debug, FromRow)]
struct TokenBalanceRow {
    token_address: String,
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<i32>,
    balance: String,
}

#[derive(Debug, FromRow)]
struct UserNftRow {
    token_address: String,
    standard: String,
    token_id: String,
    token_uri: Option<String>,
    balance: String,
}

#[derive(Debug, FromRow)]
struct AssetRow {
    address: String,
    standard: String,
    name: Option<String>,
    symbol: Option<String>,
    decimals: Option<i32>,
}

#[derive(Debug, FromRow)]
struct HolderRow {
    holder_address: String,
    amount: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BaseUriUpdate {
    old_base_uri: String,
    new_base_uri: String,
}

fn infer_base_uri_update(
    previous_uri: &str,
    new_uri: &str,
    token_id: &str,
) -> Option<BaseUriUpdate> {
    if previous_uri == new_uri {
        return None;
    }

    let markers = [
        token_id.to_owned(),
        "{id}".to_owned(),
        decimal_to_erc1155_hex_id(token_id),
    ];

    for marker in markers.iter().filter(|marker| !marker.is_empty()) {
        let Some(old_index) = previous_uri.rfind(marker) else {
            continue;
        };
        let Some(new_index) = new_uri.rfind(marker) else {
            continue;
        };

        let old_suffix = &previous_uri[old_index..];
        let new_suffix = &new_uri[new_index..];
        if old_suffix == new_suffix {
            let old_base_uri = previous_uri[..old_index].to_owned();
            let new_base_uri = new_uri[..new_index].to_owned();
            if !old_base_uri.is_empty() && old_base_uri != new_base_uri {
                return Some(BaseUriUpdate {
                    old_base_uri,
                    new_base_uri,
                });
            }
        }
    }

    let suffix_len = common_suffix_len(previous_uri, new_uri);
    if suffix_len < 8 || suffix_len >= previous_uri.len() || suffix_len >= new_uri.len() {
        return None;
    }

    let old_split = previous_uri.len() - suffix_len;
    let new_split = new_uri.len() - suffix_len;
    if !previous_uri.is_char_boundary(old_split) || !new_uri.is_char_boundary(new_split) {
        return None;
    }

    let old_base_uri = previous_uri[..old_split].to_owned();
    let new_base_uri = new_uri[..new_split].to_owned();
    if old_base_uri.is_empty() || old_base_uri == new_base_uri {
        return None;
    }

    Some(BaseUriUpdate {
        old_base_uri,
        new_base_uri,
    })
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

fn common_suffix_len(left: &str, right: &str) -> usize {
    left.as_bytes()
        .iter()
        .rev()
        .zip(right.as_bytes().iter().rev())
        .take_while(|(a, b)| a == b)
        .count()
}

#[cfg(test)]
mod tests {
    use super::{
        database_name_from_url, decimal_to_erc1155_hex_id, infer_base_uri_update,
        maintenance_database_url, quote_identifier,
    };

    #[test]
    fn infers_base_uri_from_token_id_suffix() {
        let update = infer_base_uri_update(
            "ipfs://old-base/123.json",
            "ipfs://new-base/123.json",
            "123",
        )
        .unwrap();
        assert_eq!(update.old_base_uri, "ipfs://old-base/");
        assert_eq!(update.new_base_uri, "ipfs://new-base/");
    }

    #[test]
    fn infers_base_uri_from_erc1155_placeholder() {
        let update =
            infer_base_uri_update("https://old/{id}.json", "https://new/{id}.json", "1").unwrap();
        assert_eq!(update.old_base_uri, "https://old/");
        assert_eq!(update.new_base_uri, "https://new/");
    }

    #[test]
    fn encodes_erc1155_hex_id() {
        assert_eq!(
            decimal_to_erc1155_hex_id("15"),
            "000000000000000000000000000000000000000000000000000000000000000f"
        );
    }

    #[test]
    fn builds_maintenance_database_url() {
        assert_eq!(
            maintenance_database_url(
                "postgres://postgres:postgres@postgres:5432/aether_indexer?sslmode=disable"
            )
            .unwrap(),
            "postgres://postgres:postgres@postgres:5432/postgres?sslmode=disable"
        );
    }

    #[test]
    fn extracts_database_name_from_url() {
        assert_eq!(
            database_name_from_url("postgres://user:pass@localhost:5432/aether_indexer").unwrap(),
            "aether_indexer"
        );
    }

    #[test]
    fn quotes_database_identifiers() {
        assert_eq!(quote_identifier("aether_indexer"), "\"aether_indexer\"");
        assert_eq!(quote_identifier("aether\"idx"), "\"aether\"\"idx\"");
    }
}
