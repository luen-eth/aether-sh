use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use common::ApiConfig;
use rpc::{RpcClient, RpcError};
use serde::{Deserialize, Serialize};
use storage::{NftRefreshStart, StorageError, Store};
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::error;

#[derive(Clone)]
pub struct ApiState {
    store: Store,
    rpc: RpcClient,
    page_size_default: u32,
    page_size_max: u32,
}

pub fn router(store: Store, rpc: RpcClient, config: &ApiConfig) -> Router {
    let state = ApiState {
        store,
        rpc,
        page_size_default: config.page_size_default,
        page_size_max: config.page_size_max,
    };

    Router::new()
        .route("/health", get(health))
        .route("/v1/users/{address}/tokens", get(user_tokens))
        .route("/v1/users/{address}/nfts", get(user_nfts))
        .route("/v1/assets/search", get(asset_search))
        .route("/v1/assets/{token_address}/holders", get(asset_holders))
        .route(
            "/v1/nfts/{token_address}/{token_id}/refresh-metadata",
            post(refresh_nft_metadata),
        )
        .route("/v1/transfers", get(recent_transfers))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn user_tokens(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    Query(page): Query<PageQueryInput>,
) -> Result<Json<ListResponse<common::UserTokenBalance>>, ApiError> {
    let page = state.normalize_page(page);
    let data = state
        .store
        .user_token_balances(&address, page.limit, page.offset)
        .await?;
    Ok(Json(ListResponse { data }))
}

async fn user_nfts(
    State(state): State<ApiState>,
    Path(address): Path<String>,
    Query(page): Query<PageQueryInput>,
) -> Result<Json<ListResponse<common::UserNftBalance>>, ApiError> {
    let page = state.normalize_page(page);
    let data = state
        .store
        .user_nft_balances(&address, page.limit, page.offset)
        .await?;
    Ok(Json(ListResponse { data }))
}

async fn asset_search(
    State(state): State<ApiState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<ListResponse<common::AssetSearchItem>>, ApiError> {
    let limit = query
        .limit
        .unwrap_or(state.page_size_default)
        .clamp(1, state.page_size_max);
    let text = query.q.unwrap_or_default();
    let data = state.store.search_assets(&text, limit).await?;
    Ok(Json(ListResponse { data }))
}

async fn asset_holders(
    State(state): State<ApiState>,
    Path(token_address): Path<String>,
    Query(query): Query<HolderQuery>,
) -> Result<Json<ListResponse<common::HolderItem>>, ApiError> {
    let page = state.normalize_page(PageQueryInput {
        limit: query.limit,
        offset: query.offset,
    });
    let data = state
        .store
        .holders(
            &token_address,
            query.token_id.as_deref(),
            page.limit,
            page.offset,
        )
        .await?;
    Ok(Json(ListResponse { data }))
}

async fn recent_transfers(
    State(state): State<ApiState>,
    Query(query): Query<TransferQuery>,
) -> Result<Json<ListResponse<common::TransferEvent>>, ApiError> {
    let page = state.normalize_page(PageQueryInput {
        limit: query.limit,
        offset: query.offset,
    });
    let data = state
        .store
        .recent_transfers(
            query.token_address.as_deref(),
            query.account_address.as_deref(),
            page.limit,
            page.offset,
        )
        .await?;
    Ok(Json(ListResponse { data }))
}

async fn refresh_nft_metadata(
    State(state): State<ApiState>,
    Path((token_address, token_id)): Path<(String, String)>,
) -> Result<Json<NftMetadataRefreshResponse>, ApiError> {
    const COOLDOWN_SECONDS: u64 = 300;

    match state
        .store
        .start_nft_metadata_refresh(&token_address, &token_id, COOLDOWN_SECONDS)
        .await?
    {
        NftRefreshStart::RateLimited {
            retry_after_seconds,
            token_uri,
        } => Ok(Json(NftMetadataRefreshResponse {
            status: "rate_limited",
            token_address,
            token_id,
            token_uri,
            previous_token_uri: None,
            token_uri_changed: false,
            collection_base_updated: false,
            collection_tokens_updated: 0,
            retry_after_seconds: Some(retry_after_seconds),
        })),
        NftRefreshStart::Allowed {
            key,
            previous_token_uri,
        } => {
            let token_uri = state
                .rpc
                .token_uri(key.standard, &key.token_address, &key.token_id)
                .await?;
            let completed = state
                .store
                .complete_nft_metadata_refresh(
                    &key.token_address,
                    &key.token_id,
                    key.standard,
                    &token_uri,
                )
                .await?;

            Ok(Json(NftMetadataRefreshResponse {
                status: if completed.token_uri_changed {
                    "updated"
                } else {
                    "unchanged"
                },
                token_address: key.token_address,
                token_id: key.token_id,
                token_uri: Some(completed.token_uri),
                previous_token_uri: previous_token_uri.or(completed.previous_token_uri),
                token_uri_changed: completed.token_uri_changed,
                collection_base_updated: completed.collection_base_updated,
                collection_tokens_updated: completed.collection_tokens_updated,
                retry_after_seconds: None,
            }))
        }
    }
}

impl ApiState {
    fn normalize_page(&self, query: PageQueryInput) -> PageQuery {
        PageQuery {
            limit: query
                .limit
                .unwrap_or(self.page_size_default)
                .clamp(1, self.page_size_max),
            offset: query.offset.unwrap_or(0),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PageQueryInput {
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Copy, Clone)]
struct PageQuery {
    limit: u32,
    offset: u32,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct HolderQuery {
    token_id: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TransferQuery {
    token_address: Option<String>,
    account_address: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize)]
struct ListResponse<T> {
    data: Vec<T>,
}

#[derive(Debug, Serialize)]
struct NftMetadataRefreshResponse {
    status: &'static str,
    token_address: String,
    token_id: String,
    token_uri: Option<String>,
    previous_token_uri: Option<String>,
    token_uri_changed: bool,
    collection_base_updated: bool,
    collection_tokens_updated: u64,
    retry_after_seconds: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug)]
pub enum ApiError {
    Storage(StorageError),
    Rpc(RpcError),
}

impl From<StorageError> for ApiError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<RpcError> for ApiError {
    fn from(value: RpcError) -> Self {
        Self::Rpc(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            Self::Storage(err) => {
                error!(error = %err, "api storage error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "internal_server_error".to_owned(),
                    }),
                )
                    .into_response()
            }
            Self::Rpc(err) => {
                error!(error = %err, "api rpc error");
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse {
                        error: "rpc_error".to_owned(),
                    }),
                )
                    .into_response()
            }
        }
    }
}
