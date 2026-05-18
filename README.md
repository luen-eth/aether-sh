# aether-indexer

`aether-indexer` is a Rust-based EVM indexer and HTTP API for token and NFT data.

It scans chain logs from the configured start block, parses ERC-20, ERC-721, and ERC-1155 transfer events, stores normalized transfer history in PostgreSQL, and maintains query-ready balance and holder projections.

## Features

- EVM JSON-RPC indexing through `eth_blockNumber` and `eth_getLogs`
- ERC-20, ERC-721, and ERC-1155 transfer parsing
- Catch-up indexing from block `0` or any configured start block
- Finality-aware polling with configurable confirmations
- Up to three HTTP RPC endpoints with automatic failover
- PostgreSQL-backed transfer, balance, ownership, and holder tables
- NFT token URI persistence for detected ERC-721 and ERC-1155 tokens
- Manual NFT metadata refresh with a five-minute per-token rate limit
- Collection base URI propagation when a refreshed token URI changes
- Docker Compose setup with bundled PostgreSQL
- Optional external PostgreSQL connection through `.env`
- Axum-based HTTP API

## Project Layout

- `crates/common`: shared configuration, domain types, and helpers
- `crates/rpc`: JSON-RPC client with endpoint failover
- `crates/parser`: ERC transfer log parser
- `crates/storage`: PostgreSQL schema, ingestion, and query layer
- `crates/indexer`: catch-up and polling orchestration
- `crates/api`: HTTP routes and response models
- `crates/app`: application bootstrap and runtime wiring

## Quick Start

Create an environment file:

```bash
cp .env.example .env
```

Set the primary RPC endpoint:

```env
AETHER_RPC_HTTP_URL=https://arb1.arbitrum.io/rpc
```

Start the stack:

```bash
docker compose up -d --build
```

Check the API:

```bash
curl http://localhost:8090/health
```

Expected response:

```json
{"status":"ok"}
```

## RPC Failover

The primary RPC endpoint is required. Two backup endpoints are optional.

```env
AETHER_RPC_HTTP_URL=https://primary-rpc.example
AETHER_RPC_HTTP_URL_2=https://backup-rpc-2.example
AETHER_RPC_HTTP_URL_3=https://backup-rpc-3.example
```

When the active endpoint fails, the RPC client tries the next configured endpoint. After a backup endpoint succeeds, it becomes the preferred endpoint for the following calls.

## PostgreSQL

By default, Docker Compose starts a local PostgreSQL service and the application connects to it automatically.

To use an external PostgreSQL server while keeping the default Compose file, set `AETHER_DATABASE_URL` in `.env`:

```env
AETHER_RPC_HTTP_URL=https://arb1.arbitrum.io/rpc
AETHER_DATABASE_URL=postgres://user:password@postgres.example.com:5432/aether_indexer
```

If `AETHER_DATABASE_URL` is omitted, Docker Compose uses:

```env
postgres://postgres:postgres@postgres:5432/aether_indexer
```

To run only the indexer container and skip the bundled PostgreSQL service, use the external PostgreSQL Compose file:

```bash
docker compose -f docker-compose.external-postgres.yml up -d --build
```

This mode requires `AETHER_DATABASE_URL` in `.env`.

## Configuration

The Docker Compose defaults are suitable for Arbitrum One testing. These values can be changed in `docker-compose.yml` or exported in the runtime environment.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `AETHER_RPC_HTTP_URL` | Yes | None | Primary EVM HTTP RPC endpoint |
| `AETHER_RPC_HTTP_URL_2` | No | None | First backup RPC endpoint |
| `AETHER_RPC_HTTP_URL_3` | No | None | Second backup RPC endpoint |
| `AETHER_DATABASE_URL` | No in Docker, yes locally | Bundled PostgreSQL URL | PostgreSQL connection string |
| `AETHER_CHAIN_ID` | No | `42161` in Docker | EVM chain ID |
| `AETHER_START_BLOCK` | No | `0` | First block to index |
| `AETHER_CONFIRMATIONS` | No | `20` in Docker | Blocks kept behind the head before indexing |
| `AETHER_CHUNK_SIZE` | No | `1000` | Block range size per `eth_getLogs` request |
| `AETHER_POLL_INTERVAL_MS` | No | `3000` in Docker | Delay between polling attempts |
| `AETHER_RPC_TIMEOUT_MS` | No | `30000` in Docker | RPC request timeout |
| `AETHER_API_BIND` | No | `0.0.0.0:8090` | API bind address |

## API

### Health

```http
GET /health
```

### User token balances

```http
GET /v1/users/{address}/tokens?limit=50&offset=0
```

### User NFT balances

```http
GET /v1/users/{address}/nfts?limit=50&offset=0
```

The response includes `token_uri` when it has been fetched successfully.

### Asset search

```http
GET /v1/assets/search?q=usdc&limit=20
```

### Asset holders

```http
GET /v1/assets/{token_address}/holders?token_id={optional_token_id}&limit=50&offset=0
```

For ERC-20 assets, holders are grouped by token balance.

For ERC-721 assets, omitting `token_id` returns owners grouped by owned token count. Passing `token_id` returns the current owner of that token.

For ERC-1155 assets, omitting `token_id` returns holders grouped by total balance across token IDs. Passing `token_id` returns holders for that specific token ID.

### Transfers

```http
GET /v1/transfers?token_address={optional_token}&account_address={optional_account}&limit=50&offset=0
```

### Refresh NFT metadata

```http
POST /v1/nfts/{token_address}/{token_id}/refresh-metadata
```

This endpoint fetches the current token URI from the NFT contract:

- ERC-721 uses `tokenURI(uint256)`.
- ERC-1155 uses `uri(uint256)`.

The refresh endpoint is rate-limited per collection and token ID. A token can be refreshed once every five minutes. If the request is inside the cooldown window, the response status is `rate_limited` and includes `retry_after_seconds`.

When the refreshed token URI differs from the stored URI, `aether-indexer` tries to infer a base URI change. If the old and new URIs share the same token-specific suffix, the collection base URI is updated for all indexed tokens in that collection that still use the old base URI.

Example response:

```json
{
  "status": "updated",
  "token_address": "0x...",
  "token_id": "123",
  "token_uri": "ipfs://new-base/123.json",
  "previous_token_uri": "ipfs://old-base/123.json",
  "token_uri_changed": true,
  "collection_base_updated": true,
  "collection_tokens_updated": 42,
  "retry_after_seconds": null
}
```

## Indexing Flow

1. Read the last indexed block from `sync_state`.
2. Compute the next safe block range using `AETHER_CONFIRMATIONS`.
3. Fetch transfer logs through `eth_getLogs`.
4. Parse ERC-20, ERC-721, and ERC-1155 transfer events.
5. Insert transfers idempotently.
6. Update token balances, NFT ownership, NFT balances, and holder projections.
7. Register newly detected NFT token IDs and fetch their token URI when available.
8. Advance the cursor and continue polling.

## NFT Metadata Behavior

When an NFT token ID is first detected through an ERC-721 or ERC-1155 transfer, it is inserted into `nft_tokens` with a pending URI state. The indexer then calls the contract to fetch the token URI and stores the result.

If the initial URI fetch fails, the token remains indexed and the fetch error is recorded. A later manual metadata refresh can retry the contract call.

Manual refresh is intentionally conservative:

1. The API accepts a refresh request for a single `token_address` and `token_id`.
2. A five-minute cooldown is enforced for that exact token.
3. The current contract URI is fetched.
4. If the URI is unchanged, only the token refresh state is updated.
5. If the URI changed, the indexer infers the old and new base URI.
6. Only indexed tokens whose URI still starts with the old base URI are updated to the new base URI.

Base URI changes are not a universal ERC-721 event standard, so this behavior is based on comparing the refreshed token URI against the stored URI. This avoids rewriting an entire collection unless the checked token actually changed.

## Local Development

Run checks:

```bash
cargo check
cargo test
```

Run the app locally:

```bash
export AETHER_RPC_HTTP_URL=https://arb1.arbitrum.io/rpc
export AETHER_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/aether_indexer
cargo run -p app
```

## Operations

View container status:

```bash
docker compose ps
```

Follow application logs:

```bash
docker compose logs -f aether-indexer
```

Stop the stack:

```bash
docker compose down
```

Stop the stack and remove the bundled PostgreSQL volume:

```bash
docker compose down -v
```
