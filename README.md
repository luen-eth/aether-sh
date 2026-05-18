<p align="center">
  <img src="docs/assets/banner.png" alt="aether-indexer banner" width="100%" />
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a>
  |
  <a href="#architecture">Architecture</a>
  |
  <a href="#api">API</a>
  |
  <a href="#nft-metadata-refresh">NFT Metadata</a>
  |
  <a href="#operations">Operations</a>
</p>

# aether-indexer

`aether-indexer` is a Rust-based EVM indexer and HTTP API for token and NFT data.

It scans chain logs from a configured start block, indexes ERC-20, ERC-721, and ERC-1155 transfer events, stores normalized history in PostgreSQL, and maintains query-ready projections for balances, holders, ownership, and NFT token URIs.

## What It Indexes

| Data | Source | Stored output |
| --- | --- | --- |
| ERC-20 transfers | `Transfer(address,address,uint256)` | Transfers, token balances, holders |
| ERC-721 transfers | `Transfer(address,address,uint256)` | Transfers, ownership, holders, token URI |
| ERC-1155 transfers | `TransferSingle`, `TransferBatch` | Transfers, balances, holders, token URI |
| NFT metadata refresh | `tokenURI(uint256)` or `uri(uint256)` | Updated token URI and optional collection base URI propagation |

Native coin transfers, approvals, arbitrary contract calls, and unrelated logs are not stored.

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

## Architecture

```mermaid
flowchart LR
    RPC["EVM HTTP RPC<br/>primary + 2 backups"] --> INDEXER["Indexer service<br/>catch-up + polling"]
    INDEXER --> PARSER["Log parser<br/>ERC-20 / ERC-721 / ERC-1155"]
    PARSER --> STORAGE["PostgreSQL storage<br/>idempotent writes"]
    STORAGE --> API["Axum HTTP API"]
    API --> CLIENTS["Apps / explorers / dashboards"]

    INDEXER --> URI["NFT URI fetcher<br/>tokenURI / uri"]
    URI --> STORAGE
    API --> REFRESH["Manual metadata refresh<br/>5 min per-token cooldown"]
    REFRESH --> URI
```

### Runtime Flow

```mermaid
sequenceDiagram
    participant I as Indexer
    participant R as RPC
    participant P as Parser
    participant D as PostgreSQL
    participant A as API

    I->>D: Read sync_state.last_scanned_block
    I->>R: eth_blockNumber
    I->>I: Apply confirmation lag
    I->>R: eth_getLogs for transfer topics
    I->>P: Parse ERC-20 / ERC-721 / ERC-1155 events
    P->>D: Insert transfers idempotently
    D->>D: Update balances, ownership, holders
    I->>R: tokenURI or uri for newly detected NFTs
    I->>D: Store NFT token URI or fetch error
    A->>D: Query balances, NFTs, holders, transfers
```

## Data Model

```mermaid
erDiagram
    ASSETS ||--o{ TRANSFERS : "has"
    ASSETS ||--o{ TOKEN_BALANCES : "projects"
    ASSETS ||--o{ NFT_OWNERSHIPS : "owns"
    ASSETS ||--o{ NFT_BALANCES : "balances"
    ASSETS ||--o{ NFT_TOKENS : "metadata"
    ASSETS ||--o| NFT_COLLECTION_METADATA : "base uri"

    ASSETS {
      text address PK
      text standard
      text name
      text symbol
      int decimals
      bigint first_seen_block
    }

    TRANSFERS {
      bigint chain_id
      bigint block_number
      text tx_hash
      bigint log_index
      text token_address FK
      text standard
      text from_address
      text to_address
      text token_id
      numeric value_numeric
    }

    TOKEN_BALANCES {
      text token_address FK
      text holder_address
      numeric balance_numeric
    }

    NFT_OWNERSHIPS {
      text token_address FK
      text token_id
      text owner_address
    }

    NFT_BALANCES {
      text token_address FK
      text token_id
      text holder_address
      numeric balance_numeric
    }

    NFT_TOKENS {
      text token_address FK
      text token_id
      text standard
      text token_uri
      text uri_status
      timestamptz last_uri_fetch_at
      timestamptz last_refresh_requested_at
    }

    NFT_COLLECTION_METADATA {
      text token_address FK
      text base_uri
      text updated_from_token_id
      timestamptz last_base_uri_update_at
    }
```

## RPC Failover

The primary RPC endpoint is required. Two backup endpoints are optional.

```env
AETHER_RPC_HTTP_URL=https://primary-rpc.example
AETHER_RPC_HTTP_URL_2=https://backup-rpc-2.example
AETHER_RPC_HTTP_URL_3=https://backup-rpc-3.example
```

When the active endpoint fails, the RPC client tries the next configured endpoint. After a backup endpoint succeeds, it becomes the preferred endpoint for subsequent calls.

```mermaid
flowchart TD
    START["RPC request"] --> P1["Try primary endpoint"]
    P1 -->|success| DONE["Return response"]
    P1 -->|failure| P2["Try backup endpoint 2"]
    P2 -->|success| SWITCH["Mark backup 2 as preferred"]
    P2 -->|failure| P3["Try backup endpoint 3"]
    P3 -->|success| SWITCH3["Mark backup 3 as preferred"]
    P3 -->|failure| FAIL["Return RPC error"]
    SWITCH --> DONE
    SWITCH3 --> DONE
```

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

To run only the indexer container and skip the bundled PostgreSQL service, use:

```bash
docker compose -f docker-compose.external-postgres.yml up -d --build
```

This mode requires `AETHER_DATABASE_URL` in `.env`.

## Configuration

The Docker Compose defaults are suitable for Arbitrum One testing. Values can be changed in `docker-compose.yml` or exported in the runtime environment.

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

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Service health check |
| `GET /v1/users/{address}/tokens` | ERC-20 balances for a user |
| `GET /v1/users/{address}/nfts` | ERC-721 and ERC-1155 balances for a user |
| `GET /v1/assets/search` | Search assets by address, symbol, or name |
| `GET /v1/assets/{token_address}/holders` | Holder list for ERC-20, ERC-721, or ERC-1155 |
| `GET /v1/transfers` | Recent transfers, optionally filtered by token or account |
| `POST /v1/nfts/{token_address}/{token_id}/refresh-metadata` | Refresh one NFT token URI with cooldown |

### Examples

```http
GET /v1/users/{address}/tokens?limit=50&offset=0
```

```http
GET /v1/users/{address}/nfts?limit=50&offset=0
```

The NFT response includes `token_uri` when it has been fetched successfully.

```http
GET /v1/assets/search?q=usdc&limit=20
```

```http
GET /v1/assets/{token_address}/holders?token_id={optional_token_id}&limit=50&offset=0
```

```http
GET /v1/transfers?token_address={optional_token}&account_address={optional_account}&limit=50&offset=0
```

## NFT Metadata Refresh

```http
POST /v1/nfts/{token_address}/{token_id}/refresh-metadata
```

The endpoint fetches the current token URI from the NFT contract:

- ERC-721 uses `tokenURI(uint256)`.
- ERC-1155 uses `uri(uint256)`.

Refresh requests are limited to one request every five minutes for each `token_address + token_id` pair.

```mermaid
flowchart TD
    REQ["Refresh request<br/>collection + token ID"] --> LIMIT{"Inside 5 minute cooldown?"}
    LIMIT -->|yes| RATE["Return rate_limited<br/>retry_after_seconds"]
    LIMIT -->|no| CALL["Call tokenURI or uri"]
    CALL --> COMPARE{"URI changed?"}
    COMPARE -->|no| SAME["Update fetch timestamp<br/>Return unchanged"]
    COMPARE -->|yes| INFER["Infer old and new base URI"]
    INFER --> CAN{"Base URI inferred?"}
    CAN -->|no| SINGLE["Store only refreshed token URI"]
    CAN -->|yes| COLLECTION["Rewrite indexed collection tokens<br/>that still use old base URI"]
    SINGLE --> UPDATED["Return updated"]
    COLLECTION --> UPDATED
```

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

Base URI changes are not a universal ERC-721 event standard. `aether-indexer` therefore updates a collection base URI only when a requested token refresh proves that the token URI changed and the old/new base URI can be inferred safely.

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
