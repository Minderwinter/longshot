# Kalshi Data Schema Documentation

This document describes the schema of the Kalshi market data stored in S3 and
queried via Athena (Glue database `longshot`). It covers the **markets**,
**events**, and **trades** tables, how they join together, and what additional
data the Kalshi API exposes that we are not currently capturing.

---

## Table: `markets`

Each row represents a single binary-outcome market on Kalshi. Markets are the
tradeable instruments — a single event (e.g. "What will the high temp be in NYC
on Nov 27?") can have many markets (one per outcome bucket).

**S3 locations:**
- Full universe: `s3://{bucket}/{prefix}/markets/all/chunk_*.parquet`
- Snapshot-filtered: `s3://{bucket}/{prefix}/markets/snapshot_date={YYYY-MM-DD}/data.parquet`

**Source:** `GET /markets` with `mve_filter=exclude` (excludes multivariate
event combo markets). Defined in `longshot/api/models.py:Market` and schema in
`longshot/storage/s3.py:MARKETS_SCHEMA`.

| Column | Arrow Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `ticker` | `string` | No | Unique market identifier (e.g. `HIGHNY-22NOV27-B55`). Primary key. |
| `event_ticker` | `string` | No | Ticker of the parent event this market belongs to. Foreign key to `events.event_ticker`. |
| `title` | `string` | No | Human-readable description of the market outcome (e.g. "High of 55°F or above"). |
| `status` | `string` | No | Lifecycle state. Values returned by the API: `initialized`, `inactive`, `active`, `closed`, `determined`, `disputed`, `amended`, `finalized`. Note: the API accepts `open`/`settled` as filter aliases but returns `active`/`finalized` in responses. |
| `yes_bid` | `float64` | Yes | Best bid price for the YES side, in **cents** (0–100). Divide by 100 for implied probability. |
| `yes_ask` | `float64` | Yes | Best ask price for the YES side, in **cents** (0–100). |
| `no_bid` | `float64` | Yes | Best bid price for the NO side, in **cents** (0–100). |
| `no_ask` | `float64` | Yes | Best ask price for the NO side, in **cents** (0–100). |
| `last_price` | `float64` | Yes | Last traded price, in **cents** (0–100). |
| `volume` | `int64` | Yes | Total number of contracts ever traded on this market. |
| `volume_24h` | `int64` | Yes | Number of contracts traded in the trailing 24-hour window. |
| `open_interest` | `int64` | Yes | Number of contracts currently outstanding (open positions). |
| `close_time` | `string` | Yes | ISO 8601 timestamp when the market closed (or will close) for trading. |
| `open_time` | `string` | Yes | ISO 8601 timestamp when the market opened (or will open) for trading. |
| `result` | `string` | Yes | Settlement outcome: `"yes"`, `"no"`, or `"all_no"`. `NULL` if the market has not yet settled. |
| `created_time` | `string` | Yes | ISO 8601 timestamp when the market was first created on Kalshi. |

---

## Table: `events`

Each row represents an event — a container that groups related markets under a
common question or topic. Events carry the **category** classification, which
is not present on the markets table.

**S3 location:** `s3://{bucket}/{prefix}/events/data.parquet` (single file, no
partitioning).

**Source:** `GET /events`. Defined in `scripts/ingest_events.py:EVENTS_SCHEMA`.

| Column | Arrow Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `event_ticker` | `string` | No | Unique event identifier (e.g. `HIGHNY-22NOV27`). Primary key. |
| `category` | `string` | Yes | Topic classification (e.g. `"Climate and Weather"`, `"Politics"`, `"Economics"`, `"Sports"`). |
| `title` | `string` | Yes | Human-readable event title (e.g. "What will the high temp be in NYC on Nov 27, 2022?"). |
| `sub_title` | `string` | Yes | Additional context or date qualifier (e.g. "On Nov 27, 2022"). |
| `mutually_exclusive` | `bool` | No | Whether the markets within this event are mutually exclusive (exactly one resolves YES). |
| `series_ticker` | `string` | Yes | Ticker of the parent series this event belongs to (e.g. `HIGHNY`). Series group recurring events of the same type. |

---

## Table: `trades` (snapshot-based)

Each row represents a single executed trade (fill) on a market. Trades are only
ingested as part of the snapshot pipeline and scoped to a time window.

**S3 location:**
`s3://{bucket}/{prefix}/trades/snapshot_date={YYYY-MM-DD}/data.parquet`

**Source:** `GET /markets/trades` per ticker. Defined in
`longshot/api/models.py:Trade` and schema in `longshot/storage/s3.py:TRADES_SCHEMA`.

| Column | Arrow Type | Nullable | Description |
|--------|-----------|----------|-------------|
| `trade_id` | `string` | No | Unique identifier for the trade. |
| `ticker` | `string` | No | Market ticker this trade executed on. Foreign key to `markets.ticker`. |
| `yes_price` | `float64` | No | Price of the YES side in this trade, in **cents** (0–100). |
| `no_price` | `float64` | No | Price of the NO side in this trade, in **cents** (0–100). Always `100 - yes_price`. |
| `count` | `int64` | No | Number of contracts in this trade. |
| `taker_side` | `string` | Yes | Which side the taker was on: `"yes"` or `"no"`. |
| `created_time` | `string` | Yes | ISO 8601 timestamp when the trade was executed. |

---

## Joining the Tables

The primary join is between `markets` and `events` via `event_ticker`:

```sql
SELECT m.*, e.category, e.series_ticker
FROM markets m
LEFT JOIN events e ON m.event_ticker = e.event_ticker
```

Use `LEFT JOIN` because a market should always have a parent event, but
defensive joining avoids dropping rows if an event record is missing.

Trades join to markets via `ticker`:

```sql
SELECT t.*, m.title, m.result, e.category
FROM trades t
JOIN markets m ON t.ticker = m.ticker
LEFT JOIN events e ON m.event_ticker = e.event_ticker
```

**Relationship cardinality:**
- One **event** → many **markets** (1:N via `event_ticker`)
- One **market** → many **trades** (1:N via `ticker`)
- One **series** → many **events** (1:N via `series_ticker`, series data not stored)

---

## Gap Analysis: Data We Are Not Capturing

The Kalshi API returns substantially more fields than we currently store. Below
is a catalog of what we're missing, organized by significance.

### Missing Market Fields

**High value — would improve analysis:**

| Field | Type | Why it matters |
|-------|------|----------------|
| `expiration_time` | timestamp | Distinct from `close_time`. Markets can close for trading before they expire/settle. Needed for accurate lifecycle analysis. |
| `updated_time` | timestamp | When the market was last modified. Useful for incremental ingestion and change detection. |
| `settlement_value` | integer | Actual settlement payout. Important for P&L analysis beyond binary yes/no. |
| `market_type` | string | Always `"binary"` today but future-proofs the schema. |
| `subtitle` | string | Additional market context not captured in `title`. |
| `can_close_early` | boolean | Whether the market can settle before its scheduled close. Affects risk modeling. |
| `series_ticker` | string | Available directly on the market object; would eliminate the need to join through events for series-level analysis. |
| `rules_primary` | string | The resolution rules text. Useful for NLP analysis of market structure. |
| `rules_secondary` | string | Additional resolution criteria. |

**Medium value — useful for specialized analysis:**

| Field | Type | Why it matters |
|-------|------|----------------|
| `previous_yes_bid` | integer | Prior best bid — enables spread/movement analysis without candlestick data. |
| `previous_yes_ask` | integer | Prior best ask. |
| `previous_price` | integer | Prior last-traded price. |
| `yes_sub_title` / `no_sub_title` | string | Labels for the yes/no sides of the market. |
| `expected_expiration_time` | timestamp | When expiration is expected (may differ from `latest_expiration_time`). |
| `latest_expiration_time` | timestamp | Latest possible expiration. |
| `strike_type` | string | Type of strike for the market. |
| `tick_size` | integer | Minimum price increment (deprecated in favor of `price_level_structure`). |
| `notional_value` | integer | Contract notional value in cents. |

**Low value / deprecated — probably skip:**

| Field | Type | Notes |
|-------|------|-------|
| `liquidity` / `liquidity_dollars` | int/string | Deprecated; returns 0. |
| `response_price_units` | string | Always `"usd_cent"`. |
| `*_dollars` fields | string | Dollar-string versions of cent prices (`"0.5600"`). Redundant if we have cent values. |
| `*_fp` fields | string | Fixed-point versions of volume/OI. Redundant with integer fields. |
| `fractional_trading_enabled` | boolean | Relevant only for fractional contract trading. |
| `mve_*` fields | various | MVE combo market fields. We intentionally exclude MVE markets via `mve_filter=exclude`. |
| `settlement_timer_seconds` | integer | Countdown to settlement. |
| `settlement_ts` | timestamp | Added Dec 2025. Settlement timestamp. |
| `price_level_structure` | object | Replaces `tick_size`. Defines the pricing grid. |

### Missing Event Fields

| Field | Type | Why it matters |
|-------|------|----------------|
| `collateral_return_type` | string | How collateral is returned on settlement. |
| `strike_date` | datetime | The date of the event's "strike" (e.g. the weather observation date). Very useful for time-series analysis of recurring events. |
| `strike_period` | string | Period qualifier for the strike (e.g. daily, weekly). |
| `available_on_brokers` | boolean | Whether the event is available through broker integrations. |
| `product_metadata` | object | Additional structured metadata. |

### Missing Trade Fields

| Field | Type | Why it matters |
|-------|------|----------------|
| `ts` | integer | Unix timestamp of the trade (alternative to `created_time`). |
| `price` | integer | Generic trade price in cents (as opposed to the directional `yes_price`/`no_price`). |
| `count_fp` | string | Fixed-point contract count. Redundant with `count`. |
| `*_dollars` fields | string | Dollar-string versions. Redundant with cent values. |

### Entire Data Sources Not Captured

| Data Source | Endpoint | Why it matters |
|-------------|----------|----------------|
| **Series** | `GET /series/{series_ticker}` | Series are the top-level grouping (e.g. "NYC Daily High Temp"). Stores metadata about recurring event patterns. Would enable series-level aggregation without string parsing. |
| **Candlestick / OHLC** | `GET /series/{series_ticker}/markets/{ticker}/candlesticks` | Time-series price data at configurable intervals (1m, 1h, 1d). Currently we only have point-in-time snapshots and trade-level data. Candlesticks would provide efficient OHLC history. |
| **Orderbook snapshots** | `GET /markets/{ticker}/orderbook` | Full depth-of-book at a point in time. Would enable liquidity analysis, bid-ask spread studies, and market microstructure research. |
| **Historical markets** | `GET /historical/markets` | As of March 6, 2026, settled markets older than the cutoff timestamp will be removed from the live `GET /markets` endpoint. Our current ingestion pulls from the live endpoint only — we will stop seeing old settled markets unless we also query the historical endpoint. |
| **Historical candlesticks** | `GET /historical/markets/{ticker}/candlesticks` | OHLC data for markets that have aged out of the live API. |
| **Event forecast percentiles** | `GET /events/{ticker}/forecast/percentile_history` | Historical forecast distribution data for events. |
| **Event milestones** | `GET /events` with `with_milestones=true` | Key event milestones/timeline data. |

### Critical Upcoming Risk: Historical Data Cutoff

Kalshi is partitioning data into **live** and **historical** tiers, with the
historical cutoff targeted for **March 6, 2026**. After that date:

- Settled markets older than `market_settled_ts` (initially ~1 year lookback)
  will disappear from `GET /markets`.
- Trade fills older than `trades_created_ts` will disappear from
  `GET /markets/trades`.

Our current ingestion scripts only hit the live endpoints. If we don't adapt
before the cutoff, future full-universe pulls will silently lose older settled
markets that we may not have captured yet. We should either:

1. Run a comprehensive ingestion before March 6, 2026 to capture all currently-visible settled markets, or
2. Update the ingestion pipeline to also query `GET /historical/markets` and `GET /historical/fills`.
