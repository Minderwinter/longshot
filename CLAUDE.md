# Longshot Bias Project

## Athena Usage

- **All analytical queries against S3 data should use Athena**, not DuckDB. This machine has limited RAM (3.8GB) and the dataset is 5.7M+ markets across 58 parquet chunks — DuckDB will OOM on full scans.
- Use `longshot.storage.athena.query(sql)` which handles execution, polling, pagination, and auto-casts numeric columns from the string results Athena returns.
- Athena queries run against Glue database `longshot` with tables `markets` and `events`. The `query()` function defaults to this database.
- Query results land in `s3://cmidwinter-data/athena-results/` — these accumulate and can be cleaned up periodically.
- **Category is on the `events` table, not `markets`.** Always JOIN: `FROM markets m LEFT JOIN events e ON m.event_ticker = e.event_ticker`.
- Prices (`yes_ask`, `yes_bid`, etc.) are in **cents** (0-100), not fractional probabilities (0-1). Divide by 100 to get implied probability.

## Marimo Notebooks

- Marimo tracks **all variable assignments** in a cell as outputs. Every variable name must be unique across all cells — you cannot reuse names like `chart`, `df`, `col` in multiple cells.
- Return `()` from cells that don't need to export variables.
- **Do not use `for` loops with simple variable names** (e.g., `for col in ...`) inside cells — marimo will treat the loop variable as a cell output and error on duplicates. Instead, put helper logic in imported modules or use comprehensions.
- Module-level functions (defined outside `@app.cell`) are **not visible** to cells. Define helpers in a cell and return them, or put them in a separate module.
- **Pre-aggregate in SQL.** Never pull millions of raw rows into the notebook — Athena can handle the aggregation server-side. For histograms, compute bins and counts in SQL rather than pulling raw values for client-side binning.
- Export HTML with: `uv run marimo export html notebooks/<name>.py -o notebooks/html/<name>.html`

## Project Structure

```
longshot/
├── api/         # Kalshi API client, models, rate limiter
├── ingestion/   # Market/trade fetching and snapshot orchestration
├── storage/     # athena.py (primary), db.py (DuckDB fallback), s3.py (parquet writes)
scripts/         # CLI ingestion scripts (ingest_markets.py, ingest_events.py)
notebooks/       # Marimo notebooks (.py files)
```

## Key Commands

```bash
uv run python scripts/ingest_markets.py              # Full market pull to S3
uv run python scripts/ingest_events.py                # Events pull to S3
uv run marimo edit notebooks/01_descriptive_analysis.py  # Interactive notebook
uv run marimo export html notebooks/01_descriptive_analysis.py -o notebooks/html/01_descriptive_analysis.html
```
