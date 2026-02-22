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
- **SQL quoting in marimo**: Athena SQL uses single quotes for string literals (`'1-week'`). Always pass SQL via Python **f-strings with triple double quotes** (`f"""..."""`), which allows single quotes inside the SQL without escaping. Never use double quotes inside the SQL — Athena treats them as identifier quotes, not string delimiters. When running queries outside marimo (e.g., in a `python3 -c "..."` shell command), use a heredoc (`<< 'PYEOF'`) to avoid shell quote conflicts.
- **Dollar signs in `mo.md()`**: Marimo renders markdown with LaTeX support, so bare `$` is interpreted as a math delimiter. To display literal dollar amounts, escape as `\$`. To avoid Python's "invalid escape sequence" warning, use a **raw string**: `mo.md(r"""...\$2,595...""")`.
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
