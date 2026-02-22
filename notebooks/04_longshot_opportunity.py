# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "boto3",
#     "pandas",
#     "altair",
#     "numpy",
#     "python-dotenv",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def setup_and_imports():
    import marimo as mo
    import altair as alt
    import pandas as pd
    import numpy as np

    from longshot.storage.athena import query

    return alt, mo, np, pd, query


@app.cell
def introduction(mo):
    mo.md(
        """
        # Daily Longshot Opportunity Counts

        How large is the "longshot opportunity set" on Kalshi each day?
        This notebook counts **open markets** and **longshot-priced markets**
        (last traded price 3–15 cents) at a daily 8pm UTC snapshot from
        2025-01-01 to 2026-02-22.

        **Data caveat**: We lack historical price snapshots, so we use
        `last_price` (the final traded price at data-pull time) as a proxy.
        For resolved markets this reflects the last trade before resolution,
        not the price on any specific historical date. The open/close time
        filtering is accurate — only `last_price` is approximate.
        """
    )
    return ()


@app.cell
def daily_counts_query(mo, pd, query):
    daily_counts = query("""
        WITH date_series AS (
            SELECT snapshot_date
            FROM UNNEST(sequence(date '2025-01-01', date '2026-02-22', interval '1' day)) AS t(snapshot_date)
        ),
        snapshot_ts AS (
            SELECT snapshot_date,
                   date_format(cast(snapshot_date AS timestamp) + interval '20' hour,
                               '%Y-%m-%dT%H:%i:%sZ') AS ts
            FROM date_series
        ),
        relevant_markets AS (
            SELECT m.open_time, m.close_time, m.last_price
            FROM markets m
            WHERE m.open_time <= '2026-02-22T20:00:00Z'
              AND m.close_time > '2025-01-01T20:00:00Z'
        )
        SELECT cast(s.snapshot_date AS varchar) AS snapshot_date,
               count(*) AS total_open,
               count_if(rm.last_price >= 3 AND rm.last_price <= 15) AS longshot_count
        FROM snapshot_ts s
        CROSS JOIN relevant_markets rm
        WHERE rm.open_time <= s.ts AND rm.close_time > s.ts
        GROUP BY s.snapshot_date
        ORDER BY s.snapshot_date
    """)

    daily_counts["snapshot_date"] = pd.to_datetime(daily_counts["snapshot_date"])
    daily_counts["longshot_pct"] = (
        daily_counts["longshot_count"] / daily_counts["total_open"] * 100
    )

    latest = daily_counts.iloc[-1]
    avg_open = daily_counts["total_open"].mean()
    avg_longshot = daily_counts["longshot_count"].mean()
    avg_pct = daily_counts["longshot_pct"].mean()

    mo.md(
        f"""
        ## Daily Open & Longshot Market Counts

        Queried **{len(daily_counts)}** daily snapshots (8pm UTC).

        | Metric | Latest Day | Average |
        |--------|-----------|---------|
        | Total open markets | {latest['total_open']:,.0f} | {avg_open:,.0f} |
        | Longshot (3–15¢) | {latest['longshot_count']:,.0f} | {avg_longshot:,.0f} |
        | Longshot % | {latest['longshot_pct']:.1f}% | {avg_pct:.1f}% |
        """
    )

    return (daily_counts,)


@app.cell
def daily_charts(alt, daily_counts, mo):
    total_area = (
        alt.Chart(daily_counts)
        .mark_area(color="steelblue", opacity=0.7)
        .encode(
            alt.X("snapshot_date:T", title="Date"),
            alt.Y("total_open:Q", title="Total Open Markets"),
        )
        .properties(width=700, height=200, title="Total Open Markets Over Time")
    )

    longshot_area = (
        alt.Chart(daily_counts)
        .mark_area(color="firebrick", opacity=0.7)
        .encode(
            alt.X("snapshot_date:T", title="Date"),
            alt.Y("longshot_count:Q", title="Longshot Count (3–15¢)"),
        )
        .properties(width=700, height=200, title="Longshot Markets Over Time")
    )

    pct_line = (
        alt.Chart(daily_counts)
        .mark_line(color="darkorange", strokeWidth=2)
        .encode(
            alt.X("snapshot_date:T", title="Date"),
            alt.Y("longshot_pct:Q", title="Longshot % of Open"),
        )
        .properties(width=700, height=200, title="Longshot % of Open Markets")
    )

    mo.vstack([total_area, longshot_area, pct_line])

    return ()


@app.cell
def category_breakdown_query(mo, pd, query):
    cat_daily = query("""
        WITH date_series AS (
            SELECT snapshot_date
            FROM UNNEST(sequence(date '2025-01-01', date '2026-02-22', interval '1' day)) AS t(snapshot_date)
        ),
        snapshot_ts AS (
            SELECT snapshot_date,
                   date_format(cast(snapshot_date AS timestamp) + interval '20' hour,
                               '%Y-%m-%dT%H:%i:%sZ') AS ts
            FROM date_series
        ),
        relevant_markets AS (
            SELECT m.open_time, m.close_time, m.last_price,
                   COALESCE(e.category, 'Unknown') AS category
            FROM markets m
            LEFT JOIN events e ON m.event_ticker = e.event_ticker
            WHERE m.open_time <= '2026-02-22T20:00:00Z'
              AND m.close_time > '2025-01-01T20:00:00Z'
        )
        SELECT cast(s.snapshot_date AS varchar) AS snapshot_date,
               rm.category,
               count(*) AS total_open,
               count_if(rm.last_price >= 3 AND rm.last_price <= 15) AS longshot_count
        FROM snapshot_ts s
        CROSS JOIN relevant_markets rm
        WHERE rm.open_time <= s.ts AND rm.close_time > s.ts
        GROUP BY s.snapshot_date, rm.category
        ORDER BY s.snapshot_date, rm.category
    """)

    cat_daily["snapshot_date"] = pd.to_datetime(cat_daily["snapshot_date"])

    # Identify top 6 categories by average longshot count
    cat_avg_longshot = (
        cat_daily.groupby("category")["longshot_count"]
        .mean()
        .sort_values(ascending=False)
    )
    top6_cats = cat_avg_longshot.head(6).index.tolist()

    cat_daily["category_group"] = cat_daily["category"].where(
        cat_daily["category"].isin(top6_cats), "Other"
    )

    cat_grouped = (
        cat_daily.groupby(["snapshot_date", "category_group"])
        .agg(
            longshot_count_g=("longshot_count", "sum"),
            total_open_g=("total_open", "sum"),
        )
        .reset_index()
    )

    mo.md(
        f"""
        ## Category Breakdown

        Top 6 categories by average daily longshot count:
        **{', '.join(top6_cats)}**.
        All remaining categories are grouped as "Other".
        """
    )

    return cat_grouped, top6_cats


@app.cell
def category_charts(alt, cat_grouped, mo, top6_cats):
    cat_order = top6_cats + ["Other"]

    longshot_cat_area = (
        alt.Chart(cat_grouped)
        .mark_area()
        .encode(
            alt.X("snapshot_date:T", title="Date"),
            alt.Y("longshot_count_g:Q", title="Longshot Count", stack=True),
            alt.Color(
                "category_group:N",
                title="Category",
                sort=cat_order,
            ),
        )
        .properties(width=700, height=300, title="Longshot Markets by Category")
    )

    total_cat_area = (
        alt.Chart(cat_grouped)
        .mark_area()
        .encode(
            alt.X("snapshot_date:T", title="Date"),
            alt.Y("total_open_g:Q", title="Total Open", stack=True),
            alt.Color(
                "category_group:N",
                title="Category",
                sort=cat_order,
            ),
        )
        .properties(width=700, height=300, title="Total Open Markets by Category")
    )

    mo.vstack([longshot_cat_area, total_cat_area])

    return ()


@app.cell
def summary_and_caveats(daily_counts, mo, pd):
    total_days = len(daily_counts)
    min_open = daily_counts["total_open"].min()
    max_open = daily_counts["total_open"].max()
    min_longshot = daily_counts["longshot_count"].min()
    max_longshot = daily_counts["longshot_count"].max()
    avg_open_s = daily_counts["total_open"].mean()
    avg_longshot_s = daily_counts["longshot_count"].mean()
    avg_pct_s = daily_counts["longshot_pct"].mean()

    summary_table = pd.DataFrame([
        {"Metric": "Date range", "Value": f"{daily_counts['snapshot_date'].min().date()} to {daily_counts['snapshot_date'].max().date()}"},
        {"Metric": "Total days", "Value": f"{total_days}"},
        {"Metric": "Avg open markets / day", "Value": f"{avg_open_s:,.0f}"},
        {"Metric": "Min / Max open", "Value": f"{min_open:,.0f} / {max_open:,.0f}"},
        {"Metric": "Avg longshots / day", "Value": f"{avg_longshot_s:,.0f}"},
        {"Metric": "Min / Max longshots", "Value": f"{min_longshot:,.0f} / {max_longshot:,.0f}"},
        {"Metric": "Avg longshot %", "Value": f"{avg_pct_s:.1f}%"},
    ])

    mo.md(
        """
        ## Summary Statistics
        """
    )

    mo.ui.table(summary_table, label="Daily Opportunity Summary")

    mo.md(
        """
        ### Caveats

        1. **`last_price` proxy**: We use the final traded price at data-pull
           time rather than the actual price on each historical date. Resolved
           markets show their last trade before resolution, not the 8pm price.
           This overstates longshot counts for markets that were higher-priced
           earlier and drifted down before resolving.

        2. **Market lifecycle**: Markets that opened and closed within a single
           day may be missed or double-counted depending on exact open/close
           timestamps relative to 8pm UTC.

        3. **Category composition**: The category breakdown uses the current
           event categorization. Some events may have been recategorized over
           the period.

        4. **No liquidity filter**: These counts include all markets in the
           3–15¢ range regardless of whether they had any trading activity.
           Most longshot markets are illiquid and may not represent executable
           opportunities.
        """
    )

    return ()


if __name__ == "__main__":
    app.run()
