# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "boto3",
#     "pandas",
#     "altair",
#     "python-dotenv",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def setup():
    import marimo as mo
    import altair as alt

    from longshot.storage.athena import query

    return alt, mo, query


@app.cell
def find_partitions(mo, query):
    partition_info = query("""
        SELECT
            'daily_markets' AS tbl,
            max(date) AS latest_date,
            max(hour) AS latest_hour
        FROM daily_markets
        UNION ALL
        SELECT
            'daily_events' AS tbl,
            max(date) AS latest_date,
            max(hour) AS latest_hour
        FROM daily_events
    """)

    mkt_row = partition_info[partition_info["tbl"] == "daily_markets"].iloc[0]
    evt_row = partition_info[partition_info["tbl"] == "daily_events"].iloc[0]
    mkt_date = mkt_row["latest_date"]
    mkt_hour = str(int(mkt_row["latest_hour"]))
    evt_date = evt_row["latest_date"]
    evt_hour = str(int(evt_row["latest_hour"]))

    mo.md(
        f"""
        # Daily Market Analysis

        Using the most recent daily snapshots from Athena.

        | Table | Date | Hour (UTC) |
        |-------|------|------------|
        | daily_markets | {mkt_date} | {mkt_hour} |
        | daily_events | {evt_date} | {evt_hour} |
        """
    )
    return mkt_date, mkt_hour, evt_date, evt_hour


@app.cell
def daily_overview(mo, query):
    overview_stats = query("""
        SELECT
            count(*)                            AS total_markets,
            count(DISTINCT m.event_ticker)      AS distinct_events,
            count(DISTINCT e.category)          AS categories,
            sum(m.volume)                       AS total_volume,
            avg(m.open_interest)                AS avg_open_interest
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
    """)

    ov = overview_stats.iloc[0]
    mo.md(
        f"""
        ## Snapshot Overview

        | Metric | Value |
        |--------|-------|
        | Total active markets | {int(ov.total_markets):,} |
        | Distinct events | {int(ov.distinct_events):,} |
        | Categories | {int(ov.categories)} |
        | Total volume | {int(ov.total_volume):,} contracts |
        | Avg open interest | {ov.avg_open_interest:,.1f} |
        """
    )
    return ()


@app.cell
def days_to_expiry_dist(alt, mo, query, mkt_date):
    dte_data = query(f"""
        WITH dte AS (
            SELECT
                date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) AS raw_dte
            FROM daily_markets
            WHERE close_time IS NOT NULL
              AND close_time != ''
              AND date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) >= 1
        )
        SELECT
            LEAST(raw_dte, 29) AS dte_day,
            CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END AS dte_label,
            count(*) AS market_count
        FROM dte
        GROUP BY LEAST(raw_dte, 29),
                 CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END
        ORDER BY 1
    """)

    dte_sort = [str(i) for i in range(1, 29)] + ["29+"]

    dte_chart = (
        alt.Chart(dte_data)
        .mark_bar()
        .encode(
            alt.X("dte_label:N", sort=dte_sort, title="Days to Expiry"),
            alt.Y("market_count:Q", title="Number of Markets"),
            tooltip=[
                alt.Tooltip("dte_label:N", title="DTE"),
                alt.Tooltip("market_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Markets by Days to Expiry")
    )

    mo.md("## Distribution of Markets by Days to Expiry")
    mo.ui.altair_chart(dte_chart)
    return ()


@app.cell
def dte_commentary(mo):
    mo.md(
        """
        With daily granularity, the 1-day bin dominates (~22k markets) — these
        are predominantly daily crypto and financials contracts that roll over.
        A secondary spike appears at day 14, likely biweekly expiry contracts.
        Days 3-13 are very sparse, and the 29+ catch-all aggregates ~19k
        longer-dated markets (months to years out), making the distribution
        clearly bimodal: short-term rollers and long-horizon event markets.
        """
    )
    return ()


@app.cell
def dte_volume_dist(alt, mo, query, mkt_date):
    dte_vol_data = query(f"""
        WITH dte_v AS (
            SELECT
                date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) AS raw_dte,
                volume_24h
            FROM daily_markets
            WHERE close_time IS NOT NULL
              AND close_time != ''
              AND date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) >= 1
              AND volume_24h IS NOT NULL
        )
        SELECT
            LEAST(raw_dte, 29) AS dte_vol_day,
            CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END AS dte_vol_label,
            sum(volume_24h) AS total_volume_24h
        FROM dte_v
        GROUP BY LEAST(raw_dte, 29),
                 CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END
        ORDER BY 1
    """)

    dte_vol_sort = [str(i) for i in range(1, 29)] + ["29+"]

    dte_vol_chart = (
        alt.Chart(dte_vol_data)
        .mark_bar(color="teal")
        .encode(
            alt.X("dte_vol_label:N", sort=dte_vol_sort, title="Days to Expiry"),
            alt.Y("total_volume_24h:Q", title="24h Trade Volume (contracts)"),
            tooltip=[
                alt.Tooltip("dte_vol_label:N", title="DTE"),
                alt.Tooltip("total_volume_24h:Q", title="24h Volume", format=","),
            ],
        )
        .properties(width=700, height=400, title="24h Trade Volume by Days to Expiry")
    )

    mo.md("## 24-Hour Trade Volume by Days to Expiry")
    mo.ui.altair_chart(dte_vol_chart)
    return ()


@app.cell
def dte_volume_commentary(mo):
    mo.md(
        """
        Volume tells a different story from market count. Day 14 shows a massive
        spike (~25M contracts in 24h) that dwarfs all other buckets, even
        though it has far fewer markets than day 1. The 29+ catch-all is the
        second-largest volume bin (~9M), while the 1-day bin that dominates
        market count comes in third (~3M). Liquidity clusters around specific
        expiry cycles rather than following the market-count distribution.
        """
    )
    return ()


@app.cell
def midpoint_price_dist(alt, mo, query):
    midprice_data = query("""
        WITH priced AS (
            SELECT
                (yes_bid + yes_ask) / 2.0 AS midpoint
            FROM daily_markets
            WHERE yes_bid IS NOT NULL
              AND yes_ask IS NOT NULL
        )
        SELECT
            CAST(FLOOR(midpoint / 5) * 5 AS INTEGER) AS price_bin,
            count(*) AS market_count
        FROM priced
        GROUP BY FLOOR(midpoint / 5) * 5
        ORDER BY 1
    """)

    midprice_chart = (
        alt.Chart(midprice_data)
        .mark_bar()
        .encode(
            alt.X("price_bin:Q", title="Midpoint Yes Price (cents)", scale=alt.Scale(domain=[0, 100])),
            alt.Y("market_count:Q", title="Number of Markets"),
            tooltip=[
                alt.Tooltip("price_bin:Q", title="Price Bin (cents)"),
                alt.Tooltip("market_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Markets by Midpoint Yes Price")
    )

    mo.md("## Distribution of Markets by Midpoint Yes Price")
    mo.ui.altair_chart(midprice_chart)
    return ()


@app.cell
def midprice_commentary(mo):
    mo.md(
        """
        The price distribution is heavily right-skewed: roughly two-thirds of
        active markets have a midpoint below 5 cents, meaning the market assigns
        them very low probability. There is a secondary bump around 45-55 cents
        (the "coin-flip" zone) and a modest uptick at 95+ cents for near-certain
        outcomes. This shape is typical of prediction markets where most contracts
        within a multi-outcome event are priced as longshots.
        """
    )
    return ()


@app.cell
def category_dist(alt, mo, query):
    cat_data = query("""
        SELECT
            COALESCE(e.category, 'Unknown') AS category_name,
            count(*) AS market_count
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        GROUP BY COALESCE(e.category, 'Unknown')
        ORDER BY count(*) DESC
    """)

    cat_chart = (
        alt.Chart(cat_data)
        .mark_bar()
        .encode(
            alt.X("market_count:Q", title="Number of Markets"),
            alt.Y("category_name:N", sort="-x", title="Category"),
            tooltip=[
                alt.Tooltip("category_name:N", title="Category"),
                alt.Tooltip("market_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Markets by Category")
    )

    mo.md("## Distribution of Markets by Category")
    mo.ui.altair_chart(cat_chart)
    return ()


@app.cell
def category_commentary(mo):
    mo.md(
        """
        Crypto dominates the active market count at ~44%, followed by Sports at
        ~24%. This is partly structural — crypto events tend to generate many
        individual strike-level markets (e.g., "BTC above $X"). Financials,
        Entertainment, and Politics round out the top five. The long tail of
        smaller categories (Health, Transportation) has very few active markets.
        """
    )
    return ()


@app.cell
def longshot_header(mo):
    mo.md(
        """
        ---

        # Longshot Markets (Last Price $0.03 - $0.15)

        Filtering to markets where the last traded yes price is between 3 and 15 cents
        (inclusive). These are the "longshot" contracts — low-probability events
        where the favorite-longshot bias is most pronounced.
        """
    )
    return ()


@app.cell
def longshot_overview(mo, query):
    ls_stats = query("""
        SELECT
            count(*)                            AS ls_total,
            count(DISTINCT m.event_ticker)      AS ls_events,
            sum(m.volume)                       AS ls_volume,
            avg(m.open_interest)                AS ls_avg_oi,
            avg(m.last_price)                   AS ls_avg_price
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.last_price >= 3
          AND m.last_price <= 15
    """)

    ls = ls_stats.iloc[0]
    mo.md(
        f"""
        ## Longshot Snapshot Overview

        | Metric | Value |
        |--------|-------|
        | Longshot markets | {int(ls.ls_total):,} |
        | Distinct events | {int(ls.ls_events):,} |
        | Total volume | {int(ls.ls_volume):,} contracts |
        | Avg open interest | {ls.ls_avg_oi:,.1f} |
        | Avg last price (cents) | {ls.ls_avg_price:,.1f} |
        """
    )
    return ()


@app.cell
def longshot_overview_commentary(mo):
    mo.md(
        """
        Longshot markets represent about 9% of all active markets but carry
        substantially higher average open interest (~35k vs ~8k overall),
        indicating strong speculative interest. Their combined volume of 300M+
        contracts is notable given their low price levels.
        """
    )
    return ()


@app.cell
def longshot_dte_dist(alt, mo, query, mkt_date):
    ls_dte_data = query(f"""
        WITH ls_dte AS (
            SELECT
                date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) AS raw_dte
            FROM daily_markets
            WHERE close_time IS NOT NULL
              AND close_time != ''
              AND last_price >= 3
              AND last_price <= 15
              AND date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) >= 1
        )
        SELECT
            LEAST(raw_dte, 29) AS ls_dte_day,
            CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END AS ls_dte_label,
            count(*) AS ls_market_count
        FROM ls_dte
        GROUP BY LEAST(raw_dte, 29),
                 CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END
        ORDER BY 1
    """)

    ls_dte_sort = [str(i) for i in range(1, 29)] + ["29+"]

    ls_dte_chart = (
        alt.Chart(ls_dte_data)
        .mark_bar(color="darkorange")
        .encode(
            alt.X("ls_dte_label:N", sort=ls_dte_sort, title="Days to Expiry"),
            alt.Y("ls_market_count:Q", title="Number of Markets"),
            tooltip=[
                alt.Tooltip("ls_dte_label:N", title="DTE"),
                alt.Tooltip("ls_market_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot Markets by Days to Expiry")
    )

    mo.md("## Longshot Markets — Days to Expiry")
    mo.ui.altair_chart(ls_dte_chart)
    return ()


@app.cell
def longshot_dte_commentary(mo):
    mo.md(
        """
        The longshot DTE profile is strikingly different. The 1-day bin shrinks
        to ~92 markets (vs 22k overall) — short-dated contracts have mostly
        resolved their uncertainty, leaving few in the 3-15 cent band. The 29+
        catch-all dominates with ~4,500 markets (83% of all longshots). Day 7
        and day 14 show modest spikes, mirroring the weekly/biweekly expiry
        structure seen in the overall market.
        """
    )
    return ()


@app.cell
def longshot_dte_volume_dist(alt, mo, query, mkt_date):
    ls_dte_vol_data = query(f"""
        WITH ls_dte_v AS (
            SELECT
                date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) AS raw_dte,
                volume_24h
            FROM daily_markets
            WHERE close_time IS NOT NULL
              AND close_time != ''
              AND last_price >= 3
              AND last_price <= 15
              AND date_diff('day', date('{mkt_date}'), date(from_iso8601_timestamp(close_time))) >= 1
              AND volume_24h IS NOT NULL
        )
        SELECT
            LEAST(raw_dte, 29) AS ls_dte_vol_day,
            CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END AS ls_dte_vol_label,
            sum(volume_24h) AS ls_total_vol_24h
        FROM ls_dte_v
        GROUP BY LEAST(raw_dte, 29),
                 CASE WHEN raw_dte >= 29 THEN '29+' ELSE CAST(raw_dte AS VARCHAR) END
        ORDER BY 1
    """)

    ls_dte_vol_sort = [str(i) for i in range(1, 29)] + ["29+"]

    ls_dte_vol_chart = (
        alt.Chart(ls_dte_vol_data)
        .mark_bar(color="orangered")
        .encode(
            alt.X("ls_dte_vol_label:N", sort=ls_dte_vol_sort, title="Days to Expiry"),
            alt.Y("ls_total_vol_24h:Q", title="24h Trade Volume (contracts)"),
            tooltip=[
                alt.Tooltip("ls_dte_vol_label:N", title="DTE"),
                alt.Tooltip("ls_total_vol_24h:Q", title="24h Volume", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot 24h Trade Volume by Days to Expiry")
    )

    mo.md("## Longshot Markets — 24h Trade Volume by Days to Expiry")
    mo.ui.altair_chart(ls_dte_vol_chart)
    return ()


@app.cell
def longshot_dte_volume_commentary(mo):
    mo.md(
        """
        Among longshots, day 14 again dominates 24h volume (~6M contracts),
        consistent with the overall pattern of heavy biweekly-expiry trading.
        The 29+ bucket follows at ~3.9M, and day 7 shows ~900K — a weekly
        expiry effect. Day 1 longshots trade ~774K in volume despite having
        only 92 markets, indicating high per-market turnover on the shortest-
        dated longshots.
        """
    )
    return ()


@app.cell
def longshot_midprice_dist(alt, mo, query):
    ls_midprice_data = query("""
        WITH ls_priced AS (
            SELECT
                (yes_bid + yes_ask) / 2.0 AS ls_midpoint
            FROM daily_markets
            WHERE yes_bid IS NOT NULL
              AND yes_ask IS NOT NULL
              AND last_price >= 3
              AND last_price <= 15
        )
        SELECT
            CAST(FLOOR(ls_midpoint) AS INTEGER) AS ls_price_bin,
            count(*) AS ls_price_count
        FROM ls_priced
        GROUP BY FLOOR(ls_midpoint)
        ORDER BY 1
    """)

    ls_midprice_chart = (
        alt.Chart(ls_midprice_data)
        .mark_bar(color="darkorange")
        .encode(
            alt.X("ls_price_bin:Q", title="Midpoint Yes Price (cents)", scale=alt.Scale(domain=[0, 20])),
            alt.Y("ls_price_count:Q", title="Number of Markets"),
            tooltip=[
                alt.Tooltip("ls_price_bin:Q", title="Price (cents)"),
                alt.Tooltip("ls_price_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot Markets by Midpoint Yes Price")
    )

    mo.md("## Longshot Markets — Midpoint Yes Price")
    mo.ui.altair_chart(ls_midprice_chart)
    return ()


@app.cell
def longshot_midprice_commentary(mo):
    mo.md(
        """
        Within the longshot band, the midpoint distribution peaks at 2-3 cents
        and declines roughly monotonically through the range. The heaviest
        concentration is in the 1-5 cent zone, where wide bid-ask spreads are
        common (the midpoint may differ from the last trade price, which is why
        some midpoints fall outside the 3-15 cent filter on `last_price`). These
        very low-priced markets are the core of the favorite-longshot bias
        analysis — historically they tend to overestimate the true probability.
        """
    )
    return ()


@app.cell
def longshot_category_dist(alt, mo, query):
    ls_cat_data = query("""
        SELECT
            COALESCE(e.category, 'Unknown') AS ls_category,
            count(*) AS ls_cat_count
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.last_price >= 3
          AND m.last_price <= 15
        GROUP BY COALESCE(e.category, 'Unknown')
        ORDER BY count(*) DESC
    """)

    ls_cat_chart = (
        alt.Chart(ls_cat_data)
        .mark_bar(color="darkorange")
        .encode(
            alt.X("ls_cat_count:Q", title="Number of Markets"),
            alt.Y("ls_category:N", sort="-x", title="Category"),
            tooltip=[
                alt.Tooltip("ls_category:N", title="Category"),
                alt.Tooltip("ls_cat_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot Markets by Category")
    )

    mo.md("## Longshot Markets — Category Distribution")
    mo.ui.altair_chart(ls_cat_chart)
    return ()


@app.cell
def longshot_category_commentary(mo):
    mo.md(
        """
        The category mix shifts dramatically for longshots. Crypto drops from 44%
        of all markets to just 3% of longshots — crypto contracts tend to be
        near-zero or near-100 (deep out/in-the-money strikes), so very few land
        in the 3-15 cent band. Sports takes the lead at 37% of longshots,
        followed by Entertainment (17%) and Politics (16%). These categories have
        multi-runner events (e.g., "Who will win the Super Bowl?") where most
        runners are naturally priced as longshots. Elections similarly contribute
        12% — multi-candidate races produce many low-priced contracts.
        """
    )
    return ()


if __name__ == "__main__":
    app.run()
