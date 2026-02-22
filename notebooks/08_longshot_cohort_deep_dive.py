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
def dd_setup():
    import marimo as mo
    import altair as alt
    import pandas as pd

    from longshot.storage.athena import query

    return alt, mo, pd, query


@app.cell
def dd_partitions(mo, query):
    dd_partition_info = query("""
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

    mkt_part = dd_partition_info[dd_partition_info["tbl"] == "daily_markets"].iloc[0]
    evt_part = dd_partition_info[dd_partition_info["tbl"] == "daily_events"].iloc[0]
    snap_date = mkt_part["latest_date"]
    snap_hour = str(int(mkt_part["latest_hour"]))
    evt_snap_date = evt_part["latest_date"]
    evt_snap_hour = str(int(evt_part["latest_hour"]))

    mo.md(
        f"""
        # Longshot Cohort Deep Dive: One-Week vs Two-Week

        Notebook 07 revealed that day 14 has a massive 24h volume spike (~6M
        contracts for longshots, ~25M overall) despite having far fewer markets
        than day 1, and day 7 shows ~900K in longshot volume — a secondary
        weekly-cycle effect.

        This notebook drills into two cohorts of longshot markets (last price
        3-15 cents):
        - **One-week cohort**: DTE 1-7 days
        - **Two-week cohort**: DTE 8-14 days

        Using the most recent daily snapshots from Athena.

        | Table | Date | Hour (UTC) |
        |-------|------|------------|
        | daily_markets | {snap_date} | {snap_hour} |
        | daily_events | {evt_snap_date} | {evt_snap_hour} |
        """
    )
    return snap_date, snap_hour, evt_snap_date, evt_snap_hour


@app.cell
def dd_cohort_overview(mo, query, snap_date):
    cohort_overview = query(f"""
        WITH cohort_base AS (
            SELECT
                CASE
                    WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort_name,
                m.event_ticker,
                m.volume,
                m.volume_24h,
                m.open_interest,
                m.last_price,
                m.yes_bid,
                m.yes_ask
            FROM daily_markets m
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort_name,
            count(*) AS market_count,
            count(DISTINCT event_ticker) AS distinct_events,
            sum(volume) AS total_volume,
            sum(volume_24h) AS total_volume_24h,
            avg(open_interest) AS avg_oi,
            avg(last_price) AS avg_last_price,
            avg(yes_ask - yes_bid) AS avg_spread
        FROM cohort_base
        WHERE cohort_name IS NOT NULL
        GROUP BY cohort_name
        ORDER BY cohort_name
    """)

    mo.md("## Cohort Overview: One-Week vs Two-Week Longshots")
    mo.ui.table(cohort_overview, label="Cohort Summary")
    return ()


@app.cell
def dd_cohort_commentary(mo):
    mo.md(
        """
        The two cohorts are strikingly different. The **1-week cohort** has more
        markets (509 vs 282) but less than half the 24h volume (2.1M vs 6.0M
        contracts). One-week longshots are cheaper on average (6.8 cents vs 9.5
        cents), suggesting more uncertainty has resolved — prices drift toward
        the extremes as expiry approaches. They also have tighter spreads (5.5
        cents vs 10.0 cents), indicating better liquidity for near-term markets.

        The **2-week cohort** punches well above its weight on volume. Despite
        having 44% fewer markets, it trades 2.8x the 24h volume. Average OI is
        also higher (24K vs 19K), pointing to deeper speculative interest in
        markets with more time remaining. The wider spreads suggest these are
        less efficiently priced — potentially more room for the FLB to manifest.
        """
    )
    return ()


@app.cell
def dd_dte_distribution(alt, mo, query, snap_date):
    dte_granular = query(f"""
        WITH dte_ls AS (
            SELECT
                date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) AS dte_day_val,
                volume_24h
            FROM daily_markets
            WHERE close_time IS NOT NULL
              AND close_time != ''
              AND last_price >= 3
              AND last_price <= 15
              AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 14
        )
        SELECT
            dte_day_val,
            CASE WHEN dte_day_val <= 7 THEN '1-week' ELSE '2-week' END AS dte_cohort,
            count(*) AS dte_mkt_count,
            sum(volume_24h) AS dte_vol_24h
        FROM dte_ls
        GROUP BY dte_day_val, CASE WHEN dte_day_val <= 7 THEN '1-week' ELSE '2-week' END
        ORDER BY dte_day_val
    """)

    dte_granular["dte_day_str"] = dte_granular["dte_day_val"].astype(int).astype(str)
    dte_day_sort = [str(i) for i in range(1, 15)]

    dte_count_chart = (
        alt.Chart(dte_granular)
        .mark_bar()
        .encode(
            alt.X("dte_day_str:N", sort=dte_day_sort, title="Days to Expiry"),
            alt.Y("dte_mkt_count:Q", title="Number of Markets"),
            alt.Color(
                "dte_cohort:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            tooltip=[
                alt.Tooltip("dte_day_str:N", title="DTE"),
                alt.Tooltip("dte_mkt_count:Q", title="Markets", format=","),
                alt.Tooltip("dte_cohort:N", title="Cohort"),
            ],
        )
        .properties(width=700, height=300, title="Longshot Market Count by Day (DTE 1-14)")
    )

    dte_vol_chart = (
        alt.Chart(dte_granular)
        .mark_bar()
        .encode(
            alt.X("dte_day_str:N", sort=dte_day_sort, title="Days to Expiry"),
            alt.Y("dte_vol_24h:Q", title="24h Volume (contracts)"),
            alt.Color(
                "dte_cohort:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            tooltip=[
                alt.Tooltip("dte_day_str:N", title="DTE"),
                alt.Tooltip("dte_vol_24h:Q", title="24h Volume", format=","),
                alt.Tooltip("dte_cohort:N", title="Cohort"),
            ],
        )
        .properties(width=700, height=300, title="Longshot 24h Volume by Day (DTE 1-14)")
    )

    mo.md("## Granular DTE Distribution (Days 1-14)")
    mo.vstack([mo.ui.altair_chart(dte_count_chart), mo.ui.altair_chart(dte_vol_chart)])
    return ()


@app.cell
def dd_dte_commentary(mo):
    mo.md(
        """
        The day-by-day breakdown reveals extreme concentration. In the **1-week
        cohort**, day 7 dominates with 184 markets and 911K volume — a clear
        weekly expiry cycle. Day 1 has 92 markets with 774K volume (high
        per-market turnover on the shortest-dated longshots). Days 2-4 are
        nearly empty.

        In the **2-week cohort**, day 14 is an outlier: 252 markets generating
        6.0M in 24h volume, dwarfing everything else. Days 8-13 are virtually
        dead (3-14 markets each, minimal volume). The volume spike is not
        spread across the two-week window — it's almost entirely concentrated
        on the 14th day, confirming a biweekly settlement cycle effect where
        many active sports markets have close dates exactly two weeks out.
        """
    )
    return ()


@app.cell
def dd_category_count(alt, mo, query, snap_date):
    cat_count_data = query(f"""
        SELECT
            COALESCE(e.category, 'Unknown') AS cat_name_count,
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS cat_cohort_count,
            count(*) AS cat_mkt_count
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        GROUP BY
            COALESCE(e.category, 'Unknown'),
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END
        ORDER BY cat_mkt_count DESC
    """)

    cat_count_chart = (
        alt.Chart(cat_count_data)
        .mark_bar()
        .encode(
            alt.X("cat_mkt_count:Q", title="Number of Markets"),
            alt.Y("cat_name_count:N", sort="-x", title="Category"),
            alt.Color(
                "cat_cohort_count:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            tooltip=[
                alt.Tooltip("cat_name_count:N", title="Category"),
                alt.Tooltip("cat_cohort_count:N", title="Cohort"),
                alt.Tooltip("cat_mkt_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot Markets by Category — Cohort Comparison")
    )

    mo.md("## Category Comparison — Market Count")
    mo.ui.altair_chart(cat_count_chart)
    return ()


@app.cell
def dd_category_volume(alt, mo, query, snap_date):
    cat_vol_data = query(f"""
        SELECT
            COALESCE(e.category, 'Unknown') AS cat_name_vol,
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS cat_cohort_vol,
            sum(m.volume_24h) AS cat_total_vol_24h
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        GROUP BY
            COALESCE(e.category, 'Unknown'),
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END
        ORDER BY cat_total_vol_24h DESC
    """)

    cat_vol_chart = (
        alt.Chart(cat_vol_data)
        .mark_bar()
        .encode(
            alt.X("cat_total_vol_24h:Q", title="24h Volume (contracts)"),
            alt.Y("cat_name_vol:N", sort="-x", title="Category"),
            alt.Color(
                "cat_cohort_vol:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            tooltip=[
                alt.Tooltip("cat_name_vol:N", title="Category"),
                alt.Tooltip("cat_cohort_vol:N", title="Cohort"),
                alt.Tooltip("cat_total_vol_24h:Q", title="24h Volume", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot 24h Volume by Category — Cohort Comparison")
    )

    mo.md("## Category Comparison — 24h Volume")
    mo.ui.altair_chart(cat_vol_chart)
    return ()


@app.cell
def dd_category_commentary(mo):
    mo.md(
        """
        The category split explains the cohort differences. The **2-week cohort
        is almost entirely Sports** — 252 of 282 markets (89%) and 6.0M of 6.0M
        volume (99.8%). No other category has meaningful representation. These
        are multi-runner sporting events (golf tournaments, soccer matches,
        tennis, basketball) where most outcomes are priced as longshots.

        The **1-week cohort is far more diverse**: Climate/Weather leads in
        market count (167 markets, daily weather contracts expiring weekly),
        followed by Crypto (118, strike-level markets) and Entertainment (78).
        But on volume, Politics leads the 1-week cohort (630K) driven by a
        single high-profile market (Khamenei succession, 540K alone). Sports
        contributes 586K in the 1-week cohort — modest compared to 6M in the
        two-week window.

        The volume chart makes the story unmistakable: the day-14 volume spike
        from notebook 07 is a **Sports phenomenon**. The biweekly settlement
        cycle for sports markets creates a structural volume spike that
        dominates the longshot landscape.
        """
    )
    return ()


@app.cell
def dd_top_events_1w(mo, query, snap_date):
    top_events_1w = query(f"""
        SELECT
            m.event_ticker AS evt1w_ticker,
            COALESCE(e.category, 'Unknown') AS evt1w_category,
            e.title AS evt1w_title,
            e.series_ticker AS evt1w_series,
            count(*) AS evt1w_mkt_count,
            sum(m.volume_24h) AS evt1w_vol_24h,
            sum(m.open_interest) AS evt1w_total_oi,
            avg(m.last_price) AS evt1w_avg_price
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7
        GROUP BY m.event_ticker, e.category, e.title, e.series_ticker
        ORDER BY sum(m.volume_24h) DESC
        LIMIT 20
    """)

    mo.md("## Top Events — One-Week Cohort (DTE 1-7)")
    mo.ui.table(top_events_1w, label="Top Events by 24h Volume (1-week)")
    return ()


@app.cell
def dd_top_events_2w(mo, query, snap_date):
    top_events_2w = query(f"""
        SELECT
            m.event_ticker AS evt2w_ticker,
            COALESCE(e.category, 'Unknown') AS evt2w_category,
            e.title AS evt2w_title,
            e.series_ticker AS evt2w_series,
            count(*) AS evt2w_mkt_count,
            sum(m.volume_24h) AS evt2w_vol_24h,
            sum(m.open_interest) AS evt2w_total_oi,
            avg(m.last_price) AS evt2w_avg_price
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14
        GROUP BY m.event_ticker, e.category, e.title, e.series_ticker
        ORDER BY sum(m.volume_24h) DESC
        LIMIT 20
    """)

    mo.md("## Top Events — Two-Week Cohort (DTE 8-14)")
    mo.ui.table(top_events_2w, label="Top Events by 24h Volume (2-week)")
    return ()


@app.cell
def dd_event_commentary(mo):
    mo.md(
        """
        The event-level tables reveal what drives each cohort.

        **One-week**: The top event is a NASCAR race (Autotrader 400, 19 runner
        markets, 583K volume) — a classic multi-outcome sports event with many
        longshot runners. The #2 event by volume is a single political market
        (Khamenei succession, 540K volume in one market) — an outlier driven
        by geopolitical news flow. The remaining top events span weather (NYC
        snow), AI benchmarks, entertainment (Billboard chart), and SOTU
        mentions — a genuinely diverse set.

        **Two-week**: The top 10 events are **ALL Sports**. PGA Genesis
        Invitational dominates with 3.3M volume (55% of all 2-week longshot
        volume) across just 2 longshot markets. EPL (Tottenham vs Arsenal,
        811K), NCAA basketball (652K), and tennis matches follow. These are
        events happening on or near the snapshot date whose formal market close
        times fall on the biweekly cycle. The volume reflects active trading
        around live sporting events, not "two weeks of uncertainty."
        """
    )
    return ()


@app.cell
def dd_top_markets_1w(mo, query, snap_date):
    top_mkts_1w = query(f"""
        SELECT
            m.ticker AS mkt1w_ticker,
            m.title AS mkt1w_title,
            COALESCE(e.category, 'Unknown') AS mkt1w_category,
            m.last_price AS mkt1w_last_price,
            m.yes_bid AS mkt1w_bid,
            m.yes_ask AS mkt1w_ask,
            m.volume_24h AS mkt1w_vol_24h,
            m.open_interest AS mkt1w_oi,
            date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) AS mkt1w_dte
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7
        ORDER BY m.volume_24h DESC
        LIMIT 15
    """)

    mo.md("## Top Individual Markets — One-Week Cohort (DTE 1-7)")
    mo.ui.table(top_mkts_1w, label="Top Markets by 24h Volume (1-week)")
    return ()


@app.cell
def dd_top_markets_2w(mo, query, snap_date):
    top_mkts_2w = query(f"""
        SELECT
            m.ticker AS mkt2w_ticker,
            m.title AS mkt2w_title,
            COALESCE(e.category, 'Unknown') AS mkt2w_category,
            m.last_price AS mkt2w_last_price,
            m.yes_bid AS mkt2w_bid,
            m.yes_ask AS mkt2w_ask,
            m.volume_24h AS mkt2w_vol_24h,
            m.open_interest AS mkt2w_oi,
            date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) AS mkt2w_dte
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14
        ORDER BY m.volume_24h DESC
        LIMIT 15
    """)

    mo.md("## Top Individual Markets — Two-Week Cohort (DTE 8-14)")
    mo.ui.table(top_mkts_2w, label="Top Markets by 24h Volume (2-week)")
    return ()


@app.cell
def dd_market_commentary(mo):
    mo.md(
        """
        At the individual market level, the two cohorts show different profiles.

        **One-week top markets** span multiple categories and event types —
        political succession markets, weather forecasts, AI model rankings,
        and sports runners. The top market by volume (Khamenei, ~540K) is
        a single high-stakes geopolitical contract. Prices cluster in the
        3-9 cent range with tight 1-4 cent spreads.

        **Two-week top markets** are individual sports outcomes — specific
        golfers winning the Genesis Invitational, match outcomes in soccer,
        tennis, and basketball. These are high-volume, liquid markets with
        active order books. The per-market volume is extremely high (top
        market at ~1.9M contracts), reflecting the intensity of sports
        betting activity on Kalshi.
        """
    )
    return ()


@app.cell
def dd_price_distribution(alt, mo, query, snap_date):
    price_dist_data = query(f"""
        SELECT
            CAST(last_price AS INTEGER) AS price_cent_val,
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS price_cohort_name,
            count(*) AS price_bin_count
        FROM daily_markets
        WHERE close_time IS NOT NULL
          AND close_time != ''
          AND last_price >= 3
          AND last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 14
        GROUP BY
            CAST(last_price AS INTEGER),
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END
        ORDER BY 1
    """)

    price_dist_chart = (
        alt.Chart(price_dist_data)
        .mark_bar()
        .encode(
            alt.X("price_cent_val:O", title="Last Price (cents)"),
            alt.Y("price_bin_count:Q", title="Number of Markets"),
            alt.Color(
                "price_cohort_name:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            alt.XOffset("price_cohort_name:N"),
            tooltip=[
                alt.Tooltip("price_cent_val:O", title="Price (cents)"),
                alt.Tooltip("price_cohort_name:N", title="Cohort"),
                alt.Tooltip("price_bin_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Longshot Price Distribution by Cohort")
    )

    mo.md("## Price Distribution by Cohort")
    mo.ui.altair_chart(price_dist_chart)
    return ()


@app.cell
def dd_spread_distribution(alt, mo, query, snap_date):
    spread_dist_data = query(f"""
        SELECT
            CAST(yes_ask - yes_bid AS INTEGER) AS spread_cents_val,
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS spread_cohort_name,
            count(*) AS spread_bin_count
        FROM daily_markets
        WHERE close_time IS NOT NULL
          AND close_time != ''
          AND last_price >= 3
          AND last_price <= 15
          AND yes_bid IS NOT NULL
          AND yes_ask IS NOT NULL
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 14
        GROUP BY
            CAST(yes_ask - yes_bid AS INTEGER),
            CASE
                WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END
        ORDER BY 1
    """)

    spread_dist_chart = (
        alt.Chart(spread_dist_data)
        .mark_bar()
        .encode(
            alt.X("spread_cents_val:O", title="Bid-Ask Spread (cents)"),
            alt.Y("spread_bin_count:Q", title="Number of Markets"),
            alt.Color(
                "spread_cohort_name:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            alt.XOffset("spread_cohort_name:N"),
            tooltip=[
                alt.Tooltip("spread_cents_val:O", title="Spread (cents)"),
                alt.Tooltip("spread_cohort_name:N", title="Cohort"),
                alt.Tooltip("spread_bin_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Bid-Ask Spread Distribution by Cohort")
    )

    mo.md("## Bid-Ask Spread Distribution by Cohort")
    mo.ui.altair_chart(spread_dist_chart)
    return ()


@app.cell
def dd_price_spread_commentary(mo):
    mo.md(
        """
        **Price distribution**: The 1-week cohort is left-skewed, clustering
        heavily at 3-6 cents (57% of markets). This is expected — as expiry
        approaches, uncertainty resolves and longshots drift toward lower
        prices. The 2-week cohort is more evenly distributed across the 3-15
        cent range, with a slight concentration at 11-13 cents. Markets with
        more time to expiry retain higher "residual uncertainty" pricing.

        **Spread distribution**: The 1-week cohort has dramatically tighter
        spreads — 59% of markets have spreads of 1-3 cents, vs the 2-week
        cohort which is more spread out. However, the 2-week cohort has a
        notable cluster at 100-cent spreads (11 markets with bid=0, ask=100
        or similar) — these are effectively no-quote markets where the
        order book has dried up. Despite this, the high-volume 2-week
        sports markets have competitive spreads, suggesting a bimodal
        liquidity profile: the active sports markets are well-traded while
        a tail of illiquid markets sits with wide quotes.
        """
    )
    return ()


@app.cell
def dd_volume_concentration(alt, mo, pd, query, snap_date):
    conc_data = query(f"""
        WITH evt_vol AS (
            SELECT
                m.event_ticker AS conc_event_ticker,
                COALESCE(e.category, 'Unknown') AS conc_category,
                e.title AS conc_title,
                CASE
                    WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    ELSE '2-week'
                END AS conc_cohort,
                sum(m.volume_24h) AS conc_vol_24h
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
            GROUP BY m.event_ticker, e.category, e.title,
                CASE
                    WHEN date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    ELSE '2-week'
                END
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY conc_cohort ORDER BY conc_vol_24h DESC) AS conc_rank,
                SUM(conc_vol_24h) OVER (PARTITION BY conc_cohort) AS conc_cohort_total
            FROM evt_vol
        )
        SELECT
            conc_cohort,
            conc_rank,
            conc_event_ticker,
            conc_category,
            conc_title,
            conc_vol_24h,
            conc_vol_24h * 100.0 / NULLIF(conc_cohort_total, 0) AS conc_vol_pct
        FROM ranked
        WHERE conc_rank <= 20
        ORDER BY conc_cohort, conc_rank
    """)

    # Calculate cumulative volume share per cohort
    conc_1w = conc_data[conc_data["conc_cohort"] == "1-week"].copy()
    conc_2w = conc_data[conc_data["conc_cohort"] == "2-week"].copy()
    conc_1w["conc_cum_pct"] = conc_1w["conc_vol_pct"].cumsum()
    conc_2w["conc_cum_pct"] = conc_2w["conc_vol_pct"].cumsum()
    conc_cumulative = pd.concat([conc_1w, conc_2w], ignore_index=True)

    conc_line_chart = (
        alt.Chart(conc_cumulative)
        .mark_line(point=True)
        .encode(
            alt.X("conc_rank:Q", title="Event Rank (by 24h Volume)", scale=alt.Scale(domain=[1, 20])),
            alt.Y("conc_cum_pct:Q", title="Cumulative Volume Share (%)"),
            alt.Color(
                "conc_cohort:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            tooltip=[
                alt.Tooltip("conc_rank:Q", title="Rank"),
                alt.Tooltip("conc_cohort:N", title="Cohort"),
                alt.Tooltip("conc_title:N", title="Event"),
                alt.Tooltip("conc_vol_24h:Q", title="24h Volume", format=","),
                alt.Tooltip("conc_cum_pct:Q", title="Cumulative %", format=".1f"),
            ],
        )
        .properties(width=700, height=400, title="Volume Concentration: Cumulative Share of Top 20 Events")
    )

    mo.md("## Volume Concentration by Cohort")
    mo.ui.altair_chart(conc_line_chart)
    mo.md("### Top 20 Events per Cohort (ranked by 24h volume)")
    mo.ui.table(
        conc_cumulative[["conc_cohort", "conc_rank", "conc_event_ticker", "conc_category",
                         "conc_title", "conc_vol_24h", "conc_vol_pct", "conc_cum_pct"]],
        label="Volume Concentration",
    )
    return ()


@app.cell
def dd_concentration_commentary(mo):
    mo.md(
        """
        Volume concentration differs sharply between cohorts. The **2-week
        cohort** is extremely top-heavy: the PGA Genesis Invitational alone
        accounts for ~55% of all 24h volume, and the top 3 events (PGA, EPL
        Tottenham-Arsenal, NCAA Pittsburgh-UNC) likely cover ~80%. This means
        the massive day-14 volume spike from notebook 07 is driven by a
        handful of high-profile sporting events, not a broad market phenomenon.

        The **1-week cohort** is more diversified. The top event (NASCAR,
        ~27%) and #2 (Khamenei, ~25%) together account for about half the
        volume, with meaningful contributions from weather, AI, entertainment,
        and politics. No single event dominates the way PGA does in the 2-week
        window.

        **FLB implication**: The concentrated Sports volume in the 2-week
        cohort is high-information, efficiently priced — these are actively
        traded markets with deep order books. The FLB is least likely to
        persist here. The 1-week cohort's diverse, lower-volume markets may
        offer better FLB opportunities but with thinner liquidity.
        """
    )
    return ()


@app.cell
def dd_day14_drilldown(mo, query, snap_date):
    d14_summary = query(f"""
        SELECT
            count(*) AS d14_total_markets,
            count(DISTINCT m.event_ticker) AS d14_distinct_events,
            sum(m.volume_24h) AS d14_total_vol_24h,
            sum(m.open_interest) AS d14_total_oi,
            avg(m.last_price) AS d14_avg_price,
            avg(m.yes_ask - m.yes_bid) AS d14_avg_spread
        FROM daily_markets m
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) = 14
    """)

    d14_events = query(f"""
        SELECT
            m.event_ticker AS d14_evt_ticker,
            COALESCE(e.category, 'Unknown') AS d14_evt_category,
            e.title AS d14_evt_title,
            e.series_ticker AS d14_evt_series,
            count(*) AS d14_evt_mkt_count,
            sum(m.volume_24h) AS d14_evt_vol_24h,
            sum(m.open_interest) AS d14_evt_total_oi,
            avg(m.last_price) AS d14_evt_avg_price
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) = 14
        GROUP BY m.event_ticker, e.category, e.title, e.series_ticker
        ORDER BY sum(m.volume_24h) DESC
        LIMIT 20
    """)

    d14_markets = query(f"""
        SELECT
            m.ticker AS d14_mkt_ticker,
            m.title AS d14_mkt_title,
            COALESCE(e.category, 'Unknown') AS d14_mkt_category,
            m.last_price AS d14_mkt_price,
            m.yes_bid AS d14_mkt_bid,
            m.yes_ask AS d14_mkt_ask,
            m.volume_24h AS d14_mkt_vol_24h,
            m.open_interest AS d14_mkt_oi
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND date_diff('day', date('{snap_date}'), date(from_iso8601_timestamp(m.close_time))) = 14
        ORDER BY m.volume_24h DESC
        LIMIT 15
    """)

    d14s = d14_summary.iloc[0]
    mo.md(
        f"""
        ## Day-14 Specific Drilldown

        DTE = 14 is the headline finding from notebook 07 — the biggest
        single-day volume spike for longshot markets. Here's what's behind it.

        | Metric | Value |
        |--------|-------|
        | Total longshot markets (DTE=14) | {int(d14s.d14_total_markets):,} |
        | Distinct events | {int(d14s.d14_distinct_events):,} |
        | Total 24h volume | {int(d14s.d14_total_vol_24h):,} contracts |
        | Total open interest | {int(d14s.d14_total_oi):,} |
        | Avg last price | {d14s.d14_avg_price:.1f} cents |
        | Avg bid-ask spread | {d14s.d14_avg_spread:.1f} cents |
        """
    )

    mo.md("### Top Events (DTE=14)")
    mo.ui.table(d14_events, label="Day-14 Events")
    mo.md("### Top Individual Markets (DTE=14)")
    mo.ui.table(d14_markets, label="Day-14 Markets")
    return ()


@app.cell
def dd_day14_commentary(mo):
    mo.md(
        """
        The day-14 drilldown confirms the structural explanation. All 252
        longshot markets at DTE=14 span 126 distinct events, overwhelmingly
        Sports. The 6.0M 24h volume is concentrated in events happening
        **today or this week** — PGA golf, EPL soccer, NCAA basketball, ATP
        tennis, CS2 esports — whose formal Kalshi market close dates fall on
        the biweekly settlement cycle, 14 days from the snapshot.

        This is the key finding: **the day-14 volume spike is not about
        markets with 14 days of remaining uncertainty**. It's about actively
        traded sports markets for imminent/live events that happen to close
        on the biweekly cycle. The "longshot" pricing reflects multi-runner
        event structure (most golfers/teams are priced as longshots) rather
        than deep uncertainty about the outcome.

        For FLB strategy purposes, these DTE=14 sports longshots are likely
        the **worst** candidates — they're high-volume, high-information
        markets where prices efficiently reflect probabilities. The FLB
        edge, if any, would be found in the lower-volume, diverse markets
        of the 1-week cohort and the long-dated (29+) bucket from
        notebook 07.
        """
    )
    return ()


@app.cell
def dd_overall_findings(mo):
    mo.md(
        """
        ---

        ## Overall Findings

        1. **The two cohorts are fundamentally different.** The 1-week cohort
           (509 markets, DTE 1-7) is diverse in category — weather, crypto,
           politics, entertainment, and some sports. The 2-week cohort (282
           markets, DTE 8-14) is 89% Sports by count and 99.8% by volume.

        2. **The day-14 volume spike is a Sports/settlement-cycle artifact.**
           252 longshot markets at DTE=14 generate 6.0M contracts in 24h
           volume — but these are live/imminent sporting events (PGA golf,
           EPL soccer, NCAA basketball) whose formal close dates happen to
           fall on the biweekly settlement cycle. The PGA Genesis
           Invitational alone accounts for ~55% of two-week cohort volume.

        3. **Volume concentration is extreme in the 2-week cohort.** The top
           3 events cover ~80% of 24h volume. In contrast, the 1-week cohort
           volume is spread across NASCAR, geopolitical (Khamenei), weather,
           AI benchmarks, entertainment, and political markets.

        4. **Price and spread profiles confirm the structural difference.**
           One-week longshots are cheaper (avg 6.8 vs 9.5 cents) and tighter
           (avg spread 5.5 vs 10.0 cents). Markets closer to expiry have
           resolved more uncertainty, shifting prices toward the extremes and
           attracting tighter quotes.

        5. **FLB strategy implications.** The high-volume day-14 sports
           longshots are likely the worst FLB candidates — these are actively
           traded, high-information markets where prices efficiently reflect
           probabilities. FLB edge is more likely to persist in the diverse,
           lower-volume markets of the 1-week cohort and especially in the
           long-dated (29+ day) bucket that dominates the longshot market
           count. The core tension remains: the markets with the most FLB
           mispricing are the ones with the least liquidity to execute.
        """
    )
    return ()


if __name__ == "__main__":
    app.run()
