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
def executive_summary(mo):
    mo.md(
        """
        # Longshot Deep Dive: One-Week vs Two-Week Markets

        ## Key Findings

        This analysis examines the two most active short-dated longshot cohorts on
        Kalshi — markets priced at 3-15 cents that expire within one week (DTE 1-7)
        or two weeks (DTE 8-14). These cohorts are structurally different in almost
        every dimension:

        **The two-week cohort is a Sports monoculture.** 89% of its 282 markets are
        Sports, and nearly all of them sit at exactly DTE 14. The day-14 spike that
        dominates the overall volume charts is driven almost entirely by game-day
        sports betting — NBA spreads and point totals, college basketball games,
        European football, tennis matches, and golf tournaments. The Genesis
        Invitational (PGA) alone generated 3.3M contracts in 24h volume on its
        longshot runners. These markets have very high per-market open interest
        (avg ~24k) and a turnover ratio near 0.91 — meaning nearly all outstanding
        positions are turning over daily.

        **The one-week cohort is categorically diverse.** Its 509 markets span 11
        categories led by Climate and Weather (33%), Crypto (23%), and Entertainment
        (15%). Volume leadership is even more dispersed: Politics leads (630K) on
        the strength of single high-conviction events like Ali Khamenei leaving
        office (539K volume alone), followed by Sports (NASCAR) and Climate (NYC
        snow). The one-week cohort has lower average OI (~19k) but broader
        speculative interest across many event types.

        **Price distributions reveal different market structures.** One-week
        longshots cluster at the bottom of the price range (3-5 cents), consistent
        with markets approaching expiry where most outcomes have been priced out.
        Two-week longshots are more uniformly distributed across 3-15 cents, with a
        slight concentration at the upper end (11-15 cents) — these are newly listed
        game-day markets where uncertainty is still genuine.

        **For the longshot bias project**, the two cohorts offer distinct testing
        grounds. The two-week sports markets provide high-volume, high-turnover
        environments ideal for measuring whether longshots are systematically
        overpriced in real-time sports betting. The one-week cohort's diversity
        makes it better for testing whether the bias varies across event types —
        weather, politics, crypto, and entertainment may each exhibit different
        mispricings.
        """
    )
    return ()


@app.cell
def setup():
    import marimo as mo
    import altair as alt
    import pandas as pd

    from longshot.storage.athena import query

    return alt, mo, pd, query


@app.cell
def find_partitions(mo, query):
    partition_info_08 = query("""
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

    mkt_row_08 = partition_info_08[partition_info_08["tbl"] == "daily_markets"].iloc[0]
    evt_row_08 = partition_info_08[partition_info_08["tbl"] == "daily_events"].iloc[0]
    snapshot_date = mkt_row_08["latest_date"]
    snapshot_mkt_hour = str(int(mkt_row_08["latest_hour"]))
    snapshot_evt_date = evt_row_08["latest_date"]
    snapshot_evt_hour = str(int(evt_row_08["latest_hour"]))

    mo.md(
        f"""
        ---

        ## Data Source

        Using the most recent daily snapshots from Athena. Longshot defined as
        last traded price between 3 and 15 cents (inclusive).

        | Table | Date | Hour (UTC) |
        |-------|------|------------|
        | daily_markets | {snapshot_date} | {snapshot_mkt_hour} |
        | daily_events | {snapshot_evt_date} | {snapshot_evt_hour} |
        """
    )
    return snapshot_date, snapshot_mkt_hour, snapshot_evt_date, snapshot_evt_hour


@app.cell
def cohort_overview(mo, query, snapshot_date):
    cohort_stats = query(f"""
        WITH base AS (
            SELECT
                m.ticker,
                m.event_ticker,
                m.last_price,
                m.volume,
                m.volume_24h,
                m.open_interest,
                m.yes_bid,
                m.yes_ask,
                COALESCE(e.category, 'Unknown') AS category,
                e.title AS event_title,
                date_diff('day', date('{snapshot_date}'),
                           date(from_iso8601_timestamp(m.close_time))) AS dte
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
        )
        SELECT
            CASE
                WHEN dte BETWEEN 1 AND 7 THEN '1-week'
                WHEN dte BETWEEN 8 AND 14 THEN '2-week'
                ELSE 'other'
            END AS cohort,
            count(*) AS market_count,
            count(DISTINCT event_ticker) AS event_count,
            sum(volume) AS total_volume,
            sum(volume_24h) AS total_volume_24h,
            avg(open_interest) AS avg_oi,
            avg(last_price) AS avg_last_price,
            avg((yes_bid + yes_ask) / 2.0) AS avg_midpoint
        FROM base
        GROUP BY
            CASE
                WHEN dte BETWEEN 1 AND 7 THEN '1-week'
                WHEN dte BETWEEN 8 AND 14 THEN '2-week'
                ELSE 'other'
            END
        ORDER BY 1
    """)

    one_wk = cohort_stats[cohort_stats["cohort"] == "1-week"]
    two_wk = cohort_stats[cohort_stats["cohort"] == "2-week"]

    def fmt_row(row):
        r = row.iloc[0]
        return (
            f"| Markets | {int(r.market_count):,} |\n"
            f"| Distinct events | {int(r.event_count):,} |\n"
            f"| Total volume (all-time) | {int(r.total_volume):,} |\n"
            f"| 24h volume | {int(r.total_volume_24h):,} |\n"
            f"| Avg open interest | {r.avg_oi:,.1f} |\n"
            f"| Avg last price (cents) | {r.avg_last_price:,.1f} |\n"
            f"| Avg midpoint (cents) | {r.avg_midpoint:,.1f} |"
        )

    one_wk_tbl = fmt_row(one_wk) if len(one_wk) > 0 else "| No data | — |"
    two_wk_tbl = fmt_row(two_wk) if len(two_wk) > 0 else "| No data | — |"

    mo.md(
        f"""
        ## Cohort Overview

        ### One-Week Longshots (DTE 1-7)

        | Metric | Value |
        |--------|-------|
        {one_wk_tbl}

        ### Two-Week Longshots (DTE 8-14)

        | Metric | Value |
        |--------|-------|
        {two_wk_tbl}
        """
    )
    return cohort_stats


@app.cell
def cohort_overview_commentary(mo):
    mo.md(
        """
        The one-week cohort has nearly twice as many markets (509 vs 282) spread
        across more events (172 vs 137), but the two-week cohort generates
        roughly 3x the 24h volume (6.0M vs 2.1M contracts). Two-week markets
        also carry higher average OI (24k vs 19k) and trade at a higher average
        price (9.5c vs 6.8c), suggesting they still have meaningful uncertainty.
        The one-week cohort's lower average price reflects markets approaching
        resolution where most outcomes have been largely priced out.
        """
    )
    return ()


@app.cell
def dte_granular(alt, mo, query, snapshot_date):
    dte_granular_data = query(f"""
        WITH base AS (
            SELECT
                date_diff('day', date('{snapshot_date}'),
                           date(from_iso8601_timestamp(close_time))) AS dte
            FROM daily_markets
            WHERE last_price >= 3
              AND last_price <= 15
              AND close_time IS NOT NULL
              AND close_time != ''
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 14
        )
        SELECT
            dte AS dte_day_granular,
            CASE
                WHEN dte BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS dte_cohort_label,
            count(*) AS dte_mkt_count
        FROM base
        GROUP BY dte,
            CASE WHEN dte BETWEEN 1 AND 7 THEN '1-week' ELSE '2-week' END
        ORDER BY dte
    """)

    dte_granular_chart = (
        alt.Chart(dte_granular_data)
        .mark_bar()
        .encode(
            alt.X("dte_day_granular:O", title="Days to Expiry"),
            alt.Y("dte_mkt_count:Q", title="Number of Longshot Markets"),
            alt.Color("dte_cohort_label:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            tooltip=[
                alt.Tooltip("dte_day_granular:O", title="DTE"),
                alt.Tooltip("dte_cohort_label:N", title="Cohort"),
                alt.Tooltip("dte_mkt_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400,
                     title="Longshot Market Count by Day (DTE 1-14)")
    )

    mo.md("## Granular DTE Distribution (Days 1-14)")
    mo.ui.altair_chart(dte_granular_chart)
    return ()


@app.cell
def dte_granular_commentary(mo):
    mo.md(
        """
        Within the 1-14 day window, market count concentrates at three points:
        day 7 (184 markets), day 14 (252), and day 1 (92). Days 3-4 and 8-13 are
        almost empty (single digits). This confirms that Kalshi's contract
        structure produces longshots primarily at weekly and biweekly expiry
        boundaries, not uniformly across the calendar. Day 5 shows a modest
        secondary bump (105 markets), likely from mid-week event listings.
        """
    )
    return ()


@app.cell
def dte_volume_granular(alt, mo, query, snapshot_date):
    dte_vol_granular_data = query(f"""
        WITH base AS (
            SELECT
                date_diff('day', date('{snapshot_date}'),
                           date(from_iso8601_timestamp(close_time))) AS dte,
                volume_24h
            FROM daily_markets
            WHERE last_price >= 3
              AND last_price <= 15
              AND close_time IS NOT NULL
              AND close_time != ''
              AND volume_24h IS NOT NULL
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(close_time))) BETWEEN 1 AND 14
        )
        SELECT
            dte AS dte_vol_day_granular,
            CASE
                WHEN dte BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS dte_vol_cohort_label,
            sum(volume_24h) AS dte_vol_24h
        FROM base
        GROUP BY dte,
            CASE WHEN dte BETWEEN 1 AND 7 THEN '1-week' ELSE '2-week' END
        ORDER BY dte
    """)

    dte_vol_granular_chart = (
        alt.Chart(dte_vol_granular_data)
        .mark_bar()
        .encode(
            alt.X("dte_vol_day_granular:O", title="Days to Expiry"),
            alt.Y("dte_vol_24h:Q", title="24h Volume (contracts)"),
            alt.Color("dte_vol_cohort_label:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            tooltip=[
                alt.Tooltip("dte_vol_day_granular:O", title="DTE"),
                alt.Tooltip("dte_vol_cohort_label:N", title="Cohort"),
                alt.Tooltip("dte_vol_24h:Q", title="24h Volume", format=","),
            ],
        )
        .properties(width=700, height=400,
                     title="Longshot 24h Volume by Day (DTE 1-14)")
    )

    mo.md("## Granular 24h Volume Distribution (Days 1-14)")
    mo.ui.altair_chart(dte_vol_granular_chart)
    return ()


@app.cell
def dte_volume_commentary(mo):
    mo.md(
        """
        The volume story is even more concentrated. Day 14 accounts for 6.0M of
        the combined 8.2M in 24h volume across both cohorts — roughly 74% of all
        short-dated longshot trading. Day 7 is a distant second (911K), followed
        by day 1 (774K). The per-market volume at day 14 is ~24K contracts/market,
        compared to ~5K at day 7 and ~8.4K at day 1. Mid-range days (2-6, 8-13)
        barely register, confirming that the biweekly expiry cycle is the
        dominant liquidity event for longshot markets.
        """
    )
    return ()


@app.cell
def category_comparison(alt, mo, query, snapshot_date):
    cat_comp_data = query(f"""
        WITH base AS (
            SELECT
                COALESCE(e.category, 'Unknown') AS category,
                CASE
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort AS cat_cohort,
            category AS cat_name,
            count(*) AS cat_mkt_count
        FROM base
        WHERE cohort IS NOT NULL
        GROUP BY cohort, category
        ORDER BY cohort, count(*) DESC
    """)

    cat_comp_chart = (
        alt.Chart(cat_comp_data)
        .mark_bar()
        .encode(
            alt.X("cat_mkt_count:Q", title="Number of Markets"),
            alt.Y("cat_name:N", sort="-x", title="Category"),
            alt.Color("cat_cohort:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            alt.Row("cat_cohort:N", title="Cohort"),
            tooltip=[
                alt.Tooltip("cat_name:N", title="Category"),
                alt.Tooltip("cat_cohort:N", title="Cohort"),
                alt.Tooltip("cat_mkt_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=600, height=250,
                     title="Longshot Category Distribution by Cohort")
    )

    mo.md("## Category Distribution: 1-Week vs 2-Week Longshots")
    mo.ui.altair_chart(cat_comp_chart)
    return cat_comp_data


@app.cell
def category_commentary(mo):
    mo.md(
        """
        The category composition is starkly different between cohorts. The
        one-week cohort is led by Climate and Weather (167 markets, 33%) — these
        are daily/near-daily weather observation contracts (temperature highs,
        precipitation, snow events) with naturally short horizons. Crypto follows
        (118, 23%) with daily price range and strike-level contracts. Entertainment
        (78) and Politics (63) round out the top four.

        The two-week cohort is almost entirely Sports (252/282 = 89%). The
        remaining categories have at most 10 markets each. This reflects Kalshi's
        sports market structure: game-day betting markets are listed with a
        biweekly close time, creating a batch of contracts that all expire at
        exactly DTE 14.
        """
    )
    return ()


@app.cell
def category_volume_comparison(alt, mo, query, snapshot_date):
    cat_vol_comp_data = query(f"""
        WITH base AS (
            SELECT
                COALESCE(e.category, 'Unknown') AS category,
                m.volume_24h,
                CASE
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.volume_24h IS NOT NULL
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort AS cat_vol_cohort,
            category AS cat_vol_name,
            sum(volume_24h) AS cat_vol_24h,
            count(*) AS cat_vol_mkts
        FROM base
        WHERE cohort IS NOT NULL
        GROUP BY cohort, category
        ORDER BY cohort, sum(volume_24h) DESC
    """)

    cat_vol_comp_chart = (
        alt.Chart(cat_vol_comp_data)
        .mark_bar()
        .encode(
            alt.X("cat_vol_24h:Q", title="24h Volume (contracts)"),
            alt.Y("cat_vol_name:N", sort="-x", title="Category"),
            alt.Color("cat_vol_cohort:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            alt.Row("cat_vol_cohort:N", title="Cohort"),
            tooltip=[
                alt.Tooltip("cat_vol_name:N", title="Category"),
                alt.Tooltip("cat_vol_cohort:N", title="Cohort"),
                alt.Tooltip("cat_vol_24h:Q", title="24h Volume", format=","),
                alt.Tooltip("cat_vol_mkts:Q", title="Markets", format=","),
            ],
        )
        .properties(width=600, height=250,
                     title="Longshot 24h Volume by Category and Cohort")
    )

    mo.md("## 24h Volume by Category: 1-Week vs 2-Week Longshots")
    mo.ui.altair_chart(cat_vol_comp_chart)
    return cat_vol_comp_data


@app.cell
def volume_category_commentary(mo):
    mo.md(
        """
        Volume concentration is even more extreme than market count suggests.
        In the two-week cohort, Sports accounts for 6.03M of 6.04M total 24h
        volume — effectively 99.8%. All other categories combined trade fewer
        than 15K contracts.

        The one-week cohort is more balanced: Politics leads with 630K (driven
        by a single high-profile event — Ali Khamenei leaving office, at 540K),
        followed by Sports at 586K (NASCAR Autotrader 400 race), then Climate
        at 432K (NYC snow and LA temperature markets). This diversity means
        one-week longshot volume is not dependent on any single category or
        contract structure.
        """
    )
    return ()


@app.cell
def top_events_1wk(mo, query, snapshot_date):
    top_events_1wk_data = query(f"""
        WITH base AS (
            SELECT
                m.event_ticker,
                e.title AS event_title,
                COALESCE(e.category, 'Unknown') AS category,
                m.volume_24h,
                m.open_interest,
                m.last_price
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7
        )
        SELECT
            event_ticker AS evt_1wk_ticker,
            event_title AS evt_1wk_title,
            category AS evt_1wk_category,
            count(*) AS evt_1wk_markets,
            sum(volume_24h) AS evt_1wk_vol_24h,
            avg(open_interest) AS evt_1wk_avg_oi,
            avg(last_price) AS evt_1wk_avg_price
        FROM base
        GROUP BY event_ticker, event_title, category
        ORDER BY count(*) DESC
        LIMIT 20
    """)

    mo.md("## Top One-Week Longshot Events (by Market Count)")
    mo.ui.table(top_events_1wk_data)
    return top_events_1wk_data


@app.cell
def top_events_1wk_commentary(mo):
    mo.md(
        """
        The one-week event landscape is remarkably varied. By market count, crypto
        price range events dominate — BTC, ETH, SOL, and XRP daily price ranges
        each produce 7-20 longshot contracts per event (out-of-the-money strikes).
        The NASCAR Autotrader 400 (19 markets) is the largest single sporting event.
        Entertainment and Politics events round out the list: Billboard chart
        predictions, Spotify streaming counts, State of the Union attendance, and
        Trump meeting/mention markets.

        Notably, many of these events have low-to-moderate open interest per market
        (1-5K), except for politically charged events (SOTU attendance at ~28K
        avg OI, Ali Khamenei at 3.85M OI) which attract outsized speculative
        positions.
        """
    )
    return ()


@app.cell
def top_events_2wk(mo, query, snapshot_date):
    top_events_2wk_data = query(f"""
        WITH base AS (
            SELECT
                m.event_ticker,
                e.title AS event_title,
                COALESCE(e.category, 'Unknown') AS category,
                m.volume_24h,
                m.open_interest,
                m.last_price
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14
        )
        SELECT
            event_ticker AS evt_2wk_ticker,
            event_title AS evt_2wk_title,
            category AS evt_2wk_category,
            count(*) AS evt_2wk_markets,
            sum(volume_24h) AS evt_2wk_vol_24h,
            avg(open_interest) AS evt_2wk_avg_oi,
            avg(last_price) AS evt_2wk_avg_price
        FROM base
        GROUP BY event_ticker, event_title, category
        ORDER BY count(*) DESC
        LIMIT 20
    """)

    mo.md("## Top Two-Week Longshot Events (by Market Count)")
    mo.ui.table(top_events_2wk_data)
    return top_events_2wk_data


@app.cell
def top_events_2wk_commentary(mo):
    mo.md(
        """
        The two-week events read like a sports betting slate. The top events by
        market count are: LPGA Honda Thailand (14 longshot runner markets),
        Arsenal at Tottenham goalscorer markets (12), Dallas at Indiana spread
        markets (12), and various NBA point/spread events (5-8 markets each).

        This is a classic multi-runner structure: a golf tournament produces
        longshot markets for every golfer who isn't the favorite, a soccer match
        produces goalscorer markets for each player, and NBA spread/total markets
        produce longshots at the extreme ends of the distribution.
        """
    )
    return ()


@app.cell
def top_vol_events_1wk(mo, query, snapshot_date):
    top_vol_1wk_data = query(f"""
        WITH base AS (
            SELECT
                m.event_ticker,
                e.title AS event_title,
                COALESCE(e.category, 'Unknown') AS category,
                m.volume_24h,
                m.open_interest
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.volume_24h IS NOT NULL
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7
        )
        SELECT
            event_ticker AS vol_1wk_ticker,
            event_title AS vol_1wk_title,
            category AS vol_1wk_category,
            count(*) AS vol_1wk_markets,
            sum(volume_24h) AS vol_1wk_vol_24h,
            avg(open_interest) AS vol_1wk_avg_oi
        FROM base
        GROUP BY event_ticker, event_title, category
        ORDER BY sum(volume_24h) DESC
        LIMIT 15
    """)

    mo.md("## Top One-Week Events by 24h Volume")
    mo.ui.table(top_vol_1wk_data)
    return top_vol_1wk_data


@app.cell
def top_vol_1wk_commentary(mo):
    mo.md(
        """
        The highest-volume one-week longshot events are dominated by a few
        high-conviction, single-market bets:

        1. **NASCAR Autotrader 400 Winner** (583K) — 19 driver markets, classic
           race-winner longshot structure.
        2. **Ali Khamenei out as Supreme Leader** (540K) — a single binary market
           with 3.85M open interest, by far the highest OI of any longshot. This
           is a geopolitical tail-risk bet.
        3. **NYC Snow in Feb 2026** (120K) — weather event with genuine near-term
           uncertainty.
        4. **Best AI in Feb 2026** (102K) — technology ranking market.

        The one-week cohort's volume is driven by idiosyncratic, high-attention
        events rather than systematic contract structures.
        """
    )
    return ()


@app.cell
def top_vol_events_2wk(mo, query, snapshot_date):
    top_vol_2wk_data = query(f"""
        WITH base AS (
            SELECT
                m.event_ticker,
                e.title AS event_title,
                COALESCE(e.category, 'Unknown') AS category,
                m.volume_24h,
                m.open_interest
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.volume_24h IS NOT NULL
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14
        )
        SELECT
            event_ticker AS vol_2wk_ticker,
            event_title AS vol_2wk_title,
            category AS vol_2wk_category,
            count(*) AS vol_2wk_markets,
            sum(volume_24h) AS vol_2wk_vol_24h,
            avg(open_interest) AS vol_2wk_avg_oi
        FROM base
        GROUP BY event_ticker, event_title, category
        ORDER BY sum(volume_24h) DESC
        LIMIT 15
    """)

    mo.md("## Top Two-Week Events by 24h Volume")
    mo.ui.table(top_vol_2wk_data)
    return top_vol_2wk_data


@app.cell
def top_vol_2wk_commentary(mo):
    mo.md(
        """
        Volume in the two-week cohort is dominated by a few massive sporting
        events:

        1. **PGA Genesis Invitational Winner** (3.34M) — golf tournament winner
           markets with enormous liquidity. Two longshot runners (e.g., Aldrich
           Potgieter at 3c, Rory McIlroy at 14c) account for most of it.
        2. **Tottenham vs Arsenal** (811K) — Premier League game winner.
        3. **Pittsburgh at North Carolina** (652K) — women's college basketball.
        4. **ATP Tennis matches** (211K-185K) — qualification round matches.

        The top 3 events alone account for ~4.8M of the 6.0M total two-week
        volume. This extreme concentration means the day-14 volume spike in the
        overall market analysis is essentially a handful of major sporting events
        generating enormous longshot trading activity.
        """
    )
    return ()


@app.cell
def sports_type_breakdown(mo, query, snapshot_date):
    sports_type_data = query(f"""
        WITH base AS (
            SELECT
                m.event_ticker,
                m.volume_24h,
                m.open_interest,
                CASE
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND COALESCE(e.category, 'Unknown') = 'Sports'
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort AS sports_cohort,
            CASE
                WHEN event_ticker LIKE '%SPREAD%' THEN 'Spread'
                WHEN event_ticker LIKE '%TOTAL%' THEN 'Total Points'
                WHEN event_ticker LIKE '%PTS%' THEN 'Player Points'
                WHEN event_ticker LIKE '%3PT%' THEN 'Three Pointers'
                WHEN event_ticker LIKE '%GOAL%' THEN 'Goalscorer'
                WHEN event_ticker LIKE '%GAME%' THEN 'Game Winner'
                WHEN event_ticker LIKE '%RACE%' THEN 'Race Winner'
                WHEN event_ticker LIKE '%MATCH%' THEN 'Match Winner'
                WHEN event_ticker LIKE '%H2H%' THEN 'Head to Head'
                WHEN event_ticker LIKE '%TOUR%' THEN 'Tournament Winner'
                ELSE 'Other Sports'
            END AS sports_type,
            count(*) AS sports_mkt_count,
            sum(volume_24h) AS sports_vol_24h,
            sum(open_interest) AS sports_oi
        FROM base
        WHERE cohort IS NOT NULL
        GROUP BY cohort,
            CASE
                WHEN event_ticker LIKE '%SPREAD%' THEN 'Spread'
                WHEN event_ticker LIKE '%TOTAL%' THEN 'Total Points'
                WHEN event_ticker LIKE '%PTS%' THEN 'Player Points'
                WHEN event_ticker LIKE '%3PT%' THEN 'Three Pointers'
                WHEN event_ticker LIKE '%GOAL%' THEN 'Goalscorer'
                WHEN event_ticker LIKE '%GAME%' THEN 'Game Winner'
                WHEN event_ticker LIKE '%RACE%' THEN 'Race Winner'
                WHEN event_ticker LIKE '%MATCH%' THEN 'Match Winner'
                WHEN event_ticker LIKE '%H2H%' THEN 'Head to Head'
                WHEN event_ticker LIKE '%TOUR%' THEN 'Tournament Winner'
                ELSE 'Other Sports'
            END
        ORDER BY cohort, count(*) DESC
    """)

    mo.md("## Sports Market Type Breakdown")
    mo.ui.table(sports_type_data)
    return sports_type_data


@app.cell
def sports_type_commentary(mo):
    mo.md(
        """
        The two-week sports markets reveal a rich structure of bet types:

        - **Spread bets** lead by market count (87) but have modest volume (54K)
          — many markets, but most are at extreme ends where liquidity is thin.
        - **Game Winner** markets (34) generate nearly 2M in volume — these are
          the core "who wins" bets for basketball, soccer, and other team sports.
        - **Tournament Winner** markets (16) are the biggest volume generators at
          3.3M — driven almost entirely by the PGA Genesis Invitational.
        - **Match Winner** (14 markets, 477K vol) captures tennis and esports.
        - **Total Points**, **Player Points**, **Three Pointers**, and
          **Goalscorer** markets fill out the portfolio of prop-bet-style
          longshots.

        The one-week sports cohort is much simpler: 19 of 27 markets are
        NASCAR race winner markets (583K volume), with just 6 game winners and
        2 other markets.
        """
    )
    return ()


@app.cell
def price_dist_comparison(alt, mo, query, snapshot_date):
    price_comp_data = query(f"""
        WITH base AS (
            SELECT
                m.last_price,
                CASE
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort
            FROM daily_markets m
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort AS price_cohort,
            CAST(last_price AS INTEGER) AS price_cent,
            count(*) AS price_mkt_count
        FROM base
        WHERE cohort IS NOT NULL
        GROUP BY cohort, CAST(last_price AS INTEGER)
        ORDER BY cohort, CAST(last_price AS INTEGER)
    """)

    price_comp_chart = (
        alt.Chart(price_comp_data)
        .mark_bar(opacity=0.7)
        .encode(
            alt.X("price_cent:O", title="Last Price (cents)"),
            alt.Y("price_mkt_count:Q", title="Number of Markets"),
            alt.Color("price_cohort:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            alt.XOffset("price_cohort:N"),
            tooltip=[
                alt.Tooltip("price_cent:O", title="Price (cents)"),
                alt.Tooltip("price_cohort:N", title="Cohort"),
                alt.Tooltip("price_mkt_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400,
                     title="Last Price Distribution: 1-Week vs 2-Week Longshots")
    )

    mo.md("## Price Distribution Comparison")
    mo.ui.altair_chart(price_comp_chart)
    return ()


@app.cell
def price_commentary(mo):
    mo.md(
        """
        The price distributions are structurally different:

        **One-week markets** have a clear left skew: 98 markets at 3 cents,
        declining to 12-20 by 14-15 cents. This is the expected shape for
        near-expiry longshots — most outcomes are almost certainly not happening,
        so prices cluster at the floor of the range.

        **Two-week markets** have a flatter, slightly right-skewed distribution:
        roughly 15-25 markets at each price point, with a modest peak at 13 cents
        (32 markets) and 15 cents (28 markets). The higher median price reflects
        that these are newly listed game-day markets where genuine uncertainty
        remains — a 10-15% implied probability for an underdog or extreme spread
        outcome is reasonable two weeks before resolution.
        """
    )
    return ()


@app.cell
def spread_analysis(alt, mo, query, snapshot_date):
    spread_data = query(f"""
        WITH base AS (
            SELECT
                m.yes_ask - m.yes_bid AS spread,
                CASE
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort
            FROM daily_markets m
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.yes_bid IS NOT NULL
              AND m.yes_ask IS NOT NULL
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort AS spread_cohort,
            CAST(spread AS INTEGER) AS spread_cents,
            count(*) AS spread_mkt_count
        FROM base
        WHERE cohort IS NOT NULL
        GROUP BY cohort, CAST(spread AS INTEGER)
        ORDER BY cohort, CAST(spread AS INTEGER)
    """)

    spread_chart = (
        alt.Chart(spread_data[spread_data["spread_cents"] <= 20])
        .mark_bar(opacity=0.7)
        .encode(
            alt.X("spread_cents:O", title="Bid-Ask Spread (cents)"),
            alt.Y("spread_mkt_count:Q", title="Number of Markets"),
            alt.Color("spread_cohort:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            alt.XOffset("spread_cohort:N"),
            tooltip=[
                alt.Tooltip("spread_cents:O", title="Spread (cents)"),
                alt.Tooltip("spread_cohort:N", title="Cohort"),
                alt.Tooltip("spread_mkt_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400,
                     title="Bid-Ask Spread: 1-Week vs 2-Week Longshots (spreads <= 20c)")
    )

    mo.md("## Bid-Ask Spread Comparison")
    mo.ui.altair_chart(spread_chart)
    return ()


@app.cell
def spread_commentary(mo):
    mo.md(
        """
        Both cohorts have most markets within tight 1-7 cent spreads, suggesting
        reasonable liquidity for longshot contracts. The one-week cohort has a
        sharper peak at 1-cent spread (152 markets) — these are the most liquid
        near-expiry longshots. The two-week cohort is more evenly distributed
        across 1-7 cent spreads.

        A notable tail exists: 11 two-week markets show a 100-cent spread (bid=0,
        ask=100), indicating completely illiquid contracts with no real market.
        Some one-week markets also show extreme spreads (76-99 cents). These
        outliers are likely stale or abandoned markets that should be excluded
        from any systematic bias analysis.
        """
    )
    return ()


@app.cell
def oi_concentration(alt, mo, query, snapshot_date):
    oi_data = query(f"""
        WITH base AS (
            SELECT
                COALESCE(e.category, 'Unknown') AS category,
                m.open_interest,
                m.volume_24h,
                m.last_price,
                CASE
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{snapshot_date}'),
                                    date(from_iso8601_timestamp(m.close_time)))
                         BETWEEN 8 AND 14 THEN '2-week'
                END AS cohort
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.last_price >= 3
              AND m.last_price <= 15
              AND m.close_time IS NOT NULL
              AND m.close_time != ''
              AND date_diff('day', date('{snapshot_date}'),
                             date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cohort AS oi_cohort,
            category AS oi_category,
            sum(open_interest) AS oi_total,
            avg(open_interest) AS oi_avg,
            sum(volume_24h) AS oi_vol_24h,
            count(*) AS oi_mkt_count,
            CAST(sum(volume_24h) AS DOUBLE) / NULLIF(sum(open_interest), 0) AS oi_turnover_ratio
        FROM base
        WHERE cohort IS NOT NULL
        GROUP BY cohort, category
        ORDER BY cohort, sum(open_interest) DESC
    """)

    oi_chart = (
        alt.Chart(oi_data)
        .mark_bar()
        .encode(
            alt.X("oi_total:Q", title="Total Open Interest"),
            alt.Y("oi_category:N", sort="-x", title="Category"),
            alt.Color("oi_cohort:N", title="Cohort",
                       scale=alt.Scale(domain=["1-week", "2-week"],
                                        range=["steelblue", "darkorange"])),
            alt.Row("oi_cohort:N", title="Cohort"),
            tooltip=[
                alt.Tooltip("oi_category:N", title="Category"),
                alt.Tooltip("oi_cohort:N", title="Cohort"),
                alt.Tooltip("oi_total:Q", title="Total OI", format=","),
                alt.Tooltip("oi_avg:Q", title="Avg OI", format=",.0f"),
                alt.Tooltip("oi_turnover_ratio:Q", title="Turnover Ratio", format=".2f"),
            ],
        )
        .properties(width=600, height=250,
                     title="Open Interest by Category and Cohort")
    )

    mo.md("## Open Interest Concentration by Category")
    mo.ui.altair_chart(oi_chart)
    return oi_data


@app.cell
def oi_commentary(mo):
    mo.md(
        """
        Open interest tells us where capital is parked in these longshot markets.

        In the one-week cohort, Politics leads with 5.05M total OI — almost
        entirely from the Khamenei market (3.85M OI on a single 5-cent contract).
        Crypto and Sports follow at ~850K each, with turnover ratios of 0.13 and
        0.69 respectively. Climate markets have the second-highest turnover (0.74),
        meaning weather-related longshots are actively traded relative to their
        position sizes.

        The two-week cohort is overwhelmingly Sports (6.65M of 6.78M total OI)
        with a remarkable 0.91 turnover ratio — nearly every open position turns
        over daily. This reflects the high-frequency nature of sports betting:
        prices update with pre-game information, and traders actively manage
        positions.
        """
    )
    return ()


@app.cell
def sample_markets_1wk(mo, query, snapshot_date):
    sample_1wk = query(f"""
        SELECT
            m.ticker AS sample_1wk_ticker,
            m.title AS sample_1wk_title,
            e.title AS sample_1wk_event,
            COALESCE(e.category, 'Unknown') AS sample_1wk_cat,
            m.last_price AS sample_1wk_price,
            m.volume_24h AS sample_1wk_vol24h,
            m.open_interest AS sample_1wk_oi,
            date_diff('day', date('{snapshot_date}'),
                       date(from_iso8601_timestamp(m.close_time))) AS sample_1wk_dte
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.last_price >= 3
          AND m.last_price <= 15
          AND m.close_time IS NOT NULL
          AND m.close_time != ''
          AND date_diff('day', date('{snapshot_date}'),
                         date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7
        ORDER BY m.volume_24h DESC
        LIMIT 25
    """)

    mo.md("## Sample One-Week Longshot Markets (Top 25 by 24h Volume)")
    mo.ui.table(sample_1wk)
    return sample_1wk


@app.cell
def sample_1wk_commentary(mo):
    mo.md(
        """
        The top 25 one-week longshots span a wide range of event types:

        - **Geopolitical**: Ali Khamenei leaving office (5c, DTE=7, 540K volume)
        - **Weather**: NYC snow (9c, DTE=7), NYC snow short-window (8c, DTE=3),
          LA temperature (5c, DTE=1)
        - **Sports**: NASCAR drivers — Chase Elliott, Kyle Busch, Tyler Reddick,
          etc. (5-10c each, DTE=1)
        - **Entertainment**: Billboard #1 artist, Spotify streaming counts,
          "Best AI" ranking
        - **Politics**: SOTU attendance, Trump mention bets
        - **Crypto**: BTC above $80K (3c, DTE=7)

        This diversity makes the one-week cohort an interesting test bed for
        cross-category longshot bias analysis.
        """
    )
    return ()


@app.cell
def sample_markets_2wk(mo, query, snapshot_date):
    sample_2wk = query(f"""
        SELECT
            m.ticker AS sample_2wk_ticker,
            m.title AS sample_2wk_title,
            e.title AS sample_2wk_event,
            COALESCE(e.category, 'Unknown') AS sample_2wk_cat,
            m.last_price AS sample_2wk_price,
            m.volume_24h AS sample_2wk_vol24h,
            m.open_interest AS sample_2wk_oi,
            date_diff('day', date('{snapshot_date}'),
                       date(from_iso8601_timestamp(m.close_time))) AS sample_2wk_dte
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.last_price >= 3
          AND m.last_price <= 15
          AND m.close_time IS NOT NULL
          AND m.close_time != ''
          AND date_diff('day', date('{snapshot_date}'),
                         date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14
        ORDER BY m.volume_24h DESC
        LIMIT 25
    """)

    mo.md("## Sample Two-Week Longshot Markets (Top 25 by 24h Volume)")
    mo.ui.table(sample_2wk)
    return sample_2wk


@app.cell
def sample_2wk_commentary(mo):
    mo.md(
        """
        The two-week sample is a wall of sports — every single one of the top 25
        markets is a sporting event. The highest-volume individual markets are:

        - **Rory McIlroy winning PGA Genesis** (14c, 2.18M volume, 2.94M OI)
        - **Aldrich Potgieter winning PGA Genesis** (3c, 1.16M volume)
        - **Tottenham vs Arsenal Winner** (13c, 811K volume)
        - **Pittsburgh at North Carolina Winner** (3c, 652K volume)

        All 25 markets are at DTE=14, confirming the batch-listing pattern. The
        mix covers golf (PGA), soccer (EPL, Ligue 1, Serie A), basketball (NBA,
        college), tennis (ATP), and esports (CS2). Open interest per market ranges
        from ~10K for minor matchups to nearly 3M for marquee events.
        """
    )
    return ()


if __name__ == "__main__":
    app.run()
