# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "boto3",
#     "pandas",
#     "altair",
#     "python-dotenv",
#     "pyarrow",
#     "s3fs",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def pc_setup():
    import marimo as mo
    import altair as alt
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs

    from longshot.storage.athena import query
    from longshot.config import SETTINGS

    return SETTINGS, alt, mo, pa, pd, pq, query, s3fs


@app.cell
def pc_partitions(mo, query):
    pc_partition_info = query("""
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

    pc_mkt_part = pc_partition_info[pc_partition_info["tbl"] == "daily_markets"].iloc[0]
    pc_evt_part = pc_partition_info[pc_partition_info["tbl"] == "daily_events"].iloc[0]
    pc_snap_date = pc_mkt_part["latest_date"]
    pc_snap_hour = str(int(pc_mkt_part["latest_hour"]))
    pc_evt_snap_date = pc_evt_part["latest_date"]
    pc_evt_snap_hour = str(int(pc_evt_part["latest_hour"]))

    mo.md(
        f"""
        # Portfolio Construction Heuristics

        Notebooks 08-09 established that the longshot universe (3-15 cents,
        DTE 1-14) contains ~425 investable markets after liquidity screening
        (`volume_24h >= 100`), with ~$14K/day in investable capacity. This
        notebook shifts from "is there an edge?" to **"how do we construct a
        portfolio to test it?"**

        The goal is rough, actionable heuristics to select markets for
        small-size live testing (fixed 1 No contract per market) — not
        precision optimization.

        **Key new analysis**: Mutually exclusive (ME) events contain markets
        where at most one can resolve YES. These are naturally anti-correlated
        and capital-efficient — Kalshi returns excess collateral when buying
        No across multiple outcomes in the same ME event.

        | Table | Date | Hour (UTC) |
        |-------|------|------------|
        | daily_markets | {pc_snap_date} | {pc_snap_hour} |
        | daily_events | {pc_evt_snap_date} | {pc_evt_snap_hour} |
        """
    )
    return pc_snap_date, pc_snap_hour, pc_evt_snap_date, pc_evt_snap_hour


@app.cell
def pc_key_findings(mo):
    mo.md(
        r"""
        ### Key Findings

        1. **Portfolio of 374 markets across 227 events** — after removing
           Financials (wide spreads, no edge) and capping non-ME events at 3
           markets each. Total capital required: **\$278** effective collateral
           (1 No contract per market).

        2. **44% of investable longshots are in mutually exclusive events** —
           higher in the 1-week cohort (53%) than 2-week (31%). Weather
           (70% ME) and Entertainment (61% ME) contribute the most ME
           structure, not Sports (only 41% ME, since spread/points markets
           aren't ME).

        3. **Collateral efficiency is concentrated, not broad.** The 33 ME
           clusters average just 3.2 markets. Two outliers drive most
           savings: NASCAR (18.5x efficiency) and Bitcoin range (11.7x).
           Category-level efficiency: Crypto 6.3x, Sports 6.1x, Weather
           2.3x. Total portfolio savings: 20% (\$68 on \$346 nominal).

        4. **Expected edge is thin: +\$4.56 across 374 contracts** (~1.2
           cents per contract, Becker calibration). This is smaller than
           the average bid-ask spread in every category except Weather.
           Execution quality — not market selection — will determine
           whether this strategy is profitable.

        5. **The portfolio is a variance bet.** With 95th-percentile loss
           at -\$3.06, even a correctly-calibrated FLB edge can produce
           losses over a 1-2 week test window. The purpose is to validate
           fills and execution, not to prove profitability in one round.
        """
    )
    return ()


# ── Section 1: Investable Longshot Universe ────────────────────────────────


@app.cell
def pc_universe_query(mo, query, pc_snap_date):
    pc_universe_summary = query(f"""
        WITH base AS (
            SELECT
                CASE
                    WHEN date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS pc_cohort,
                CASE WHEN e.mutually_exclusive = true THEN 'ME' ELSE 'Non-ME' END AS pc_me_label,
                m.volume_24h,
                CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 AS pc_dollar_vol
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            pc_cohort,
            pc_me_label,
            count(*) AS pc_market_count,
            sum(volume_24h) AS pc_total_vol_24h,
            sum(pc_dollar_vol) AS pc_total_dollar_vol
        FROM base
        WHERE pc_cohort IS NOT NULL
        GROUP BY pc_cohort, pc_me_label
        ORDER BY pc_cohort, pc_me_label
    """)

    mo.output.replace(mo.vstack([
        mo.md("---\n## Section 1: Investable Longshot Universe"),
        mo.ui.table(pc_universe_summary, label="Investable Universe by Cohort & ME Status"),
    ]))
    return (pc_universe_summary,)


@app.cell
def pc_universe_commentary(mo, pc_universe_summary):
    pc_total_mkts = int(pc_universe_summary["pc_market_count"].sum())
    pc_me_mkts = int(pc_universe_summary[pc_universe_summary["pc_me_label"] == "ME"]["pc_market_count"].sum())
    pc_nonme_mkts = pc_total_mkts - pc_me_mkts
    pc_me_pct = pc_me_mkts / pc_total_mkts * 100 if pc_total_mkts > 0 else 0

    # Per-cohort ME breakdown
    pc_1w = pc_universe_summary[pc_universe_summary["pc_cohort"] == "1-week"]
    pc_1w_total = int(pc_1w["pc_market_count"].sum())
    pc_1w_me = int(pc_1w[pc_1w["pc_me_label"] == "ME"]["pc_market_count"].sum()) if len(pc_1w[pc_1w["pc_me_label"] == "ME"]) > 0 else 0
    pc_2w = pc_universe_summary[pc_universe_summary["pc_cohort"] == "2-week"]
    pc_2w_total = int(pc_2w["pc_market_count"].sum())
    pc_2w_me = int(pc_2w[pc_2w["pc_me_label"] == "ME"]["pc_market_count"].sum()) if len(pc_2w[pc_2w["pc_me_label"] == "ME"]) > 0 else 0

    mo.md(
        f"""
        The investable longshot universe contains **{pc_total_mkts}** markets
        passing all filters (3-15 cents, volume_24h >= 100, DTE 1-14).

        Of these, **{pc_me_mkts}** ({pc_me_pct:.0f}%) are in mutually exclusive
        events and **{pc_nonme_mkts}** ({100 - pc_me_pct:.0f}%) are non-ME.

        The ME fraction differs sharply by cohort:
        - **1-week**: {pc_1w_total} markets, {pc_1w_me} ME ({pc_1w_me*100//pc_1w_total if pc_1w_total else 0}%) — Weather (70% ME) and Entertainment (61% ME) drive this
        - **2-week**: {pc_2w_total} markets, {pc_2w_me} ME ({pc_2w_me*100//pc_2w_total if pc_2w_total else 0}%) — most Sports markets are Non-ME (spread bets, point totals, goalscorer props aren't mutually exclusive)

        The 1-week cohort's higher ME fraction is a pleasant surprise: the
        diverse, higher-edge cohort also has more collateral-efficient
        structure.
        """
    )
    return ()


# ── Section 2: Event Structure Analysis ────────────────────────────────────


@app.cell
def pc_event_structure_query(query, pc_snap_date):
    pc_event_buckets = query(f"""
        WITH per_event AS (
            SELECT
                m.event_ticker AS pe_event_ticker,
                COALESCE(e.category, 'Unknown') AS pe_category,
                e.title AS pe_event_title,
                CASE WHEN e.mutually_exclusive = true THEN 'ME' ELSE 'Non-ME' END AS pe_me_label,
                count(*) AS pe_longshot_count,
                sum(m.volume_24h) AS pe_total_vol_24h
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
            GROUP BY m.event_ticker, e.category, e.title, e.mutually_exclusive
        )
        SELECT
            pe_me_label,
            CASE
                WHEN pe_longshot_count = 1 THEN '1'
                WHEN pe_longshot_count BETWEEN 2 AND 5 THEN '2-5'
                WHEN pe_longshot_count BETWEEN 6 AND 10 THEN '6-10'
                WHEN pe_longshot_count BETWEEN 11 AND 20 THEN '11-20'
                ELSE '20+'
            END AS pe_bucket,
            count(*) AS pe_event_count,
            sum(pe_longshot_count) AS pe_market_count
        FROM per_event
        GROUP BY
            pe_me_label,
            CASE
                WHEN pe_longshot_count = 1 THEN '1'
                WHEN pe_longshot_count BETWEEN 2 AND 5 THEN '2-5'
                WHEN pe_longshot_count BETWEEN 6 AND 10 THEN '6-10'
                WHEN pe_longshot_count BETWEEN 11 AND 20 THEN '11-20'
                ELSE '20+'
            END
        ORDER BY pe_me_label, pe_bucket
    """)

    pc_top_longshot_events = query(f"""
        SELECT
            m.event_ticker AS tle_event_ticker,
            COALESCE(e.category, 'Unknown') AS tle_category,
            e.title AS tle_event_title,
            CASE WHEN e.mutually_exclusive = true THEN 'ME' ELSE 'Non-ME' END AS tle_me_label,
            count(*) AS tle_longshot_count,
            sum(m.volume_24h) AS tle_total_vol_24h
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND m.volume_24h >= 100
          AND date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        GROUP BY m.event_ticker, e.category, e.title, e.mutually_exclusive
        ORDER BY count(*) DESC
        LIMIT 15
    """)
    return pc_event_buckets, pc_top_longshot_events


@app.cell
def pc_event_structure_viz(alt, mo, pc_event_buckets, pc_top_longshot_events):
    pc_bucket_sort = ["1", "2-5", "6-10", "11-20", "20+"]

    pc_bucket_chart = (
        alt.Chart(pc_event_buckets)
        .mark_bar()
        .encode(
            alt.X("pe_bucket:N", sort=pc_bucket_sort, title="Longshot Markets per Event"),
            alt.Y("pe_event_count:Q", title="Number of Events"),
            alt.Color(
                "pe_me_label:N",
                scale=alt.Scale(domain=["ME", "Non-ME"], range=["#2ca02c", "#d62728"]),
                title="ME Status",
            ),
            alt.XOffset("pe_me_label:N"),
            tooltip=[
                alt.Tooltip("pe_bucket:N", title="Bucket"),
                alt.Tooltip("pe_me_label:N", title="ME Status"),
                alt.Tooltip("pe_event_count:Q", title="Events", format=","),
                alt.Tooltip("pe_market_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=600, height=350, title="Longshot Markets per Event — Distribution by ME Status")
    )

    mo.output.replace(mo.vstack([
        mo.md("---\n## Section 2: Event Structure Analysis"),
        mo.ui.altair_chart(pc_bucket_chart),
        mo.md("### Top 15 Events by Investable Longshot Count"),
        mo.ui.table(pc_top_longshot_events, label="Top Events by Longshot Market Count"),
    ]))
    return ()


@app.cell
def pc_event_commentary(mo):
    mo.md(
        """
        Most ME events are **small clusters**: 81 ME events have just 1
        investable longshot, and 31 have 2-5. Only 2 ME events have 10+
        longshot markets — the **NASCAR Autotrader 400** (19 runners, the
        largest cluster by far) and the **Bitcoin price range** event (12
        range buckets). Non-ME events show a similar pattern but include
        more mid-sized clusters: 5 events with 6-10 markets (NBA spreads,
        SOTU mentions) and 1 with 11-20 (Bitcoin directional).

        The top events table reveals the building blocks for portfolio
        construction: NASCAR provides the single largest anti-correlated
        cluster, while crypto range markets and weather temperature buckets
        provide smaller but numerous ME clusters. Non-ME multi-market
        events (EPL goalscorers, SOTU attendance, NBA spreads) need to be
        capped to limit correlated exposure.
        """
    )
    return ()


# ── Section 3: Collateral Efficiency of ME Clusters ───────────────────────


@app.cell
def pc_collateral_query(query, pc_snap_date):
    pc_me_clusters = query(f"""
        WITH investable AS (
            SELECT
                m.event_ticker AS cl_event_ticker,
                COALESCE(e.category, 'Unknown') AS cl_category,
                e.title AS cl_event_title,
                m.last_price AS cl_yes_price,
                (100 - m.last_price) AS cl_no_price,
                m.volume_24h AS cl_vol_24h
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND e.mutually_exclusive = true
              AND date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cl_event_ticker,
            cl_category,
            cl_event_title,
            count(*) AS cl_n_markets,
            avg(cl_no_price) AS cl_avg_no_price,
            sum(cl_no_price) AS cl_nominal_cost,
            max(cl_no_price) AS cl_effective_cost,
            CAST(sum(cl_no_price) AS DOUBLE) / NULLIF(CAST(max(cl_no_price) AS DOUBLE), 0) AS cl_efficiency_ratio,
            sum(cl_yes_price) AS cl_total_yes_premium,
            sum(cl_vol_24h) AS cl_total_vol_24h
        FROM investable
        GROUP BY cl_event_ticker, cl_category, cl_event_title
        HAVING count(*) >= 2
        ORDER BY count(*) DESC
    """)
    return (pc_me_clusters,)


@app.cell
def pc_collateral_viz(alt, mo, pd, pc_me_clusters):
    # Per-category aggregation of collateral efficiency
    pc_cat_collateral = (
        pc_me_clusters.groupby("cl_category")
        .agg(
            cl_cat_events=("cl_event_ticker", "count"),
            cl_cat_total_markets=("cl_n_markets", "sum"),
            cl_cat_nominal=("cl_nominal_cost", "sum"),
            cl_cat_effective=("cl_effective_cost", "sum"),
        )
        .reset_index()
    )
    pc_cat_collateral["cl_cat_efficiency"] = (
        pc_cat_collateral["cl_cat_nominal"] / pc_cat_collateral["cl_cat_effective"]
    )
    pc_cat_collateral = pc_cat_collateral.sort_values("cl_cat_efficiency", ascending=False)

    pc_eff_chart = (
        alt.Chart(pc_cat_collateral)
        .mark_bar()
        .encode(
            alt.X("cl_cat_efficiency:Q", title="Collateral Efficiency Ratio (nominal / effective)"),
            alt.Y("cl_category:N", sort="-x", title="Category"),
            alt.Color("cl_category:N", legend=None),
            tooltip=[
                alt.Tooltip("cl_category:N", title="Category"),
                alt.Tooltip("cl_cat_events:Q", title="ME Events", format=","),
                alt.Tooltip("cl_cat_total_markets:Q", title="Total Markets", format=","),
                alt.Tooltip("cl_cat_nominal:Q", title="Nominal Cost (cents)", format=",.0f"),
                alt.Tooltip("cl_cat_effective:Q", title="Effective Cost (cents)", format=",.0f"),
                alt.Tooltip("cl_cat_efficiency:Q", title="Efficiency Ratio", format=".1f"),
            ],
        )
        .properties(width=600, height=350, title="ME Collateral Efficiency by Category")
    )

    mo.output.replace(mo.vstack([
        mo.md("---\n## Section 3: Collateral Efficiency of ME Clusters"),
        mo.md("### ME Clusters — Per-Event Detail"),
        mo.ui.table(
            pc_me_clusters[["cl_event_ticker", "cl_category", "cl_event_title",
                             "cl_n_markets", "cl_nominal_cost", "cl_effective_cost",
                             "cl_efficiency_ratio", "cl_total_yes_premium"]],
            label="ME Cluster Collateral Analysis",
        ),
        mo.md("### Collateral Efficiency by Category"),
        mo.ui.altair_chart(pc_eff_chart),
    ]))
    return ()


@app.cell
def pc_collateral_commentary(mo):
    mo.md(
        r"""
        **How ME collateral works**: In a mutually exclusive event, at most
        one market resolves YES. When you buy No on multiple outcomes, Kalshi
        only requires collateral equal to the **max single No position cost**,
        not the sum. This reduces capital requirements for multi-market ME
        clusters.

        **Actual top cluster — NASCAR Autotrader 400** (19 longshot runners):
        - Nominal cost = 19 runners × avg 95¢ No price = \$17.99
        - Effective cost = max(97¢) = \$0.97
        - **Efficiency ratio = 18.5x**
        - Total yes premiums collected = \$1.01 if all resolve No

        **Category-level efficiency**:
        - **Crypto**: 6.3x — driven by the 12-market Bitcoin range cluster
        - **Sports**: 6.1x — driven almost entirely by the NASCAR event
        - **Entertainment**: 2.7x — small clusters (4 markets each)
        - **Weather**: 2.3x — many 2-3 market clusters, consistent but modest

        The efficiency gains are concentrated in a few large clusters. The
        33 ME cluster events average just 3.2 markets each. Outside of
        NASCAR and Bitcoin range, efficiency ratios are typically 2-3x.
        Still valuable — but the collateral story is driven by a handful
        of large events, not a broad structural advantage.
        """
    )
    return ()


# ── Section 4: Category-Level Portfolio Heuristics ─────────────────────────


@app.cell
def pc_cat_profile_query(query, pc_snap_date):
    pc_cat_profiles = query(f"""
        WITH base AS (
            SELECT
                COALESCE(e.category, 'Unknown') AS cp_category,
                m.event_ticker AS cp_event_ticker,
                CASE WHEN e.mutually_exclusive = true THEN 1 ELSE 0 END AS cp_me,
                CASE
                    WHEN date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN 1
                    ELSE 0
                END AS cp_is_1w,
                m.last_price AS cp_last_price,
                m.yes_bid AS cp_yes_bid,
                m.yes_ask AS cp_yes_ask,
                m.volume_24h AS cp_vol_24h
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            cp_category,
            count(*) AS cp_market_count,
            count(DISTINCT cp_event_ticker) AS cp_event_count,
            CAST(count(*) AS DOUBLE) / NULLIF(CAST(count(DISTINCT cp_event_ticker) AS DOUBLE), 0) AS cp_avg_mkts_per_event,
            sum(cp_me) * 100.0 / NULLIF(count(*), 0) AS cp_me_pct,
            sum(CAST(cp_vol_24h AS DOUBLE) * CAST(cp_last_price AS DOUBLE) / 100.0) AS cp_total_dollar_vol,
            avg(cp_yes_ask - cp_yes_bid) AS cp_avg_spread,
            avg(cp_last_price) AS cp_avg_price,
            sum(cp_is_1w) * 100.0 / NULLIF(count(*), 0) AS cp_pct_1w
        FROM base
        GROUP BY cp_category
        ORDER BY count(*) DESC
    """)
    return (pc_cat_profiles,)


@app.cell
def pc_cat_profile_viz(alt, mo, pc_cat_profiles):
    pc_cat_bar = (
        alt.Chart(pc_cat_profiles)
        .transform_fold(
            ["cp_market_count", "cp_event_count"],
            as_=["cp_metric", "cp_value"],
        )
        .mark_bar()
        .encode(
            alt.X("cp_value:Q", title="Count"),
            alt.Y("cp_category:N", sort="-x", title="Category"),
            alt.Color(
                "cp_metric:N",
                scale=alt.Scale(
                    domain=["cp_market_count", "cp_event_count"],
                    range=["steelblue", "darkorange"],
                ),
                title="Metric",
                legend=alt.Legend(
                    labelExpr="datum.value === 'cp_market_count' ? 'Markets' : 'Events'"
                ),
            ),
            alt.XOffset("cp_metric:N"),
            tooltip=[
                alt.Tooltip("cp_category:N", title="Category"),
                alt.Tooltip("cp_metric:N", title="Metric"),
                alt.Tooltip("cp_value:Q", title="Count", format=","),
            ],
        )
        .properties(width=600, height=400, title="Investable Markets & Events by Category")
    )

    mo.output.replace(mo.vstack([
        mo.md("---\n## Section 4: Category-Level Portfolio Heuristics"),
        mo.md("### Category Profiles"),
        mo.ui.table(pc_cat_profiles, label="Category Profile Summary"),
        mo.ui.altair_chart(pc_cat_bar),
    ]))
    return ()


@app.cell
def pc_heuristic_rules(mo):
    mo.md(
        r"""
        ### Portfolio Selection Heuristics

        Based on the category profiles above, here are concrete selection
        rules for the initial test portfolio. Target allocations reflect the
        natural composition of the investable universe:

        | Category | Target Alloc | Selection Rule | Max Mkts/Event | Rationale |
        |----------|-------------|----------------|----------------|-----------|
        | Sports | ~40% | All ME longshots; cap Non-ME at 3/event | 3 (Non-ME) | 41% ME, collateral efficient for winner-take-all events |
        | Weather | ~25% | All investable | All | 100% 1-week, 70% ME, daily resolution, tight 2¢ spreads |
        | Crypto | ~10% | All investable | 3 (Non-ME) | 37% ME, 100% 1-week, strongest FLB signal |
        | Entertainment | ~9% | All investable | All | 61% ME, 94% 1-week, behavioral edge |
        | Politics | ~5% | Cap at 3/event | 3 | Only 8% ME, correlated within category |
        | Mentions | ~3% | Cap at 3/event | 3 | 0% ME, SOTU-driven, event-specific |
        | Economics | ~2% | All investable | All | Small but uncorrelated |
        | Sci/Tech | ~1% | All investable | All | Tiny but interesting (AI benchmarks) |
        | Financials | 0% | Skip | — | Wide spreads (14¢), weakest edge |

        **Position sizing**: Fixed **1 No contract per market** — minimum size
        to validate the strategy. Scale up later once fill quality and edge
        are confirmed.

        **ME events**: Include all longshot markets — these are anti-correlated
        clusters where collateral efficiency makes even weak per-market edge
        worthwhile.

        **Non-ME events**: Cap at 3 markets per event (ranked by volume_24h)
        to avoid concentration in correlated outcomes.
        """
    )
    return ()


# ── Section 5: Sample Portfolio ────────────────────────────────────────────


@app.cell
def pc_build_portfolio(pd, query, pc_snap_date):
    # Pull full investable universe at market level
    pc_raw_universe = query(f"""
        SELECT
            m.ticker AS pf_ticker,
            m.title AS pf_title,
            m.event_ticker AS pf_event_ticker,
            COALESCE(e.category, 'Unknown') AS pf_category,
            e.title AS pf_event_title,
            CASE WHEN e.mutually_exclusive = true THEN 1 ELSE 0 END AS pf_me,
            CASE
                WHEN date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS pf_cohort,
            m.last_price AS pf_last_price,
            m.yes_bid AS pf_yes_bid,
            m.yes_ask AS pf_yes_ask,
            (m.yes_ask - m.yes_bid) AS pf_spread,
            m.volume_24h AS pf_vol_24h,
            date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) AS pf_dte
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND m.volume_24h >= 100
          AND date_diff('day', date('{pc_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        ORDER BY m.volume_24h DESC
    """)

    # 1. Skip Financials
    pc_filtered = pc_raw_universe[pc_raw_universe["pf_category"] != "Financials"].copy()

    # 2. ME events: include all longshot markets
    pc_me_markets = pc_filtered[pc_filtered["pf_me"] == 1].copy()

    # 3. Non-ME events: cap at 3 markets per event, rank by volume_24h
    pc_nonme_markets = pc_filtered[pc_filtered["pf_me"] == 0].copy()
    pc_nonme_markets["pf_evt_rank"] = (
        pc_nonme_markets.groupby("pf_event_ticker")["pf_vol_24h"]
        .rank(ascending=False, method="first")
    )
    pc_nonme_markets = pc_nonme_markets[pc_nonme_markets["pf_evt_rank"] <= 3].drop(
        columns=["pf_evt_rank"]
    )

    # 4. Combine
    pc_portfolio = pd.concat([pc_me_markets, pc_nonme_markets], ignore_index=True)

    # 5. Add computed columns
    pc_portfolio["pf_no_cost_cents"] = 100 - pc_portfolio["pf_last_price"]
    pc_portfolio["pf_position_size"] = 1  # 1 No contract
    pc_portfolio["pf_nominal_collateral"] = pc_portfolio["pf_no_cost_cents"] / 100.0  # dollars
    # Becker calibration: 5¢ market → 4.18% true prob → ratio 0.836
    pc_portfolio["pf_true_prob"] = (pc_portfolio["pf_last_price"] / 100.0) * 0.836
    # E[P&L] per No contract = (yes_price - 100 * true_prob) / 100 in dollars
    pc_portfolio["pf_expected_pnl"] = (
        (pc_portfolio["pf_last_price"] - 100 * pc_portfolio["pf_true_prob"]) / 100.0
    )

    return (pc_portfolio,)


@app.cell
def pc_portfolio_summary(alt, mo, pd, pc_portfolio):
    import math

    pc_n_markets = len(pc_portfolio)
    pc_n_events = pc_portfolio["pf_event_ticker"].nunique()
    pc_n_me_markets = int((pc_portfolio["pf_me"] == 1).sum())
    pc_n_nonme_markets = pc_n_markets - pc_n_me_markets

    # Nominal collateral = sum of no_cost for all positions
    pc_nominal_total = pc_portfolio["pf_nominal_collateral"].sum()

    # Effective collateral (ME events: max no_cost per event; non-ME: sum)
    pc_me_effective = (
        pc_portfolio[pc_portfolio["pf_me"] == 1]
        .groupby("pf_event_ticker")["pf_no_cost_cents"]
        .max()
        .sum() / 100.0
    )
    pc_nonme_effective = pc_portfolio[pc_portfolio["pf_me"] == 0]["pf_nominal_collateral"].sum()
    pc_effective_total = pc_me_effective + pc_nonme_effective
    pc_collateral_savings = pc_nominal_total - pc_effective_total

    # Expected P&L
    pc_total_expected_pnl = pc_portfolio["pf_expected_pnl"].sum()
    # Variance: sum of per-market variance
    # Var[P&L_i] = p_no * p_yes * (yes_price/100)^2 + p_no * p_yes * (no_cost/100)^2
    # Simplified: Var[P&L_i] = true_prob * (1 - true_prob) * 1  (since payoff is 0 or 1 dollar)
    # More precisely: profit_if_no = yes_price/100, loss_if_yes = no_cost/100
    # Var = (1-tp) * (yes_price/100 - E)^2 + tp * (-no_cost/100 - E)^2 ... but simpler:
    # Var = (1-tp)*tp * ((yes_price + no_cost)/100)^2 = tp*(1-tp) * 1^2 = tp*(1-tp)
    pc_total_variance = (
        pc_portfolio["pf_true_prob"] * (1 - pc_portfolio["pf_true_prob"])
    ).sum()
    pc_total_std = math.sqrt(pc_total_variance)
    pc_pnl_95th_low = pc_total_expected_pnl - 1.645 * pc_total_std

    # Cohort split
    pc_cohort_split = pc_portfolio.groupby("pf_cohort").size().reset_index(name="pf_cs_count")

    # Category distribution
    pc_cat_dist = (
        pc_portfolio.groupby("pf_category")
        .agg(pf_cd_count=("pf_ticker", "count"), pf_cd_me=("pf_me", "sum"))
        .reset_index()
        .sort_values("pf_cd_count", ascending=False)
    )

    pc_cat_dist_chart = (
        alt.Chart(pc_cat_dist)
        .mark_bar()
        .encode(
            alt.X("pf_cd_count:Q", title="Markets Selected"),
            alt.Y("pf_category:N", sort="-x", title="Category"),
            tooltip=[
                alt.Tooltip("pf_category:N", title="Category"),
                alt.Tooltip("pf_cd_count:Q", title="Markets"),
                alt.Tooltip("pf_cd_me:Q", title="ME Markets"),
            ],
        )
        .properties(width=600, height=350, title="Portfolio: Markets by Category")
    )

    # Top 10 events by market count
    pc_top_port_events = (
        pc_portfolio.groupby(["pf_event_ticker", "pf_category", "pf_event_title", "pf_me"])
        .agg(pf_te_count=("pf_ticker", "count"), pf_te_vol=("pf_vol_24h", "sum"))
        .reset_index()
        .sort_values("pf_te_count", ascending=False)
        .head(10)
    )

    mo.output.replace(mo.vstack([
        mo.md("---\n## Section 5: Sample Portfolio"),
        mo.md(
            f"""
            ### Portfolio Summary

            Applying the heuristic rules to the investable universe:

            | Metric | Value |
            |--------|-------|
            | Total markets selected | {pc_n_markets:,} |
            | ME markets | {pc_n_me_markets:,} |
            | Non-ME markets | {pc_n_nonme_markets:,} |
            | Distinct events | {pc_n_events:,} |
            | Nominal collateral (1 contract each) | \\${pc_nominal_total:,.2f} |
            | Effective collateral (after ME returns) | \\${pc_effective_total:,.2f} |
            | Collateral savings from ME | \\${pc_collateral_savings:,.2f} |
            | Expected P&L (Becker calibration) | \\${pc_total_expected_pnl:,.2f} |
            | 95th percentile loss | \\${pc_pnl_95th_low:,.2f} |
            """
        ),
        mo.md("### Category Distribution"),
        mo.ui.altair_chart(pc_cat_dist_chart),
        mo.ui.table(pc_cat_dist, label="Category Breakdown"),
        mo.md("### Cohort Split"),
        mo.ui.table(pc_cohort_split, label="Cohort Split"),
        mo.md("### Top 10 Events by Market Count"),
        mo.ui.table(pc_top_port_events, label="Top Events in Portfolio"),
    ]))
    return ()


@app.cell
def pc_portfolio_detail(mo, pc_portfolio):
    pc_display_cols = [
        "pf_ticker", "pf_title", "pf_category", "pf_event_ticker",
        "pf_me", "pf_cohort", "pf_last_price", "pf_spread",
        "pf_vol_24h", "pf_dte", "pf_no_cost_cents",
        "pf_position_size", "pf_nominal_collateral", "pf_expected_pnl",
    ]
    mo.output.replace(mo.vstack([
        mo.md("### Full Portfolio Detail"),
        mo.ui.table(
            pc_portfolio[pc_display_cols].sort_values("pf_vol_24h", ascending=False),
            label="All Selected Markets (sorted by 24h volume)",
        ),
    ]))
    return ()


@app.cell
def pc_findings(mo):
    mo.md(
        r"""
        ---
        ## Findings & Next Steps

        ### Portfolio Composition
        - **374 markets** selected from 425 investable (removed 5 Financials,
          capped 46 non-ME markets exceeding the 3-per-event limit)
        - **227 distinct events** — good diversification, average 1.6 markets
          per event
        - **184 ME + 190 Non-ME** — roughly balanced, with ME providing
          anti-correlation and Non-ME providing breadth
        - **Category mix**: Sports 42%, Weather 25%, Crypto 11%,
          Entertainment 9%, Politics 5%, Mentions 3%, other 5%
        - **Cohort split**: 61% 1-week (227 markets), 39% 2-week (147)

        ### Collateral & Expected P&L
        - Nominal collateral (1 contract each): **\$346**
        - Effective collateral (after ME returns): **\$278** — a 20% savings
        - The savings are modest because most ME clusters are small (2-4
          markets). The big efficiency gains concentrate in NASCAR (18.5x)
          and Bitcoin range (11.7x)
        - Expected P&L (Becker calibration): **+\$4.56** across 374
          contracts — an edge of ~1.2 cents per contract
        - 95th percentile loss: **-\$3.06** — the portfolio can lose money
          even if the FLB is real, due to variance across hundreds of
          low-probability events

        ### Key Risks
        - **Sports concentration**: 42% of portfolio in Sports, creating
          implicit correlation to a few sporting events (NASCAR alone is
          19 markets)
        - **Thin liquidity in edge categories**: Weather has tight 2¢
          spreads, but Crypto (3.4¢), Entertainment (5.4¢), and especially
          Sports (10.9¢) have spreads that eat into the ~1.2¢ per-contract
          edge
        - **Spread costs dominate**: The 1.2¢ expected edge per contract is
          smaller than the average spread in most categories. Execution at
          mid or better is critical
        - **Single-snapshot bias**: Portfolio composition changes daily as
          events resolve and new ones open

        ### Next Steps
        1. **Deploy**: Place 1 No contract per market across the portfolio,
           targeting limit orders at or near the bid to minimize spread cost
        2. **Track fills**: Monitor execution quality — what fraction of
           limit orders fill? Average slippage vs quoted prices?
        3. **Monitor resolutions**: Track outcomes over 2-4 weeks to measure
           realized edge vs the +\$4.56 Becker-calibrated expectation
        4. **Refine**: If edge materializes, concentrate on categories with
           positive realized returns and increase to 5-10 contracts per
           market
        """
    )
    return ()


# ── Section 6: Save Portfolio to S3 ───────────────────────────────────────


@app.cell
def pc_s3_write(SETTINGS, mo, pa, pc_portfolio, pc_snap_date, pc_snap_hour, pq, s3fs):
    pc_s3_path = (
        f"s3://{SETTINGS.s3_bucket}/{SETTINGS.s3_prefix}"
        f"/ad-hoc/sim_portfolio/date={pc_snap_date}/hour={pc_snap_hour}/data.parquet"
    )

    pc_writer_fs = s3fs.S3FileSystem(
        key=SETTINGS.aws_access_key_id,
        secret=SETTINGS.aws_secret_access_key,
        client_kwargs={"region_name": SETTINGS.aws_region},
    )

    pc_write_table = pa.Table.from_pandas(pc_portfolio)
    with pc_writer_fs.open(pc_s3_path, "wb") as pc_f:
        pq.write_table(pc_write_table, pc_f)

    pc_s3_rows = len(pc_portfolio)
    pc_s3_size = pc_writer_fs.info(pc_s3_path)["size"]

    mo.md("---\n## Section 6: Save Portfolio to S3")
    return pc_s3_path, pc_s3_rows, pc_s3_size


@app.cell
def pc_s3_confirm(mo, pc_s3_path, pc_s3_rows, pc_s3_size):
    mo.md(
        f"""
        ### Portfolio saved to S3

        | Detail | Value |
        |--------|-------|
        | S3 Path | `{pc_s3_path}` |
        | Rows | {pc_s3_rows:,} |
        | File Size | {pc_s3_size:,} bytes |

        Portfolio snapshot saved successfully. Read back with:
        ```python
        import pyarrow.parquet as pq
        df = pq.read_table("{pc_s3_path}").to_pandas()
        ```
        """
    )
    return ()


if __name__ == "__main__":
    app.run()
