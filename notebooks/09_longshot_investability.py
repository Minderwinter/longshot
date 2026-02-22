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
def inv_setup():
    import marimo as mo
    import altair as alt
    import pandas as pd

    from longshot.storage.athena import query

    return alt, mo, pd, query


@app.cell
def inv_partitions(mo, query):
    inv_partition_info = query("""
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

    inv_mkt_part = inv_partition_info[inv_partition_info["tbl"] == "daily_markets"].iloc[0]
    inv_evt_part = inv_partition_info[inv_partition_info["tbl"] == "daily_events"].iloc[0]
    inv_snap_date = inv_mkt_part["latest_date"]
    inv_snap_hour = str(int(inv_mkt_part["latest_hour"]))
    inv_evt_snap_date = inv_evt_part["latest_date"]
    inv_evt_snap_hour = str(int(inv_evt_part["latest_hour"]))

    mo.md(
        f"""
        # Longshot Investability Screening

        Notebook 08 identified two longshot cohorts (1-week DTE 1-7, 2-week DTE
        8-14) with fundamentally different characteristics. The key tension:
        **markets with the most FLB mispricing have the least liquidity.**

        This notebook quantifies that tension by applying two screens:
        1. **Liquidity filter**: Only markets with `volume_24h >= 100` (at least
           100 contracts traded in 24h)
        2. **Investable sizing**: Estimate dollar volume as
           `volume_24h * last_price / 100` (in dollars), then assume we can
           capture at most **2% of daily dollar volume** without excessive price
           impact

        Using the most recent daily snapshots from Athena.

        | Table | Date | Hour (UTC) |
        |-------|------|------------|
        | daily_markets | {inv_snap_date} | {inv_snap_hour} |
        | daily_events | {inv_evt_snap_date} | {inv_evt_snap_hour} |
        """
    )
    return inv_snap_date, inv_snap_hour, inv_evt_snap_date, inv_evt_snap_hour


# ── Section 1: Liquidity Screening ──────────────────────────────────────────


@app.cell
def inv_section1_header(mo):
    mo.md("---\n## Section 1: Liquidity Screening (volume_24h >= 100)")
    return ()


@app.cell
def inv_survival_by_cohort(query, inv_snap_date):
    survival_cohort = query(f"""
        WITH base AS (
            SELECT
                CASE
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS surv_cohort,
                m.volume_24h
            FROM daily_markets m
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            surv_cohort,
            count(*) AS total_markets,
            count(CASE WHEN volume_24h >= 100 THEN 1 END) AS screened_markets,
            count(CASE WHEN volume_24h >= 100 THEN 1 END) * 100.0 / NULLIF(count(*), 0) AS survival_pct
        FROM base
        WHERE surv_cohort IS NOT NULL
        GROUP BY surv_cohort
        ORDER BY surv_cohort
    """)
    return (survival_cohort,)


@app.cell
def inv_survival_cohort_display(mo, survival_cohort):
    mo.md("### Survival Rates by Cohort")
    mo.ui.table(survival_cohort, label="Liquidity Screen: volume_24h >= 100")
    return ()


@app.cell
def inv_survival_by_category(query, inv_snap_date):
    survival_cat = query(f"""
        WITH base_cat AS (
            SELECT
                COALESCE(e.category, 'Unknown') AS surv_cat_name,
                CASE
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS surv_cat_cohort,
                m.volume_24h
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            surv_cat_name,
            surv_cat_cohort,
            count(*) AS surv_cat_total,
            count(CASE WHEN volume_24h >= 100 THEN 1 END) AS surv_cat_screened,
            count(CASE WHEN volume_24h >= 100 THEN 1 END) * 100.0 / NULLIF(count(*), 0) AS surv_cat_pct
        FROM base_cat
        WHERE surv_cat_cohort IS NOT NULL
        GROUP BY surv_cat_name, surv_cat_cohort
        HAVING count(*) >= 3
        ORDER BY surv_cat_pct DESC
    """)
    return (survival_cat,)


@app.cell
def inv_survival_cat_chart(alt, mo, survival_cat):
    surv_cat_chart = (
        alt.Chart(survival_cat)
        .mark_bar()
        .encode(
            alt.X("surv_cat_pct:Q", title="Survival Rate (%)"),
            alt.Y("surv_cat_name:N", sort="-x", title="Category"),
            alt.Color(
                "surv_cat_cohort:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            alt.XOffset("surv_cat_cohort:N"),
            tooltip=[
                alt.Tooltip("surv_cat_name:N", title="Category"),
                alt.Tooltip("surv_cat_cohort:N", title="Cohort"),
                alt.Tooltip("surv_cat_total:Q", title="Total Markets", format=","),
                alt.Tooltip("surv_cat_screened:Q", title="Screened Markets", format=","),
                alt.Tooltip("surv_cat_pct:Q", title="Survival %", format=".1f"),
            ],
        )
        .properties(width=700, height=400, title="Liquidity Screen Survival Rate by Category & Cohort")
    )

    mo.md("### Survival Rates by Category & Cohort")
    mo.ui.altair_chart(surv_cat_chart)
    return ()


@app.cell
def inv_liquidity_commentary(mo):
    mo.md(
        """
        The liquidity screen eliminates roughly half of all longshot markets.
        The **1-week cohort** drops from 509 to 256 markets (50% survival),
        while the **2-week cohort** fares slightly better at 60% (282 to 169).

        Category survival rates tell a clear story. **Mentions** markets pass
        easily (92% in 1-week, 89% in 2-week) — these are high-profile
        political/news markets with active trading. **Sports** also survives
        well: 78% in the 1-week cohort, 61% in the 2-week cohort. The
        worst-performing categories are **Financials** (24% survival in
        1-week) and **Elections** (0% — all 3 two-week election markets
        have volume below 100).

        The diverse categories that might harbor FLB edge show mixed results:
        **Climate/Weather** at 56%, **Crypto** at 46%, **Entertainment** at
        44%, and **Politics** at 40%. The liquidity screen cuts deepest into
        exactly the categories where mispricing is most likely to persist.
        """
    )
    return ()


# ── Section 2: Investable Dollar Volume ─────────────────────────────────────


@app.cell
def inv_section2_header(mo):
    mo.md("---\n## Section 2: Investable Dollar Volume")
    return ()


@app.cell
def inv_dollar_vol_by_cohort(query, inv_snap_date):
    dollar_vol_cohort = query(f"""
        WITH screened AS (
            SELECT
                CASE
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS dv_cohort,
                CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 AS dv_dollar_vol,
                CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 * 0.02 AS dv_investable
            FROM daily_markets m
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            dv_cohort,
            count(*) AS dv_market_count,
            sum(dv_dollar_vol) AS dv_total_dollar_vol,
            sum(dv_investable) AS dv_total_investable,
            avg(dv_investable) AS dv_avg_investable
        FROM screened
        WHERE dv_cohort IS NOT NULL
        GROUP BY dv_cohort
        ORDER BY dv_cohort
    """)
    return (dollar_vol_cohort,)


@app.cell
def inv_dollar_vol_cohort_display(mo, dollar_vol_cohort):
    mo.md("### Investable Dollar Volume by Cohort (2% of daily dollar volume)")
    mo.ui.table(dollar_vol_cohort, label="Dollar Volume Summary")
    return ()


@app.cell
def inv_dollar_vol_by_category(query, inv_snap_date):
    dollar_vol_cat = query(f"""
        WITH screened_cat AS (
            SELECT
                COALESCE(e.category, 'Unknown') AS dvc_cat_name,
                CASE
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS dvc_cohort,
                CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 AS dvc_dollar_vol,
                CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 * 0.02 AS dvc_investable
            FROM daily_markets m
            LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            dvc_cat_name,
            dvc_cohort,
            count(*) AS dvc_market_count,
            sum(dvc_dollar_vol) AS dvc_total_dollar_vol,
            sum(dvc_investable) AS dvc_total_investable,
            avg(dvc_investable) AS dvc_avg_investable
        FROM screened_cat
        WHERE dvc_cohort IS NOT NULL
        GROUP BY dvc_cat_name, dvc_cohort
        ORDER BY dvc_total_investable DESC
    """)
    return (dollar_vol_cat,)


@app.cell
def inv_dollar_vol_cat_chart(alt, mo, dollar_vol_cat):
    dvc_chart = (
        alt.Chart(dollar_vol_cat)
        .mark_bar()
        .encode(
            alt.X("dvc_total_investable:Q", title="Total Investable Amount ($)"),
            alt.Y("dvc_cat_name:N", sort="-x", title="Category"),
            alt.Color(
                "dvc_cohort:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            alt.XOffset("dvc_cohort:N"),
            tooltip=[
                alt.Tooltip("dvc_cat_name:N", title="Category"),
                alt.Tooltip("dvc_cohort:N", title="Cohort"),
                alt.Tooltip("dvc_market_count:Q", title="Markets", format=","),
                alt.Tooltip("dvc_total_dollar_vol:Q", title="Dollar Volume", format="$,.0f"),
                alt.Tooltip("dvc_total_investable:Q", title="Investable (2%)", format="$,.2f"),
                alt.Tooltip("dvc_avg_investable:Q", title="Avg per Market", format="$,.2f"),
            ],
        )
        .properties(width=700, height=400, title="Total Investable Amount by Category & Cohort (2% of Dollar Volume)")
    )

    mo.md("### Investable Amount by Category & Cohort")
    mo.ui.altair_chart(dvc_chart)
    return ()


@app.cell
def inv_dollar_vol_commentary(mo):
    mo.md(
        """
        The investable amounts are strikingly small. At a 2% price-impact
        threshold, the **entire 1-week cohort** supports just **$2,595** in
        daily investable capital across 256 markets — an average of $10.13
        per market. The **2-week cohort** is ~4.5x larger at **$11,561**
        across 169 markets ($68.41 average), but this is still modest.

        The category breakdown reveals extreme concentration. **Sports
        (2-week)** accounts for $11,537 of the $11,561 two-week total
        (99.8%). In the 1-week cohort, investable capacity is more spread:
        Sports $747, Politics $683, Climate/Weather $631, then a long tail
        of sub-$200 categories. The Khamenei political market alone
        contributes $539 — over 20% of the 1-week investable total.

        Combined across both cohorts: **~$14,155 per day in total
        investable capacity** for the entire longshot universe. This is a
        fundamental constraint — even a small retail trader would struggle
        to deploy meaningful capital without moving prices.
        """
    )
    return ()


# ── Section 3: Per-Market Distribution ──────────────────────────────────────


@app.cell
def inv_section3_header(mo):
    mo.md("---\n## Section 3: Per-Market Investable Distribution")
    return ()


@app.cell
def inv_per_market_bins(query, inv_snap_date):
    investable_bins = query(f"""
        WITH per_mkt AS (
            SELECT
                CASE
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                    WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 8 AND 14 THEN '2-week'
                END AS bin_cohort,
                CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 * 0.02 AS bin_investable
            FROM daily_markets m
            WHERE m.close_time IS NOT NULL
              AND m.close_time != ''
              AND m.last_price >= 3
              AND m.last_price <= 15
              AND m.volume_24h >= 100
              AND date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        )
        SELECT
            bin_cohort,
            CASE
                WHEN bin_investable < 1 THEN '$0-1'
                WHEN bin_investable < 5 THEN '$1-5'
                WHEN bin_investable < 10 THEN '$5-10'
                WHEN bin_investable < 50 THEN '$10-50'
                ELSE '$50+'
            END AS bin_label,
            count(*) AS bin_count
        FROM per_mkt
        WHERE bin_cohort IS NOT NULL
        GROUP BY
            bin_cohort,
            CASE
                WHEN bin_investable < 1 THEN '$0-1'
                WHEN bin_investable < 5 THEN '$1-5'
                WHEN bin_investable < 10 THEN '$5-10'
                WHEN bin_investable < 50 THEN '$10-50'
                ELSE '$50+'
            END
        ORDER BY bin_cohort, bin_label
    """)
    return (investable_bins,)


@app.cell
def inv_per_market_chart(alt, mo, investable_bins):
    bin_sort_order = ["$0-1", "$1-5", "$5-10", "$10-50", "$50+"]

    bin_chart = (
        alt.Chart(investable_bins)
        .mark_bar()
        .encode(
            alt.X("bin_label:N", sort=bin_sort_order, title="Investable Amount per Market"),
            alt.Y("bin_count:Q", title="Number of Markets"),
            alt.Color(
                "bin_cohort:N",
                scale=alt.Scale(domain=["1-week", "2-week"], range=["steelblue", "darkorange"]),
                title="Cohort",
            ),
            alt.XOffset("bin_cohort:N"),
            tooltip=[
                alt.Tooltip("bin_label:N", title="Bin"),
                alt.Tooltip("bin_cohort:N", title="Cohort"),
                alt.Tooltip("bin_count:Q", title="Markets", format=","),
            ],
        )
        .properties(width=700, height=400, title="Distribution of Per-Market Investable Amount by Cohort")
    )

    mo.md("### Per-Market Investable Amount Distribution")
    mo.ui.altair_chart(bin_chart)
    return ()


@app.cell
def inv_top_markets(query, inv_snap_date):
    top_investable = query(f"""
        SELECT
            m.ticker AS top_inv_ticker,
            m.title AS top_inv_title,
            COALESCE(e.category, 'Unknown') AS top_inv_category,
            CASE
                WHEN date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 7 THEN '1-week'
                ELSE '2-week'
            END AS top_inv_cohort,
            m.last_price AS top_inv_price,
            m.yes_bid AS top_inv_bid,
            m.yes_ask AS top_inv_ask,
            m.volume_24h AS top_inv_vol_24h,
            CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 AS top_inv_dollar_vol,
            CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 * 0.02 AS top_inv_investable,
            date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) AS top_inv_dte
        FROM daily_markets m
        LEFT JOIN daily_events e ON m.event_ticker = e.event_ticker
        WHERE m.close_time IS NOT NULL
          AND m.close_time != ''
          AND m.last_price >= 3
          AND m.last_price <= 15
          AND m.volume_24h >= 100
          AND date_diff('day', date('{inv_snap_date}'), date(from_iso8601_timestamp(m.close_time))) BETWEEN 1 AND 14
        ORDER BY CAST(m.volume_24h AS DOUBLE) * CAST(m.last_price AS DOUBLE) / 100.0 DESC
        LIMIT 20
    """)
    return (top_investable,)


@app.cell
def inv_top_markets_display(mo, top_investable):
    mo.md("### Top 20 Markets by Investable Amount")
    mo.ui.table(top_investable, label="Top Markets by Investable Amount (2% of Dollar Volume)")
    return ()


@app.cell
def inv_distribution_commentary(mo):
    mo.md(
        """
        The per-market distribution confirms that most longshot markets
        are effectively untradeable at any meaningful size. In the **1-week
        cohort**, 127 of 256 markets (50%) have investable amounts under
        $1 — you can't even buy one contract without exceeding the 2%
        impact threshold. Another 61 (24%) fall in the $1-5 range. Only
        10 markets (4%) exceed $50 in investable capacity.

        The **2-week cohort** is somewhat better but still thin: 81 of
        169 markets (48%) are in the sub-$1 bin, while 15 markets (9%)
        exceed $50. The top market (Rory McIlroy, PGA Genesis) supports
        $6,101 in investable volume — a single sports event accounting
        for over half the two-week cohort's capacity.

        The top 20 markets are overwhelmingly Sports (16 of 20), with
        the 2-week PGA and EPL markets dominating. The highest-ranked
        1-week market is the Khamenei political contract at $539 (#5
        overall). Only 5 of the top 20 are from the 1-week cohort
        (NASCAR runners, weather, Khamenei).
        """
    )
    return ()


# ── Overall Findings ────────────────────────────────────────────────────────


@app.cell
def inv_overall_findings(mo):
    mo.md(
        """
        ---

        ## Overall Findings

        1. **Half of longshot markets fail the liquidity screen.** The
           volume_24h >= 100 filter eliminates 50% of the 1-week cohort
           (509 to 256) and 40% of the 2-week cohort (282 to 169). The
           categories most likely to harbor FLB edge (Crypto, Entertainment,
           Politics) have the lowest survival rates (40-46%).

        2. **Total investable capacity is ~$14,155 per day.** At a 2%
           price-impact threshold, the 1-week cohort supports $2,595 and
           the 2-week cohort $11,561. This is the ceiling for a
           non-price-impacting longshot strategy across all of Kalshi.

        3. **Most markets can't support even a $1 position.** Half of
           screened markets in both cohorts have investable amounts under
           $1. Only 25 markets total (10 in 1-week, 15 in 2-week)
           exceed $50 in daily investable capacity.

        4. **The investability paradox is confirmed.** The 2-week Sports
           cohort — which notebook 08 identified as the *worst* FLB
           candidate due to efficient pricing — holds 82% of total
           investable capacity ($11,537 of $14,155). The diverse 1-week
           cohort where FLB edge is most plausible offers just $2,595.

        5. **A single event dominates capacity.** The PGA Genesis
           Invitational alone accounts for $6,101 investable — 43% of
           all longshot investable volume. Remove it and total capacity
           drops to ~$8,000.

        6. **Strategic conclusion.** The longshot FLB strategy on Kalshi
           faces a hard capacity constraint. Even if the mispricing is
           real, the maximum non-impacting daily deployment is roughly
           $2,500-14,000 depending on how aggressively you trade into
           Sports markets. At these sizes, transaction costs (spreads of
           3-10 cents on 3-15 cent contracts = 20-100% round-trip cost)
           likely consume any edge. The FLB on Kalshi is an academic
           curiosity, not a viable trading strategy.
        """
    )
    return ()


if __name__ == "__main__":
    app.run()
