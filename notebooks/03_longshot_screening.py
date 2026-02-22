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
def results_summary(mo):
    mo.md(
        """
        # Longshot Screening — Results Summary

        Screened the **2025-01-01T20:00Z** snapshot for longshot No-selling
        candidates using only forward-looking data and academic calibration.

        | Stage | Result |
        |-------|--------|
        | Snapshot markets | 6,454 |
        | Traded tickers (24h) | 1,159 |
        | After screen (3-12¢, 5+ trades, >1d to close) | **51 candidates** |
        | After 15% relative edge filter | **51 candidates** (all pass) |
        | Portfolio positions (quarter-Kelly) | **51 positions** |
        | Total deployed | **$59,228** of $100K bankroll |
        | Categories represented | 6 of 17 |

        ### Ex-Post Outcome (retrospective — not used in screening)

        | Metric | Screened Portfolio | Naive Baseline (all 3-12¢) |
        |--------|--------------------|---------------------------|
        | Markets resolved | 49 | 259 |
        | Actual Yes rate | **8.16%** | **0.39%** |
        | Avg implied prob | 6.98% | ~7% |
        | Avg P&L per contract | **-$0.012** (loss) | **+$0.059** (profit) |

        **Key insight**: The screening filters (requiring 5+ trades) selected for
        *actively traded* longshots that were more likely to resolve Yes (8.16%)
        than the broader population (0.39%). The liquidity filter inadvertently
        selected for markets with real information flow, where the longshot bias
        is weaker. The naive strategy of selling No on *all* 3-12¢ longshots
        — including illiquid dead markets — was more profitable.

        This raises a fundamental tension for FLB strategies: you need liquidity
        to execute, but liquidity signals that the market has real information,
        which erodes the mispricing you're trying to exploit.
        """
    )
    return ()


@app.cell
def setup_and_imports():
    import marimo as mo
    import altair as alt
    import pandas as pd
    import numpy as np
    import math

    from longshot.storage.athena import query

    return alt, math, mo, np, pd, query


@app.cell
def strategy_parameters(mo):
    # --- Snapshot parameters ---
    SNAPSHOT_DATE = "2025-01-01"
    SNAPSHOT_HOUR = 20

    # --- Screening parameters ---
    YES_MIN = 3        # min yes price (cents)
    YES_MAX = 12       # max yes price (cents)
    MIN_TRADE_COUNT = 5
    MIN_REL_EDGE = 0.15

    # --- Portfolio parameters ---
    BANKROLL = 100_000
    KELLY_FRAC = 0.25
    MAX_POSITION_PCT = 0.05
    MAX_CATEGORY_PCT = 0.15
    MAX_DEPLOYED_PCT = 0.70

    # --- Academic calibration (Becker 2026) ---
    # Market price (cents) -> estimated true probability
    ACADEMIC_CALIBRATION = {
        1: 0.0043,
        3: 0.020,
        5: 0.0418,
        10: 0.075,
        15: 0.12,
    }

    mo.md(
        f"""
        # Longshot Screening — 2025-01-01T20:00Z Snapshot

        Screen the point-in-time snapshot for longshot-selling candidates using
        **only information available at the snapshot time**. Academic calibration
        from Becker (2026) provides the true-probability anchor.

        | Parameter | Value |
        |-----------|-------|
        | Snapshot | `{SNAPSHOT_DATE}` T{SNAPSHOT_HOUR}:00Z |
        | Price range | {YES_MIN}¢ – {YES_MAX}¢ |
        | Min trades (24h) | {MIN_TRADE_COUNT} |
        | Min relative edge | {MIN_REL_EDGE:.0%} |
        | Bankroll | ${BANKROLL:,} |
        | Kelly fraction | {KELLY_FRAC} |
        | Max position | {MAX_POSITION_PCT:.0%} |
        | Max category | {MAX_CATEGORY_PCT:.0%} |
        | Max deployed | {MAX_DEPLOYED_PCT:.0%} |

        **Academic base rates (Becker 2026):**

        | Market Price | True Prob |
        |-------------|-----------|
        | 1¢ | 0.43% |
        | 3¢ | 2.00% |
        | 5¢ | 4.18% |
        | 10¢ | 7.50% |
        | 15¢ | 12.00% |
        """
    )

    return (
        SNAPSHOT_DATE, SNAPSHOT_HOUR,
        YES_MIN, YES_MAX, MIN_TRADE_COUNT, MIN_REL_EDGE,
        BANKROLL, KELLY_FRAC, MAX_POSITION_PCT, MAX_CATEGORY_PCT, MAX_DEPLOYED_PCT,
        ACADEMIC_CALIBRATION,
    )


@app.cell
def trade_derived_pricing(SNAPSHOT_DATE, SNAPSHOT_HOUR, alt, mo, query):
    trade_pricing = query(f"""
        SELECT
            ticker,
            count(*)                                             AS trade_count,
            SUM(count)                                           AS total_contracts,
            SUM(yes_price * count) * 1.0 / NULLIF(SUM(count), 0) AS vwap_yes,
            AVG(CASE WHEN taker_side = 'no'  THEN yes_price END) AS trade_implied_bid,
            AVG(CASE WHEN taker_side = 'yes' THEN yes_price END) AS trade_implied_ask,
            AVG(CASE WHEN taker_side = 'yes' THEN yes_price END)
              - AVG(CASE WHEN taker_side = 'no' THEN yes_price END) AS spread,
            SUM(CASE WHEN taker_side = 'yes' THEN count ELSE 0 END) * 1.0
              / NULLIF(SUM(count), 0)                            AS yes_taker_pct
        FROM ad_hoc_trades
        WHERE snapshot_date = '{SNAPSHOT_DATE}'
          AND snapshot_hour = {SNAPSHOT_HOUR}
        GROUP BY ticker
    """)

    vwap_hist = query(f"""
        WITH ticker_vwap AS (
            SELECT
                ticker,
                SUM(yes_price * count) * 1.0 / NULLIF(SUM(count), 0) AS vwap
            FROM ad_hoc_trades
            WHERE snapshot_date = '{SNAPSHOT_DATE}'
              AND snapshot_hour = {SNAPSHOT_HOUR}
            GROUP BY ticker
        )
        SELECT
            CAST(FLOOR(vwap / 2) * 2 AS INTEGER) AS vwap_bin,
            count(*) AS n
        FROM ticker_vwap
        WHERE vwap IS NOT NULL
        GROUP BY FLOOR(vwap / 2) * 2
        ORDER BY vwap_bin
    """)

    spread_hist = query(f"""
        WITH ticker_spread AS (
            SELECT
                ticker,
                AVG(CASE WHEN taker_side = 'yes' THEN yes_price END)
                  - AVG(CASE WHEN taker_side = 'no' THEN yes_price END) AS spread_val
            FROM ad_hoc_trades
            WHERE snapshot_date = '{SNAPSHOT_DATE}'
              AND snapshot_hour = {SNAPSHOT_HOUR}
            GROUP BY ticker
            HAVING count(*) >= 2
        )
        SELECT
            CAST(FLOOR(spread_val / 2) * 2 AS INTEGER) AS spread_bin,
            count(*) AS n
        FROM ticker_spread
        WHERE spread_val IS NOT NULL
        GROUP BY FLOOR(spread_val / 2) * 2
        ORDER BY spread_bin
    """)

    vwap_chart = (
        alt.Chart(vwap_hist)
        .mark_bar()
        .encode(
            alt.X("vwap_bin:Q", title="VWAP (cents, 2¢ bins)"),
            alt.Y("n:Q", title="# Tickers"),
        )
        .properties(width=350, height=250, title="VWAP Distribution")
    )

    spread_chart = (
        alt.Chart(spread_hist)
        .mark_bar(color="coral")
        .encode(
            alt.X("spread_bin:Q", title="Spread (cents, 2¢ bins)"),
            alt.Y("n:Q", title="# Tickers"),
        )
        .properties(width=350, height=250, title="Spread Distribution")
    )

    mo.md(
        f"""
        ## Trade-Derived Pricing

        Aggregated from `ad_hoc_trades` — 24h trade window ending at snapshot time.

        **{len(trade_pricing):,}** tickers with at least one trade. Median VWAP is 28¢
        (the full traded universe skews toward mid-range). Spreads are only computable
        when both Yes and No taker-side trades exist — about 63% of tickers lack
        a two-sided spread.
        """
    )

    mo.hstack([vwap_chart, spread_chart])

    return (trade_pricing,)


@app.cell
def screen_snapshot_candidates(
    SNAPSHOT_DATE, SNAPSHOT_HOUR,
    YES_MIN, YES_MAX, MIN_TRADE_COUNT,
    alt, mo, pd, query, trade_pricing,
):
    snapshot_candidates = query(f"""
        SELECT
            m.ticker,
            m.event_ticker,
            m.title,
            m.yes_bid,
            m.yes_ask,
            m.volume,
            m.volume_24h,
            m.open_interest,
            m.close_time,
            m.open_time,
            e.category
        FROM ad_hoc_market_snapshots m
        LEFT JOIN events e ON m.event_ticker = e.event_ticker
        WHERE m.snapshot_date = '{SNAPSHOT_DATE}'
          AND m.snapshot_hour = {SNAPSHOT_HOUR}
    """)

    snapshot_ts = pd.Timestamp(f"{SNAPSHOT_DATE}T{SNAPSHOT_HOUR}:00:00Z")
    snapshot_candidates["close_ts"] = pd.to_datetime(
        snapshot_candidates["close_time"], format="ISO8601", utc=True
    )
    snapshot_candidates["days_to_close"] = (
        (snapshot_candidates["close_ts"] - snapshot_ts).dt.total_seconds() / 86400
    )

    candidates_merged = snapshot_candidates.merge(
        trade_pricing[["ticker", "trade_count", "total_contracts", "vwap_yes",
                        "trade_implied_bid", "trade_implied_ask", "spread",
                        "yes_taker_pct"]],
        on="ticker",
        how="left",
    )

    # Use VWAP as primary price; fall back to yes_ask if no trades
    candidates_merged["price"] = candidates_merged["vwap_yes"].fillna(
        candidates_merged["yes_ask"]
    )

    # Apply filters
    screened = candidates_merged[
        (candidates_merged["price"] >= YES_MIN)
        & (candidates_merged["price"] <= YES_MAX)
        & (candidates_merged["trade_count"].fillna(0) >= MIN_TRADE_COUNT)
        & (candidates_merged["days_to_close"] >= 1)
    ].copy()

    price_screen_hist = (
        alt.Chart(screened)
        .mark_bar()
        .encode(
            alt.X("price:Q", bin=alt.Bin(step=1), title="Price (cents)"),
            alt.Y("count():Q", title="# Candidates"),
        )
        .properties(width=350, height=250, title="Candidate Price Distribution")
    )

    cat_screen_bar = (
        alt.Chart(screened)
        .mark_bar()
        .encode(
            alt.X("count():Q", title="# Candidates"),
            alt.Y("category:N", sort="-x", title="Category"),
        )
        .properties(width=350, height=250, title="Candidates by Category")
    )

    mo.md(
        f"""
        ## Screened Candidates

        Starting from **{len(snapshot_candidates):,}** snapshot markets,
        after joining with trade pricing and applying filters:
        - Price in [{YES_MIN}¢, {YES_MAX}¢]
        - >= {MIN_TRADE_COUNT} trades in 24h
        - >= 1 day to close

        **{len(screened):,}** candidates survive the screen.

        The candidate pool is dominated by **Politics** (pardon markets, executive
        orders, cabinet confirmations) and **Entertainment** (Golden Globes, Oscars).
        Only a handful of Economics, Crypto, Financials, and Science markets qualify.
        """
    )

    mo.hstack([price_screen_hist, cat_screen_bar])

    return (screened,)


@app.cell
def score_and_rank_candidates(
    ACADEMIC_CALIBRATION, MIN_REL_EDGE,
    alt, mo, np, pd, screened,
):
    # Build interpolation from academic calibration
    calib_prices = sorted(ACADEMIC_CALIBRATION.keys())
    calib_probs = [ACADEMIC_CALIBRATION[p] for p in calib_prices]

    def interpolate_true_prob(price_cents):
        return float(np.interp(price_cents, calib_prices, calib_probs))

    scored = screened.copy()
    scored["est_true_prob"] = scored["price"].apply(interpolate_true_prob)
    scored["implied_prob"] = scored["price"] / 100.0
    scored["edge"] = scored["implied_prob"] - scored["est_true_prob"]
    scored["relative_edge"] = scored["edge"] / scored["implied_prob"]

    # Normalize components
    tc_min = scored["total_contracts"].min()
    tc_max = scored["total_contracts"].max()
    tc_range = tc_max - tc_min if tc_max > tc_min else 1
    scored["liquidity_norm"] = (scored["total_contracts"] - tc_min) / tc_range

    # Time score: 1.0 if days_to_close in [7, 30], linear decay outside
    def compute_time_score(d):
        if d < 1:
            return 0.0
        elif d < 7:
            return d / 7.0
        elif d <= 30:
            return 1.0
        elif d <= 90:
            return 1.0 - (d - 30) / 60.0
        else:
            return 0.0

    scored["time_score"] = scored["days_to_close"].apply(compute_time_score)

    scored["taker_flow_score"] = scored["yes_taker_pct"].fillna(0.5)

    cat_counts = scored["category"].value_counts()
    scored["diversification_score"] = scored["category"].map(
        lambda c: 1.0 / cat_counts.get(c, 1)
    )

    # Normalize each component to [0, 1]
    def norm_col(s):
        smin, smax = s.min(), s.max()
        return (s - smin) / (smax - smin) if smax > smin else pd.Series(0.5, index=s.index)

    scored["edge_norm"] = norm_col(scored["edge"])
    scored["liq_norm"] = norm_col(scored["liquidity_norm"])
    scored["time_norm"] = norm_col(scored["time_score"])
    scored["div_norm"] = norm_col(scored["diversification_score"])
    scored["flow_norm"] = norm_col(scored["taker_flow_score"])

    scored["composite_score"] = (
        0.40 * scored["edge_norm"]
        + 0.25 * scored["liq_norm"]
        + 0.15 * scored["time_norm"]
        + 0.10 * scored["div_norm"]
        + 0.10 * scored["flow_norm"]
    )

    # Filter by minimum relative edge
    ranked = (
        scored[scored["relative_edge"] >= MIN_REL_EDGE]
        .sort_values("composite_score", ascending=False)
        .reset_index(drop=True)
    )

    ranked_display = ranked[
        ["ticker", "title", "category", "price", "spread", "edge",
         "relative_edge", "composite_score"]
    ].copy()
    ranked_display.columns = [
        "Ticker", "Title", "Category", "VWAP (¢)", "Spread (¢)", "Edge",
        "Rel Edge", "Score",
    ]

    mo.md(
        f"""
        ## Scored & Ranked Candidates

        After applying minimum relative edge >= {MIN_REL_EDGE:.0%}:
        **{len(ranked):,}** candidates remain. All 51 screened candidates pass
        the 15% relative edge threshold — the FLB calibration gives meaningful
        edge across the entire 3-12¢ range.

        Edge ranges from ~0.8¢ to ~2.7¢ per contract. The composite score
        weights edge (40%), liquidity (25%), time horizon (15%),
        diversification (10%), and taker flow (10%).

        Top-ranked candidates tend to be higher-priced (10-12¢) where absolute
        edge is largest, combined with decent liquidity.
        """
    )

    mo.ui.table(ranked_display, label="Ranked Candidates")

    return interpolate_true_prob, ranked


@app.cell
def position_sizing(
    BANKROLL, KELLY_FRAC, MAX_POSITION_PCT, MAX_CATEGORY_PCT, MAX_DEPLOYED_PCT,
    math, mo, pd, ranked,
):
    portfolio = ranked.copy()

    # Quarter-Kelly sizing
    portfolio["kelly_raw"] = (
        (portfolio["implied_prob"] - portfolio["est_true_prob"])
        / portfolio["implied_prob"]
    )
    portfolio["quarter_kelly"] = KELLY_FRAC * portfolio["kelly_raw"]
    portfolio["position_dollars"] = portfolio["quarter_kelly"].clip(upper=MAX_POSITION_PCT) * BANKROLL
    portfolio["no_price_dollars"] = (100 - portfolio["price"]) / 100.0
    portfolio["num_contracts"] = portfolio.apply(
        lambda r: math.floor(r["position_dollars"] / r["no_price_dollars"])
        if r["no_price_dollars"] > 0 else 0,
        axis=1,
    )
    portfolio["actual_cost"] = portfolio["num_contracts"] * portfolio["no_price_dollars"]

    # Category cap: scale down if any category exceeds MAX_CATEGORY_PCT of bankroll
    cat_cap = MAX_CATEGORY_PCT * BANKROLL
    cat_totals = portfolio.groupby("category")["actual_cost"].transform("sum")
    portfolio["cat_scale"] = (cat_cap / cat_totals).clip(upper=1.0)
    portfolio["actual_cost"] = portfolio["actual_cost"] * portfolio["cat_scale"]
    portfolio["num_contracts"] = portfolio.apply(
        lambda r: math.floor(r["actual_cost"] / r["no_price_dollars"])
        if r["no_price_dollars"] > 0 else 0,
        axis=1,
    )
    portfolio["actual_cost"] = portfolio["num_contracts"] * portfolio["no_price_dollars"]

    # Total deployment cap
    total_deployed = portfolio["actual_cost"].sum()
    deploy_cap = MAX_DEPLOYED_PCT * BANKROLL
    if total_deployed > deploy_cap:
        deploy_scale = deploy_cap / total_deployed
        portfolio["actual_cost"] = portfolio["actual_cost"] * deploy_scale
        portfolio["num_contracts"] = portfolio.apply(
            lambda r: math.floor(r["actual_cost"] / r["no_price_dollars"])
            if r["no_price_dollars"] > 0 else 0,
            axis=1,
        )
        portfolio["actual_cost"] = portfolio["num_contracts"] * portfolio["no_price_dollars"]

    # Drop zero-contract positions
    final_portfolio = portfolio[portfolio["num_contracts"] > 0].copy()

    portfolio_display = final_portfolio[
        ["ticker", "category", "price", "edge", "actual_cost", "num_contracts"]
    ].copy()
    portfolio_display.columns = [
        "Ticker", "Category", "VWAP (¢)", "Edge", "Position ($)", "Contracts",
    ]

    mo.md(
        f"""
        ## Position Sizing (Quarter-Kelly)

        Kelly fraction = {KELLY_FRAC}, max position = {MAX_POSITION_PCT:.0%},
        category cap = {MAX_CATEGORY_PCT:.0%}, deployment cap = {MAX_DEPLOYED_PCT:.0%}.

        **{len(final_portfolio):,}** positions sized. The category cap binds for
        Politics (30 candidates compressed to $15K), Entertainment (13 to $15K),
        and Economics (5 to $15K). Single-category positions (Crypto, Sci/Tech,
        Financials) each get ~$5K unconstrained by the cap.

        Total deployment is ~$59K — well under the 70% cap, leaving a
        ~$41K reserve.
        """
    )

    mo.ui.table(portfolio_display, label="Final Portfolio")

    return (final_portfolio,)


@app.cell
def portfolio_summary_charts(BANKROLL, alt, final_portfolio, mo, pd):
    deployed_total = final_portfolio["actual_cost"].sum()
    reserve_total = BANKROLL - deployed_total
    avg_edge_val = final_portfolio["edge"].mean()
    weighted_spread = (
        (final_portfolio["spread"].fillna(0) * final_portfolio["actual_cost"]).sum()
        / deployed_total
        if deployed_total > 0 else 0
    )

    summary_stats = pd.DataFrame([
        {"Metric": "Total Positions", "Value": f"{len(final_portfolio):,}"},
        {"Metric": "Deployed $", "Value": f"${deployed_total:,.2f}"},
        {"Metric": "Reserve $", "Value": f"${reserve_total:,.2f}"},
        {"Metric": "Avg Edge", "Value": f"{avg_edge_val:.4f}"},
        {"Metric": "Wtd Avg Spread", "Value": f"{weighted_spread:.2f}¢"},
    ])

    cat_alloc = (
        final_portfolio.groupby("category")["actual_cost"]
        .sum()
        .reset_index()
        .rename(columns={"actual_cost": "allocated"})
    )
    cat_alloc_chart = (
        alt.Chart(cat_alloc)
        .mark_bar()
        .encode(
            alt.X("allocated:Q", title="Allocated ($)"),
            alt.Y("category:N", sort="-x", title="Category"),
        )
        .properties(width=350, height=250, title="Category Allocation")
    )

    edge_hist_chart = (
        alt.Chart(final_portfolio)
        .mark_bar()
        .encode(
            alt.X("edge:Q", bin=alt.Bin(maxbins=15), title="Edge"),
            alt.Y("count():Q", title="# Positions"),
        )
        .properties(width=350, height=250, title="Edge Distribution")
    )

    score_spread_scatter = (
        alt.Chart(final_portfolio)
        .mark_circle(size=60)
        .encode(
            alt.X("spread:Q", title="Spread (¢)"),
            alt.Y("composite_score:Q", title="Composite Score"),
            alt.Color("category:N", title="Category"),
            tooltip=[
                alt.Tooltip("ticker:N"),
                alt.Tooltip("spread:Q", format=".1f"),
                alt.Tooltip("composite_score:Q", format=".3f"),
            ],
        )
        .properties(width=400, height=300, title="Score vs Spread")
    )

    mo.md("## Portfolio Summary")
    mo.ui.table(summary_stats, label="Portfolio Stats")
    mo.hstack([cat_alloc_chart, edge_hist_chart])
    score_spread_scatter

    return ()


@app.cell
def stricter_volume_filter(
    ACADEMIC_CALIBRATION, BANKROLL, KELLY_FRAC, MAX_POSITION_PCT,
    MIN_REL_EDGE, interpolate_true_prob, math, mo, np, pd, screened,
):
    strict_screened = screened[
        (screened["total_contracts"].fillna(0) * screened["price"].fillna(0) / 100 >= 1000)
    ].copy()

    # Re-score strict candidates
    strict_screened["est_true_prob_s"] = strict_screened["price"].apply(interpolate_true_prob)
    strict_screened["implied_prob_s"] = strict_screened["price"] / 100.0
    strict_screened["edge_s"] = strict_screened["implied_prob_s"] - strict_screened["est_true_prob_s"]
    strict_screened["relative_edge_s"] = strict_screened["edge_s"] / strict_screened["implied_prob_s"]
    strict_filtered = strict_screened[strict_screened["relative_edge_s"] >= MIN_REL_EDGE].copy()

    # Position sizing for strict filter
    strict_filtered["kelly_s"] = (
        (strict_filtered["implied_prob_s"] - strict_filtered["est_true_prob_s"])
        / strict_filtered["implied_prob_s"]
    )
    strict_filtered["qk_s"] = KELLY_FRAC * strict_filtered["kelly_s"]
    strict_filtered["pos_dollars_s"] = strict_filtered["qk_s"].clip(upper=MAX_POSITION_PCT) * BANKROLL
    strict_filtered["no_price_s"] = (100 - strict_filtered["price"]) / 100.0
    strict_filtered["contracts_s"] = strict_filtered.apply(
        lambda r: math.floor(r["pos_dollars_s"] / r["no_price_s"])
        if r["no_price_s"] > 0 else 0,
        axis=1,
    )
    strict_filtered["cost_s"] = strict_filtered["contracts_s"] * strict_filtered["no_price_s"]

    strict_deployed = strict_filtered["cost_s"].sum()
    strict_avg_edge = strict_filtered["edge_s"].mean() if len(strict_filtered) > 0 else 0
    strict_cats = strict_filtered["category"].nunique() if len(strict_filtered) > 0 else 0

    # Identify dropped candidates
    base_tickers = set(screened["ticker"])
    strict_tickers = set(strict_screened["ticker"])
    dropped_tickers = base_tickers - strict_tickers
    dropped_candidates = screened[screened["ticker"].isin(dropped_tickers)][
        ["ticker", "title", "category", "price", "total_contracts"]
    ].copy()
    dropped_candidates["dollar_volume"] = (
        dropped_candidates["total_contracts"].fillna(0) * dropped_candidates["price"].fillna(0) / 100
    )

    base_avg_edge = (
        (screened["price"] / 100.0 - screened["price"].apply(interpolate_true_prob)).mean()
        if len(screened) > 0 else 0
    )

    comparison = pd.DataFrame([
        {
            "Filter": "Base Screen",
            "Candidates": len(screened),
            "Avg Edge": f"{base_avg_edge:.4f}",
            "Categories": screened["category"].nunique(),
        },
        {
            "Filter": "Strict ($1K vol)",
            "Candidates": len(strict_filtered),
            "Avg Edge": f"{strict_avg_edge:.4f}" if len(strict_filtered) > 0 else "N/A",
            "Categories": strict_cats,
        },
    ])

    mo.md(
        f"""
        ## Sensitivity: Stricter Volume Filter

        Require **$1,000 minimum 24h traded dollar volume**
        (`total_contracts * vwap_yes / 100 >= 1000`).

        **{len(strict_screened):,}** candidates survive the volume filter
        (vs **{len(screened):,}** in base screen).

        After relative edge filter: **{len(strict_filtered):,}** strict candidates.

        **{len(dropped_tickers):,}** candidates dropped by the stricter volume requirement.

        This is a stark finding: **92% of longshot candidates have under $1K
        in 24h dollar volume.** Most longshots trade only a few hundred dollars
        per day. The 4 survivors are all Politics markets (Biden pardons,
        popular vote margin, Musk cabinet) where substantial volume flowed.
        Notably, all 4 resolved No — a perfect outcome for this strict filter.
        """
    )

    mo.ui.table(comparison, label="Base vs Strict Comparison")

    if len(dropped_candidates) > 0:
        mo.md("### Dropped Candidates (thin markets)")
        mo.ui.table(
            dropped_candidates.sort_values("dollar_volume"),
            label="Dropped by Volume Filter",
        )

    return ()


@app.cell
def expiry_deep_dive(alt, mo, pd, screened, interpolate_true_prob):
    expiry = screened.copy()
    expiry["est_true_prob_e"] = expiry["price"].apply(interpolate_true_prob)
    expiry["edge_e"] = expiry["price"] / 100.0 - expiry["est_true_prob_e"]

    def assign_expiry_bucket(d):
        if d < 1:
            return "<1d"
        elif d <= 1:
            return "1d"
        elif d <= 2:
            return "2d"
        elif d <= 3:
            return "3d"
        elif d <= 4:
            return "4d"
        elif d <= 5:
            return "5d"
        elif d <= 6:
            return "6d"
        elif d <= 7:
            return "7d"
        elif d <= 14:
            return "8-14d"
        elif d <= 30:
            return "15-30d"
        elif d <= 90:
            return "31-90d"
        else:
            return "90d+"

    BUCKET_ORDER = ["<1d", "1d", "2d", "3d", "4d", "5d", "6d", "7d",
                     "8-14d", "15-30d", "31-90d", "90d+"]

    expiry["expiry_bucket"] = expiry["days_to_close"].apply(assign_expiry_bucket)

    bucket_agg = (
        expiry.groupby("expiry_bucket")
        .agg(
            candidate_count=("ticker", "count"),
            avg_edge_bucket=("edge_e", "mean"),
            avg_liquidity=("total_contracts", "mean"),
        )
        .reset_index()
    )
    bucket_agg["bucket_sort"] = bucket_agg["expiry_bucket"].map(
        {b: i for i, b in enumerate(BUCKET_ORDER)}
    )
    bucket_agg = bucket_agg.sort_values("bucket_sort")

    count_bar = (
        alt.Chart(bucket_agg)
        .mark_bar()
        .encode(
            alt.X("expiry_bucket:N", sort=BUCKET_ORDER, title="Expiry Bucket"),
            alt.Y("candidate_count:Q", title="# Candidates"),
        )
        .properties(width=500, height=250, title="Candidates by Expiry Bucket")
    )

    edge_line = (
        alt.Chart(bucket_agg)
        .mark_line(point=True, color="firebrick")
        .encode(
            alt.X("expiry_bucket:N", sort=BUCKET_ORDER, title="Expiry Bucket"),
            alt.Y("avg_edge_bucket:Q", title="Avg Edge"),
        )
        .properties(width=500, height=200, title="Avg Edge by Expiry Bucket")
    )

    liq_line = (
        alt.Chart(bucket_agg)
        .mark_line(point=True, color="teal")
        .encode(
            alt.X("expiry_bucket:N", sort=BUCKET_ORDER, title="Expiry Bucket"),
            alt.Y("avg_liquidity:Q", title="Avg Contracts (24h)"),
        )
        .properties(width=500, height=200, title="Avg Liquidity by Expiry Bucket")
    )

    mo.md(
        """
        ## Expiry Deep Dive

        Distribution of `days_to_close` across screened candidates, with
        average edge and liquidity per bucket.

        The largest cluster is **15-30d** (20 candidates) — mostly political
        markets resolving in late January 2025 (inauguration-adjacent). The
        **5d** bucket (8 candidates) captures Golden Globes markets closing
        Jan 5-6. **90d+** (13 candidates) are long-dated political/economic
        markets that tie up capital for months.

        Liquidity spikes in the 6d bucket (avg ~60K contracts) driven by
        a few very active markets. Edge is relatively uniform across
        buckets (~1.0-2.0¢), suggesting the FLB is not strongly
        time-dependent in this snapshot.
        """
    )

    mo.vstack([count_bar, edge_line, liq_line])

    return ()


@app.cell
def ex_post_evaluation(
    SNAPSHOT_DATE, SNAPSHOT_HOUR,
    alt, mo, np, pd, query, ranked, interpolate_true_prob,
):
    mo.md(
        """
        ## Ex-Post Evaluation

        **This section uses outcome data and is NOT part of the screening process.**
        It evaluates how the screen would have performed retrospectively.

        The strategy is to **buy No contracts** on screened longshots (equivalent
        to selling the overpriced Yes side). Profit per No-win = yes_price/100;
        loss per Yes-win = (100 - yes_price)/100.
        """
    )

    outcomes = query(f"""
        SELECT
            ticker,
            result
        FROM ad_hoc_market_snapshots
        WHERE snapshot_date = '{SNAPSHOT_DATE}'
          AND snapshot_hour = {SNAPSHOT_HOUR}
          AND result IN ('yes', 'no')
    """)

    eval_df = ranked.merge(outcomes, on="ticker", how="inner")
    eval_df["resolved_yes"] = (eval_df["result"] == "yes").astype(int)

    total_eval = len(eval_df)
    yes_count = eval_df["resolved_yes"].sum()
    no_count = total_eval - yes_count
    actual_yes_rate = yes_count / total_eval if total_eval > 0 else 0
    avg_implied = eval_df["implied_prob"].mean() if total_eval > 0 else 0

    # Simulated P&L for selling No contracts
    # Sell No at (100 - vwap_yes) cents. If market resolves No, profit = no_price.
    # If market resolves Yes, loss = (100 - no_price) = vwap_yes cents.
    eval_df["no_sell_price"] = (100 - eval_df["price"]) / 100.0
    eval_df["pnl_per_contract"] = np.where(
        eval_df["resolved_yes"] == 0,
        eval_df["price"] / 100.0,           # No wins: profit = yes_price (edge captured)
        -(100 - eval_df["price"]) / 100.0,  # Yes wins: loss = no_price paid
    )

    total_pnl = eval_df["pnl_per_contract"].sum()
    avg_pnl = eval_df["pnl_per_contract"].mean() if total_eval > 0 else 0

    # Naive baseline: all longshots in snapshot with yes_ask in range
    naive_outcomes = query(f"""
        SELECT
            m.ticker,
            m.yes_ask,
            m.result
        FROM ad_hoc_market_snapshots m
        WHERE m.snapshot_date = '{SNAPSHOT_DATE}'
          AND m.snapshot_hour = {SNAPSHOT_HOUR}
          AND m.yes_ask >= 3
          AND m.yes_ask <= 12
          AND m.result IN ('yes', 'no')
    """)

    naive_yes_rate = (
        (naive_outcomes["result"] == "yes").mean() if len(naive_outcomes) > 0 else 0
    )
    naive_avg_price = naive_outcomes["yes_ask"].mean() / 100.0 if len(naive_outcomes) > 0 else 0
    naive_pnl = np.where(
        naive_outcomes["result"] == "no",
        naive_outcomes["yes_ask"] / 100.0,
        -(100 - naive_outcomes["yes_ask"]) / 100.0,
    ).mean() if len(naive_outcomes) > 0 else 0

    eval_stats = pd.DataFrame([
        {"Metric": "Resolved markets (screened)", "Value": f"{total_eval:,}"},
        {"Metric": "Yes outcomes", "Value": f"{yes_count:,}"},
        {"Metric": "No outcomes", "Value": f"{no_count:,}"},
        {"Metric": "Actual Yes rate (screened)", "Value": f"{actual_yes_rate:.2%}"},
        {"Metric": "Avg implied prob (screened)", "Value": f"{avg_implied:.2%}"},
        {"Metric": "Avg P&L per contract (screened)", "Value": f"${avg_pnl:.4f}"},
        {"Metric": "Total P&L (sum, screened)", "Value": f"${total_pnl:.2f}"},
        {"Metric": "--- Baseline ---", "Value": "---"},
        {"Metric": "Naive longshots (3-12¢)", "Value": f"{len(naive_outcomes):,}"},
        {"Metric": "Naive Yes rate", "Value": f"{naive_yes_rate:.2%}"},
        {"Metric": "Naive avg P&L per contract", "Value": f"${naive_pnl:.4f}"},
    ])

    outcome_bar = (
        alt.Chart(
            pd.DataFrame({
                "Outcome": ["Yes", "No"],
                "Count": [yes_count, no_count],
            })
        )
        .mark_bar()
        .encode(
            alt.X("Outcome:N"),
            alt.Y("Count:Q"),
            alt.Color("Outcome:N", scale=alt.Scale(range=["firebrick", "steelblue"])),
        )
        .properties(width=200, height=250, title="Screened Outcomes")
    )

    pnl_hist = (
        alt.Chart(eval_df)
        .mark_bar()
        .encode(
            alt.X("pnl_per_contract:Q", bin=alt.Bin(maxbins=20), title="P&L per contract ($)"),
            alt.Y("count():Q", title="# Markets"),
        )
        .properties(width=350, height=250, title="P&L Distribution")
    )

    mo.ui.table(eval_stats, label="Ex-Post Performance")
    mo.hstack([outcome_bar, pnl_hist])

    return ()


@app.cell
def key_findings(mo):
    mo.md(
        """
        ## Key Findings

        1. **51 candidates survived the screen** from 6,454 snapshot markets.
           The price filter (3-12¢) and liquidity filter (5+ trades) are the
           binding constraints. All 51 pass the 15% relative edge threshold.

        2. **The FLB calibration gives consistent edge** across the 3-12¢ range
           (0.8-2.7¢ per contract, 16-33% relative edge). Academic base rates
           are well below market-implied probabilities at every price point.

        3. **Portfolio construction deployed $59K of $100K**, with the category
           cap (15%) binding for Politics, Entertainment, and Economics. The
           deployment cap (70%) did not bind. 51 positions across 6 categories.

        4. **Liquidity is the critical constraint.** Only 4 of 51 candidates
           have >$1K in 24h dollar volume. Most longshots trade <$100/day.
           The strict-filter portfolio (4 positions, all Politics) went 4-for-4
           on No outcomes.

        5. **Expiry clusters around 15-30 days** (20 candidates) driven by
           inauguration-adjacent political markets. The 5-day bucket (8
           candidates) is Golden Globes. Edge is relatively flat across
           expiry buckets.

        6. **Ex-post: the screen lost money.** 4 of 49 resolved candidates
           resolved Yes (8.16% actual vs 6.98% implied), producing avg P&L of
           -$0.012 per contract. The naive baseline (all 259 markets at 3-12¢,
           no liquidity filter) had only 0.39% Yes rate and +$0.059 avg P&L.

        7. **The liquidity-selection paradox.** Requiring 5+ trades selects for
           markets where real information is flowing — which are precisely the
           markets where longshot mispricing is *weakest*. The illiquid dead
           markets that never resolve Yes are where the FLB edge lives, but
           they're unexecutable. This is the core tension for any practical
           FLB strategy.
        """
    )
    return ()


if __name__ == "__main__":
    app.run()
