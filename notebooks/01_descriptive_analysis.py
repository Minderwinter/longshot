# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "pyarrow",
#     "s3fs",
#     "pandas",
#     "altair",
#     "python-dotenv",
# ]
# ///

import marimo

__generated_with = "0.10.0"
app = marimo.App(width="medium")


@app.cell
def load_data():
    import marimo as mo
    import pandas as pd

    from longshot.storage.s3 import read_markets, read_trades

    SNAPSHOT_DATE = "2025-01-01"

    markets_table = read_markets(SNAPSHOT_DATE)
    trades_table = read_trades(SNAPSHOT_DATE)

    markets = markets_table.to_pandas()
    trades = trades_table.to_pandas()

    mo.md(
        f"""
        # Longshot Bias — Descriptive Analysis

        **Snapshot date**: {SNAPSHOT_DATE}

        | Metric | Value |
        |--------|-------|
        | Total markets | {len(markets):,} |
        | Settled markets (result != '') | {markets['result'].notna().sum():,} |
        | Total trades | {len(trades):,} |
        | Total volume (contracts) | {markets['volume'].sum():,.0f} |
        | Categories | {markets['category'].nunique()} |
        """
    )
    return SNAPSHOT_DATE, markets, mo, pd, trades


@app.cell
def summary_by_category(markets, mo):
    cat_summary = (
        markets.groupby("category")
        .agg(
            count=("ticker", "count"),
            avg_yes_ask=("yes_ask", "mean"),
            total_volume=("volume", "sum"),
            avg_open_interest=("open_interest", "mean"),
        )
        .sort_values("count", ascending=False)
        .reset_index()
    )
    mo.md("## Markets by Category")
    return (cat_summary,)


@app.cell
def show_category_table(cat_summary, mo):
    mo.ui.table(cat_summary, label="Category Summary")
    return ()


@app.cell
def price_distribution(markets, mo):
    import altair as alt

    mo.md("## Distribution of Yes Ask Prices")

    df = markets.dropna(subset=["yes_ask"]).copy()
    df["yes_ask_cents"] = df["yes_ask"] * 100

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            alt.X("yes_ask_cents:Q", bin=alt.Bin(maxbins=50), title="Yes Ask (cents)"),
            alt.Y("count()", title="Number of Markets"),
            alt.Color("category:N", title="Category"),
        )
        .properties(width=700, height=400, title="Yes Ask Price Distribution by Category")
    )
    return alt, chart, df


@app.cell
def show_price_chart(chart, mo):
    mo.ui.altair_chart(chart)
    return ()


@app.cell
def calibration_analysis(markets, mo):
    import numpy as np
    import pandas as pd

    mo.md(
        """
        ## Longshot Bias Calibration

        Bin markets by `yes_ask` price, then compute the actual resolution rate
        (fraction where `result == 'yes'`) vs the implied probability (midpoint
        of each price bin).  The favorite-longshot bias predicts that cheap
        contracts resolve Yes *less often* than their price implies.
        """
    )

    settled = markets.dropna(subset=["yes_ask", "result"]).copy()
    settled = settled[settled["result"].isin(["yes", "no"])]
    settled["won"] = (settled["result"] == "yes").astype(int)
    settled["yes_ask_cents"] = settled["yes_ask"] * 100

    bins = [0, 5, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    settled["price_bin"] = pd.cut(settled["yes_ask_cents"], bins=bins)

    calibration = (
        settled.groupby("price_bin", observed=True)
        .agg(
            n=("won", "count"),
            actual_win_rate=("won", "mean"),
        )
        .reset_index()
    )
    calibration["bin_midpoint"] = calibration["price_bin"].apply(
        lambda x: (x.left + x.right) / 2
    )
    calibration["implied_prob"] = calibration["bin_midpoint"] / 100

    return bins, calibration, np, settled


@app.cell
def calibration_chart(alt, calibration, mo):
    import altair as alt

    base = alt.Chart(calibration).encode(
        alt.X("implied_prob:Q", title="Implied Probability (midpoint of price bin)"),
    )

    points = base.mark_circle(size=80, color="steelblue").encode(
        alt.Y("actual_win_rate:Q", title="Actual Win Rate"),
        tooltip=[
            alt.Tooltip("implied_prob:Q", format=".0%", title="Implied"),
            alt.Tooltip("actual_win_rate:Q", format=".0%", title="Actual"),
            alt.Tooltip("n:Q", title="# Markets"),
        ],
    )

    diagonal = (
        alt.Chart(
            alt.Data(values=[{"x": 0, "y": 0}, {"x": 1, "y": 1}])
        )
        .mark_line(strokeDash=[5, 5], color="gray")
        .encode(x="x:Q", y="y:Q")
    )

    chart = (
        (diagonal + points)
        .properties(
            width=600,
            height=400,
            title="Calibration: Implied Probability vs Actual Win Rate",
        )
    )

    mo.ui.altair_chart(chart)
    return ()


@app.cell
def flb_by_category(markets, alt, mo):
    import pandas as pd

    settled = markets.dropna(subset=["yes_ask", "result"]).copy()
    settled = settled[settled["result"].isin(["yes", "no"])]
    settled["won"] = (settled["result"] == "yes").astype(int)

    cat_calib = (
        settled.groupby("category")
        .agg(
            n=("won", "count"),
            actual_win_rate=("won", "mean"),
            avg_yes_ask=("yes_ask", "mean"),
        )
        .reset_index()
    )
    cat_calib["implied_prob"] = cat_calib["avg_yes_ask"]
    cat_calib["edge"] = cat_calib["implied_prob"] - cat_calib["actual_win_rate"]

    chart = (
        alt.Chart(cat_calib)
        .mark_bar()
        .encode(
            alt.X("category:N", sort="-y", title="Category"),
            alt.Y("edge:Q", title="FLB Edge (implied − actual)"),
            alt.Color("category:N", legend=None),
            tooltip=[
                alt.Tooltip("category:N"),
                alt.Tooltip("edge:Q", format=".3f", title="Edge"),
                alt.Tooltip("n:Q", title="# Markets"),
            ],
        )
        .properties(width=600, height=400, title="Favorite-Longshot Bias by Category")
    )

    mo.md("## FLB Edge by Category")
    mo.ui.altair_chart(chart)
    return ()


@app.cell
def trade_volume_analysis(trades, alt, mo):
    import pandas as pd

    trades_with_vol = trades.copy()
    trades_with_vol["notional"] = trades_with_vol["yes_price"] * trades_with_vol["count"]

    vol_by_ticker = (
        trades_with_vol.groupby("ticker")
        .agg(
            trade_count=("trade_id", "count"),
            total_contracts=("count", "sum"),
            total_notional=("notional", "sum"),
        )
        .sort_values("total_contracts", ascending=False)
        .head(30)
        .reset_index()
    )

    chart = (
        alt.Chart(vol_by_ticker)
        .mark_bar()
        .encode(
            alt.X("total_contracts:Q", title="Total Contracts Traded"),
            alt.Y("ticker:N", sort="-x", title="Market Ticker"),
            tooltip=[
                alt.Tooltip("ticker:N"),
                alt.Tooltip("trade_count:Q", title="Trades"),
                alt.Tooltip("total_contracts:Q", title="Contracts"),
            ],
        )
        .properties(width=700, height=500, title="Top 30 Markets by Trade Volume")
    )

    mo.md("## Trade Volume — Top 30 Markets")
    mo.ui.altair_chart(chart)
    return ()


if __name__ == "__main__":
    app.run()
