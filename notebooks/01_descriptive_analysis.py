# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "marimo",
#     "duckdb",
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

    from longshot.storage.db import EVENTS_ALL, MARKETS_ALL, connect

    db = connect()

    # Create a view joining markets with event categories.
    # Category lives on the event, not the market — exclude market's category column if present.
    db.sql(f"""
        CREATE OR REPLACE VIEW markets AS
        SELECT
            m.ticker, m.event_ticker, m.title, m.status,
            m.yes_bid, m.yes_ask, m.no_bid, m.no_ask,
            m.last_price, m.volume, m.volume_24h, m.open_interest,
            m.close_time, m.open_time, m.result, m.created_time,
            e.category
        FROM '{MARKETS_ALL}' m
        LEFT JOIN '{EVENTS_ALL}' e USING (event_ticker)
    """)

    return db, mo


@app.cell
def overview(db, mo):
    stats = db.sql("""
        SELECT
            count(*)                                         AS total_markets,
            count(*) FILTER (result != '' AND result IS NOT NULL) AS settled_markets,
            count(DISTINCT category)                         AS categories,
            sum(volume)                                      AS total_volume,
            min(created_time)                                AS earliest_created,
            max(created_time)                                AS latest_created
        FROM markets
    """).fetchone()

    mo.md(
        f"""
        # Longshot Bias — Descriptive Analysis (Full Universe)

        Querying **all non-MVE markets** from S3 via DuckDB, joined with events for categories.

        | Metric | Value |
        |--------|-------|
        | Total markets | {stats[0]:,} |
        | Settled markets | {stats[1]:,} |
        | Categories | {stats[2]} |
        | Total volume | {stats[3]:,.0f} |
        | Earliest created | {stats[4]} |
        | Latest created | {stats[5]} |
        """
    )
    return ()


@app.cell
def category_summary(db, mo):
    cat_summary = db.sql("""
        SELECT
            category,
            count(*)           AS market_count,
            avg(yes_ask)       AS avg_yes_ask,
            sum(volume)        AS total_volume,
            avg(open_interest) AS avg_open_interest
        FROM markets
        GROUP BY category
        ORDER BY market_count DESC
    """).df()

    mo.md("## Markets by Category")
    return (cat_summary,)


@app.cell
def show_category_table(cat_summary, mo):
    mo.ui.table(cat_summary, label="Category Summary")
    return ()


@app.cell
def price_distribution(db, mo):
    import altair as alt

    mo.md("## Distribution of Yes Ask Prices (settled markets)")

    df = db.sql("""
        SELECT yes_ask AS yes_ask_cents, category
        FROM markets
        WHERE yes_ask IS NOT NULL
          AND result IN ('yes', 'no')
    """).df()

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
    return alt, chart


@app.cell
def show_price_chart(chart, mo):
    mo.ui.altair_chart(chart)
    return ()


@app.cell
def calibration_analysis(db, mo):
    mo.md(
        """
        ## Longshot Bias Calibration

        Bin settled markets by `yes_ask` price, then compute the actual
        resolution rate vs the implied probability.  The favorite-longshot
        bias predicts that cheap contracts resolve Yes *less often* than
        their price implies.
        """
    )

    calibration = db.sql("""
        WITH settled AS (
            SELECT
                yes_ask AS yes_ask_cents,
                CASE WHEN result = 'yes' THEN 1 ELSE 0 END AS won
            FROM markets
            WHERE result IN ('yes', 'no')
              AND yes_ask IS NOT NULL
        ),
        binned AS (
            SELECT
                CASE
                    WHEN yes_ask_cents <= 5  THEN  2.5
                    WHEN yes_ask_cents <= 10 THEN  7.5
                    WHEN yes_ask_cents <= 15 THEN 12.5
                    WHEN yes_ask_cents <= 20 THEN 17.5
                    WHEN yes_ask_cents <= 30 THEN 25.0
                    WHEN yes_ask_cents <= 40 THEN 35.0
                    WHEN yes_ask_cents <= 50 THEN 45.0
                    WHEN yes_ask_cents <= 60 THEN 55.0
                    WHEN yes_ask_cents <= 70 THEN 65.0
                    WHEN yes_ask_cents <= 80 THEN 75.0
                    WHEN yes_ask_cents <= 90 THEN 85.0
                    ELSE 95.0
                END AS bin_midpoint,
                won
            FROM settled
        )
        SELECT
            bin_midpoint,
            bin_midpoint / 100.0 AS implied_prob,
            count(*)             AS n,
            avg(won)             AS actual_win_rate
        FROM binned
        GROUP BY bin_midpoint
        ORDER BY bin_midpoint
    """).df()

    return (calibration,)


@app.cell
def calibration_chart(alt, calibration, mo):
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
        alt.Chart(alt.Data(values=[{"x": 0, "y": 0}, {"x": 1, "y": 1}]))
        .mark_line(strokeDash=[5, 5], color="gray")
        .encode(x="x:Q", y="y:Q")
    )

    chart = (diagonal + points).properties(
        width=600,
        height=400,
        title="Calibration: Implied Probability vs Actual Win Rate",
    )

    mo.ui.altair_chart(chart)
    return ()


@app.cell
def flb_by_category(alt, db, mo):
    cat_calib = db.sql("""
        SELECT
            category,
            count(*)     AS n,
            avg(CASE WHEN result = 'yes' THEN 1 ELSE 0 END) AS actual_win_rate,
            avg(yes_ask) / 100.0 AS implied_prob,
            avg(yes_ask) / 100.0 - avg(CASE WHEN result = 'yes' THEN 1 ELSE 0 END) AS edge
        FROM markets
        WHERE result IN ('yes', 'no')
          AND yes_ask IS NOT NULL
          AND category IS NOT NULL
        GROUP BY category
        ORDER BY edge DESC
    """).df()

    chart = (
        alt.Chart(cat_calib)
        .mark_bar()
        .encode(
            alt.X("category:N", sort="-y", title="Category"),
            alt.Y("edge:Q", title="FLB Edge (implied - actual)"),
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
def volume_by_category(alt, db, mo):
    vol = db.sql("""
        SELECT
            category,
            count(*)    AS market_count,
            sum(volume) AS total_volume
        FROM markets
        WHERE volume IS NOT NULL
          AND category IS NOT NULL
        GROUP BY category
        ORDER BY total_volume DESC
    """).df()

    chart = (
        alt.Chart(vol)
        .mark_bar()
        .encode(
            alt.X("total_volume:Q", title="Total Volume (contracts)"),
            alt.Y("category:N", sort="-x", title="Category"),
            tooltip=[
                alt.Tooltip("category:N"),
                alt.Tooltip("total_volume:Q", format=",.0f", title="Volume"),
                alt.Tooltip("market_count:Q", title="Markets"),
            ],
        )
        .properties(width=700, height=400, title="Volume by Category")
    )

    mo.md("## Trade Volume by Category")
    mo.ui.altair_chart(chart)
    return ()


if __name__ == "__main__":
    app.run()
