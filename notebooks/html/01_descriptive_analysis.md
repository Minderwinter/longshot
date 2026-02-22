---
title: 01 Descriptive Analysis
marimo-version: 0.20.1
width: medium
header: |-
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
---

```python {.marimo name="setup"}
import marimo as mo
import altair as alt

from longshot.storage.athena import query
```

```python {.marimo name="overview"}
stats = query("""
    SELECT
        count(*)                                                    AS total_markets,
        count_if(result != '' AND result IS NOT NULL)               AS settled_markets,
        count(DISTINCT e.category)                                  AS categories,
        sum(m.volume)                                               AS total_volume,
        min(m.created_time)                                         AS earliest_created,
        max(m.created_time)                                         AS latest_created
    FROM markets m
    LEFT JOIN events e ON m.event_ticker = e.event_ticker
""")

s = stats.iloc[0]
mo.md(
    f"""
    # Longshot Bias — Descriptive Analysis

    Querying **all non-MVE markets** via Athena, joined with events for categories.

    | Metric | Value |
    |--------|-------|
    | Total markets | {int(s.total_markets):,} |
    | Settled markets | {int(s.settled_markets):,} |
    | Categories | {int(s.categories)} |
    | Total volume | {int(s.total_volume):,} |
    | Earliest created | {s.earliest_created} |
    | Latest created | {s.latest_created} |
    """
)
```

```python {.marimo name="category_summary"}
cat_summary = query("""
    SELECT
        e.category,
        count(*)            AS market_count,
        avg(m.yes_ask)      AS avg_yes_ask,
        sum(m.volume)       AS total_volume,
        avg(m.open_interest) AS avg_open_interest
    FROM markets m
    LEFT JOIN events e ON m.event_ticker = e.event_ticker
    GROUP BY e.category
    ORDER BY count(*) DESC
""")

mo.md("## Markets by Category")
mo.ui.table(cat_summary, label="Category Summary")
```

```python {.marimo name="price_distribution"}
mo.md("## Distribution of Yes Ask Prices (settled markets)")

price_df = query("""
    SELECT
        CAST(FLOOR(m.yes_ask / 2) * 2 AS INTEGER) AS price_bin,
        e.category,
        count(*) AS n
    FROM markets m
    LEFT JOIN events e ON m.event_ticker = e.event_ticker
    WHERE m.yes_ask IS NOT NULL
      AND m.yes_ask > 0
      AND m.result IN ('yes', 'no')
    GROUP BY FLOOR(m.yes_ask / 2) * 2, e.category
    ORDER BY price_bin
""")

price_chart = (
    alt.Chart(price_df)
    .mark_bar()
    .encode(
        alt.X("price_bin:Q", title="Yes Ask (cents)"),
        alt.Y("n:Q", title="Number of Markets"),
        alt.Color("category:N", title="Category"),
    )
    .properties(width=700, height=400, title="Yes Ask Price Distribution by Category")
)

mo.ui.altair_chart(price_chart)
```

```python {.marimo name="calibration_analysis"}
mo.md(
    """
    ## Longshot Bias Calibration

    Bin settled markets by `yes_ask` price, then compute the actual
    resolution rate vs the implied probability.  The favorite-longshot
    bias predicts that cheap contracts resolve Yes *less often* than
    their price implies.
    """
)

calibration = query("""
    WITH binned AS (
        SELECT
            CASE
                WHEN yes_ask <= 5  THEN  2.5
                WHEN yes_ask <= 10 THEN  7.5
                WHEN yes_ask <= 15 THEN 12.5
                WHEN yes_ask <= 20 THEN 17.5
                WHEN yes_ask <= 30 THEN 25.0
                WHEN yes_ask <= 40 THEN 35.0
                WHEN yes_ask <= 50 THEN 45.0
                WHEN yes_ask <= 60 THEN 55.0
                WHEN yes_ask <= 70 THEN 65.0
                WHEN yes_ask <= 80 THEN 75.0
                WHEN yes_ask <= 90 THEN 85.0
                ELSE 95.0
            END AS bin_midpoint,
            CASE WHEN result = 'yes' THEN 1 ELSE 0 END AS won
        FROM markets
        WHERE result IN ('yes', 'no')
          AND yes_ask IS NOT NULL
          AND yes_ask > 0
    )
    SELECT
        bin_midpoint,
        bin_midpoint / 100.0 AS implied_prob,
        count(*)             AS n,
        avg(won)             AS actual_win_rate
    FROM binned
    GROUP BY bin_midpoint
    ORDER BY bin_midpoint
""")

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

calib_chart = (diagonal + points).properties(
    width=600,
    height=400,
    title="Calibration: Implied Probability vs Actual Win Rate",
)

mo.ui.altair_chart(calib_chart)
```

```python {.marimo name="flb_by_category"}
cat_calib = query("""
    SELECT
        e.category,
        count(*)     AS n,
        avg(CASE WHEN m.result = 'yes' THEN 1 ELSE 0 END) AS actual_win_rate,
        avg(m.yes_ask) / 100.0 AS implied_prob,
        avg(m.yes_ask) / 100.0 - avg(CASE WHEN m.result = 'yes' THEN 1 ELSE 0 END) AS edge
    FROM markets m
    LEFT JOIN events e ON m.event_ticker = e.event_ticker
    WHERE m.result IN ('yes', 'no')
      AND m.yes_ask IS NOT NULL
      AND m.yes_ask > 0
      AND e.category IS NOT NULL
    GROUP BY e.category
    ORDER BY edge DESC
""")

flb_chart = (
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
mo.ui.altair_chart(flb_chart)
```

```python {.marimo name="volume_by_category"}
vol = query("""
    SELECT
        e.category,
        count(*)     AS market_count,
        sum(m.volume) AS total_volume
    FROM markets m
    LEFT JOIN events e ON m.event_ticker = e.event_ticker
    WHERE m.volume IS NOT NULL
      AND e.category IS NOT NULL
    GROUP BY e.category
    ORDER BY sum(m.volume) DESC
""")

vol_chart = (
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
mo.ui.altair_chart(vol_chart)
```