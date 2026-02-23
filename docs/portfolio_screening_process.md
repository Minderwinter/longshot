# Longshot Portfolio Screening Process

Directions for constructing a test portfolio to validate the favorite-longshot bias (FLB) on prediction markets. These steps are platform-general and assume access to market-level and event-level data from a prediction market exchange.

---

## Step 1: Define the Longshot Universe

Filter the full market catalog to isolate longshot candidates:

- **Price range**: Select markets where the Yes price (implied probability) is between 3% and 15%. Below 3% markets are too illiquid and dominated by noise; above 15% the FLB edge dissipates.
- **Days to expiration (DTE)**: Restrict to markets expiring within 1-14 days. Shorter horizons concentrate the bias — the market has less time to correct mispricing. Markets beyond 14 days carry too much fundamental uncertainty to exploit efficiently.
- **Investability filter**: Require minimum 24-hour trading volume (e.g., 100 contracts) to ensure you can realistically enter and exit positions. Markets with zero or negligible volume are uninvestable regardless of their theoretical edge.

The resulting set is your **investable longshot universe**.

## Step 2: Identify Mutually Exclusive (ME) Event Clusters

Most prediction market exchanges group related markets into **events** and flag whether the outcomes within an event are mutually exclusive (at most one can resolve Yes).

ME events are critical for portfolio construction because:

1. **Natural anti-correlation**: If one outcome wins, the others necessarily lose. Buying No across all outcomes in an ME event is inherently diversified.
2. **Collateral efficiency**: Many exchanges return excess collateral when you hold offsetting No positions in ME events. Instead of locking up capital equal to the sum of all No positions, you only lock up the maximum single-position cost. This can provide 5-20x capital efficiency for large ME clusters.

Tag every market in your universe with its event identifier and ME flag. Group ME markets by event to identify clusters.

## Step 3: Analyze Event Structure

Characterize the shape of your universe:

- **Markets per event**: Count how many investable longshot markets belong to each event. Single-market events offer no intra-event diversification. Large ME clusters (10+ markets from the same event, such as tournament or multi-runner race outcomes) are especially valuable.
- **Category distribution**: Examine which market categories (sports, politics, crypto, weather, entertainment, economics, etc.) contribute investable longshots and in what proportions.
- **Cohort analysis**: Split the universe by DTE bands (e.g., 1-7 days vs 8-14 days). Shorter-DTE cohorts tend to show stronger FLB edge but may have less volume; longer-DTE cohorts may have more capacity but weaker edge.

## Step 4: Estimate Collateral Efficiency

For each ME event cluster with 2+ investable longshot markets, compute:

- **Nominal cost**: Sum of No prices across all markets in the cluster (what you'd pay without ME collateral offsets).
- **Effective cost**: The maximum single No price in the cluster (what you actually lock up under ME collateral mechanics).
- **Efficiency ratio**: Nominal / Effective. Higher ratios mean more capital-efficient deployment.

Aggregate by category to understand which segments benefit most from ME mechanics.

## Step 5: Set Category-Level Allocation Heuristics

Not all categories carry equal edge or risk. Develop per-category rules based on:

- **FLB strength**: Categories where empirical analysis shows the largest gap between market-implied probability and true resolution probability deserve higher allocation.
- **ME prevalence**: Categories dominated by ME events (e.g., sports tournaments) get natural diversification and collateral efficiency, justifying higher weight even if per-market edge is moderate.
- **Correlation structure**: Some categories (e.g., politics) have high intra-category correlation — one event outcome may predict others. Cap exposure to avoid concentration risk.
- **Liquidity**: Categories with thin volume may have strong theoretical edge but can't absorb meaningful size. Weight allocation by investable capacity.
- **Skip weak categories**: If a category shows negligible FLB edge (gap between implied and true probability doesn't cover transaction costs), exclude it entirely.

## Step 6: Build the Sample Portfolio

Apply concrete selection rules:

1. **ME events**: Include all investable longshot markets within ME events. These are anti-correlated clusters — you want full coverage.
2. **Non-ME events**: Cap at a fixed number of markets per event (e.g., 3), ranked by trading volume. Without ME mechanics, holding many markets from the same non-ME event creates correlation without collateral benefit.
3. **Exclude weak categories**: Drop any category where expected edge doesn't cover spread and fee costs.
4. **Position sizing**: Use fixed uniform sizing (e.g., 1 No contract per market) for initial validation. The goal at this stage is to observe fill rates, slippage, and resolution outcomes — not to optimize returns.

## Step 7: Compute Portfolio Summary Statistics

Before deploying, verify the portfolio's properties:

- **Total market count** and distribution across categories
- **Number of independent events** (diversification proxy — more independent events = less tail risk)
- **Total nominal vs effective collateral** (after ME returns)
- **Expected P&L**: Using calibrated true probabilities (e.g., from historical FLB analysis), estimate expected profit per market and aggregate. A simple binomial model gives both expected value and a confidence interval on losses.
- **Concentration metrics**: What fraction of the portfolio sits in the largest category or single event? Flag if any single cluster dominates.

## Step 8: Deploy and Monitor

Execute the portfolio at minimum size to validate:

- **Fill rates**: What fraction of intended orders actually execute? Illiquid markets may not fill.
- **Slippage**: How much worse than the last traded price do you actually pay?
- **Resolution tracking**: Monitor outcomes over 2-4 weeks. Compare actual resolution rates to both implied probabilities and calibrated true probabilities.
- **Iterate**: Scale up sizing only after confirming that fills are reliable, slippage is manageable, and resolution rates align with FLB expectations.

---

## Key Principles

- **Start small**: The first deployment is for learning, not profit. Use minimum position sizes.
- **Exploit ME structure**: Mutually exclusive event clusters are the single biggest structural advantage — they provide diversification and capital efficiency simultaneously.
- **Diversify across categories**: Don't let any single category dominate, even if it appears to have the most capacity.
- **Pre-aggregate analysis**: Do as much computation server-side as possible. Pulling millions of raw market rows for client-side processing is wasteful and slow.
- **Heuristics over optimization**: At this stage, simple rules (fixed size, category caps, ME inclusion) outperform complex optimization. There isn't enough live data yet to calibrate a precise model.
