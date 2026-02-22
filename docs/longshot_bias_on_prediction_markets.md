# Systematic longshot selling on Kalshi: a quantitative playbook

**Buying "No" on overpriced low-probability contracts is the single most empirically validated edge in prediction markets today.** Two landmark studies—Bürgi, Deng & Whelan's analysis of 313,972 Kalshi contract prices and Jonathan Becker's dataset of 72.1 million trades covering $18.26 billion in volume—independently confirm a systematic favorite-longshot bias (FLB) where contracts priced below 10¢ lose over **60% of invested money** for Yes buyers. The flip side: No buyers at those same prices earn consistent positive returns. This bias has persisted in betting markets for 75+ years since Griffith first documented it in 1949, and while there are early signs it may be narrowing on Kalshi, it remains statistically significant across every category and every year of data. For a data scientist deploying $50K–$100K with API access and disciplined risk management, this strategy can target **8–20% annualized returns** with structurally capped downside—a fundamentally safer analog to the options-selling strategies it resembles.

---

## The empirical case is unusually strong

The evidence base for this strategy rests on two complementary academic studies plus decades of cross-market research, making it one of the best-documented edges in any retail-accessible market.

**Bürgi, Deng & Whelan (2026)**, published as CEPR Discussion Paper No. 20631, analyzed 156,986 Yes contracts across 46,282 outcomes from 12,403 Kalshi events (inception through April 2025). Their Mincer-Zarnowitz regressions show a positive ψ coefficient across every category—meaning low-priced contracts systematically overstate true probabilities. The overall average return on all Kalshi contracts is approximately **-20%**, but this masks a stark asymmetry: contracts priced above 70¢ show statistically significant positive post-fee returns, while contracts below 10¢ destroy capital at rates exceeding **-60%**. Takers average **-31.46%** returns versus **-9.64%** for makers, and makers buying contracts above 50¢ earn a **+2.6%** average return per trade (with 33% standard deviation).

**Becker (2026)** corroborates and extends these findings. His analysis shows 5¢ contracts win only **4.18%** of the time versus the 5% implied by their price—a 16.36% mispricing. At 1¢, takers win just **0.43%** versus 1% implied, a staggering -57% mispricing. The maker-taker wealth transfer is precise: takers lose **-1.12%** excess return on average while makers gain exactly **+1.12%**. Crucially, makers buying No earn **+1.25%** excess return versus +0.77% for makers buying Yes—confirming that the No side carries an additional structural edge beyond maker status alone.

The broader academic literature reinforces this. Snowberg & Wolfers (2010) showed the FLB persisted for over 50 years in horse racing across the US, UK, and Australia with returns of **-61%** at 100/1 odds. Page & Clemen (2013) found a simple strategy of buying >50¢ and selling <50¢ on InTrade yielded **9.55%** out-of-sample returns. The behavioral mechanism—probability overweighting as described by Prospect Theory—requires only a modest β parameter of 0.06–0.12 to explain the entire pattern, and Bürgi et al. demonstrate that a model with β=0 (no behavioral bias) cannot fit the data.

---

## Which categories offer the richest hunting ground

Not all Kalshi categories are created equal for longshot selling. The data reveals a clear hierarchy of opportunity that should directly inform portfolio allocation.

**Category-level FLB magnitude** from both studies paints a consistent picture. Becker's maker-taker gap analysis shows the widest spreads in **World Events (7.32 percentage points)**, **Media (7.28pp)**, and **Entertainment (4.79pp)**—these are the most behaviorally driven categories where emotional retail flow creates the most mispricing. **Weather (2.57pp)** and **Crypto (2.69pp)** occupy a middle tier. **Sports (2.23pp)** has a moderate gap but dominates in absolute volume, accounting for 89% of Kalshi's $263.5 million in 2025 fee revenue. **Finance is nearly perfectly efficient at just 0.17pp**, making it essentially worthless for this strategy.

Bürgi et al.'s regression analysis confirms Crypto has the strongest ψ coefficient at **0.058**, followed by the "Other" category at 0.053 and Financials at 0.032. The key tension is between bias magnitude and available liquidity:

- **Sports**: Moderate bias but enormous liquidity. Kalshi's weekly sports volume exceeds **$1 billion**. You can deploy meaningful capital here without moving the market, but competition from SIG and other institutional market makers is fiercest.
- **Entertainment/Culture**: Large bias but thin liquidity. Oscar, Grammy, and Billboard markets attract emotional retail bettors who massively overprice longshots, but order books may support only $500–$2,000 per position before significant slippage.
- **Weather**: Moderate bias with daily resolution (excellent for capital turnover), but extremely thin liquidity. Chris Dodds' live weather bot found only ~$20/week capacity per market before moving prices.
- **Economics**: Monthly resolution tied to CPI, jobs, and GDP releases. Moderate bias with decent liquidity around major data releases. These contracts are uncorrelated with sports, making them valuable for diversification.
- **Politics**: Strong FLB during election cycles, largely dormant otherwise. Highly correlated within category (a single political shock moves all political markets simultaneously).
- **Crypto**: Strongest statistical FLB but volatile, with prices subject to sudden regime changes. Hourly-reset markets were excluded from the academic studies, so available data covers only longer-duration crypto contracts.

**For portfolio construction, the optimal mix weights sports heavily for liquidity (40–50% of positions), supplements with economics and weather for uncorrelated daily/monthly resolution (20–30%), and selectively picks entertainment and political longshots when available (20–30%).** Avoid finance contracts entirely—the 0.17pp gap doesn't cover transaction costs.

---

## Screening, ranking, and identifying opportunities

Building a systematic scanner requires clear criteria for what makes a good longshot selling opportunity. The empirical data provides precise guidance on price thresholds, and Kalshi's API makes programmatic implementation straightforward.

**Price thresholds and edge by level.** The FLB is not linear—it accelerates dramatically at extreme prices. Based on Becker's calibration data:

| Yes Price | Actual Win Rate | Implied Win Rate | Mispricing | Strategy Attractiveness |
|-----------|----------------|-----------------|------------|------------------------|
| 1¢ | 0.43% | 1.0% | -57% | Extreme edge but near-zero volume |
| 3¢ | ~2.0% | 3.0% | ~-33% | High edge, limited volume |
| 5¢ | 4.18% | 5.0% | -16.4% | Sweet spot: strong edge, decent volume |
| 10¢ | ~7.5% | 10.0% | ~-25% | Good edge, better volume |
| 15¢ | ~12% | 15.0% | ~-20% | Moderate edge, best volume |
| 20¢+ | Approaches fair | 20%+ | Diminishing | Below minimum edge threshold |

**The sweet spot for systematic longshot selling is Yes contracts priced at 3–12¢ (No at 88–97¢).** Below 3¢, volume is too thin. Above 15¢, the bias narrows to levels where fees and estimation error can eliminate the edge.

**Opportunity ranking metrics** should incorporate five factors, weighted roughly as follows:

1. **Edge percentage (40% weight)**: Estimated as (market Yes price − true probability) / market Yes price. Minimum threshold: 15% relative edge (e.g., market at 5¢, you estimate ≤4.25¢ true probability). Use historical base rates from Becker's dataset for similar contract types as your probability anchor.
2. **Liquidity score (25% weight)**: Minimum $1,000 in recent 24-hour volume on the relevant side. Check order book depth at your target No price—can you get filled for your desired position size without moving the market more than 1¢?
3. **Time to resolution (15% weight)**: Shorter is better for capital turnover. A 7-day contract at 3% edge compounds much faster than a 90-day contract at 5% edge. Target: 7–30 days optimal.
4. **Correlation penalty (10% weight)**: Discount contracts that share macro risk factors with existing portfolio positions. Map each contract to 5–10 macro factors (recession risk, political party, specific sport league, interest rates, geopolitical tension).
5. **Category diversification bonus (10% weight)**: Favor contracts in underrepresented categories to maintain balance.

**API implementation for scanning.** Kalshi's REST API at `https://api.elections.kalshi.com/trade-api/v2` provides everything needed. The key endpoint is `GET /markets` with `status=open`, paginating through all active markets (up to 1,000 per page). For each market, check `yes_price` against your threshold. The `GET /markets/{ticker}/orderbook` endpoint provides depth data for liquidity assessment. Rate limits at the Basic tier (20 reads/sec) are sufficient for a scan running every few minutes. The official `kalshi_python` SDK and community tools like `mickbransfield/kalshi` on GitHub provide ready-made scaffolding. Becker's open-source dataset (`Jon-Becker/prediction-market-analysis` on GitHub) with Parquet files covering June 2021–November 2025 is the single most valuable resource for backtesting and calibrating your base-rate estimates.

---

## Entry, exit, and position sizing rules

A systematic strategy requires precise, pre-committed rules to prevent behavioral drift. The following framework integrates Kelly criterion sizing with practical constraints specific to prediction market structure.

**Position sizing via fractional Kelly.** For a No contract with market Yes price p_m and your estimated true probability q of the event occurring, the Kelly fraction is:

```
f* = (p_m − q) / p_m
```

For a 5¢ Yes contract where you estimate 3% true probability: f* = (0.05 − 0.03) / 0.05 = **40% of bankroll**. This is far too aggressive. Full Kelly implies a 33% probability of halving your bankroll before doubling it. **Use quarter-Kelly (f = 0.25 × f*) as your baseline**, which captures ~50% of optimal growth with dramatically reduced drawdowns. In this example: 0.25 × 40% = **10% maximum allocation**.

Hard caps override Kelly regardless of output: **no single position exceeding 5% of bankroll, no single category exceeding 15%, and maximum 70% of total capital deployed** with 30% held as reserve.

| Yes Price | No Price | True Prob (est.) | Full Kelly | Quarter Kelly | Max Position ($100K) |
|-----------|----------|-----------------|------------|---------------|---------------------|
| 3¢ | 97¢ | 2% | 33% | 8.3% | $5,000 (cap) |
| 5¢ | 95¢ | 3% | 40% | 10.0% | $5,000 (cap) |
| 8¢ | 92¢ | 5% | 37.5% | 9.4% | $5,000 (cap) |
| 10¢ | 90¢ | 7% | 30% | 7.5% | $5,000 (cap) |
| 15¢ | 85¢ | 11% | 26.7% | 6.7% | $5,000 (cap) |

**Entry execution: use maker (limit) orders exclusively.** Maker fees are exactly 25% of taker fees (0.0175 × C × P × (1−P) versus 0.07). At extreme prices, both round to $0.01 per contract, but at moderate extremes (80–90¢ No), the savings compound meaningfully across hundreds of contracts. Place limit orders 1–2¢ inside the current best bid for No. Be patient—in thin markets, fills may take hours or days, which is acceptable for a systematic strategy.

**Exit rules before resolution.** Define three escalating triggers:

- **Yellow alert (Yes rises to 20–25¢)**: Review the position. Has genuinely new information emerged, or is this noise/thin-market volatility? If new information, reduce by 50%. If noise, hold.
- **Orange alert (Yes rises to 30–35¢)**: Mandatory exit of at least 50% of the position. Your original edge thesis is likely compromised.
- **Red alert (Yes rises to 50¢+)**: Full exit. The event is now a coin flip; your longshot-selling edge is gone entirely.

**Time decay works in your favor** but differently than options theta. As time passes without the event occurring, Yes prices naturally drift toward zero, increasing your unrealized profit. Near resolution, prices snap to 0 or $1 rapidly. The key risk window is mid-life: early enough that new information can emerge but late enough that you've committed capital for weeks. Contracts with **7–30 day resolution** optimize the tradeoff between capital turnover speed and information risk.

---

## Risk management: making the steamroller smaller

The "picking up pennies in front of a steamroller" analogy is the most important objection to this strategy. The structural comparison to options selling reveals both why the analogy partially applies and why prediction markets make the steamroller fundamentally smaller and more predictable.

**Critical structural advantages over options selling.** In options, selling out-of-the-money puts or calls exposes you to theoretically unlimited losses (naked calls), margin calls that force liquidation at the worst possible time, and complex Greeks exposure (delta, gamma, vega all moving against you in a crisis). In prediction markets, **your maximum loss per contract is the No price you paid—period.** Buying No at 95¢ means your worst case is losing 95¢, with no margin calls, no forced liquidation cascades, and no volatility surface risk. The payoff is purely binary: $1 or $0. This eliminates the primary destruction mechanism in options-selling blowups, where forced liquidation at inflated implied volatility causes losses far exceeding the original position.

**Tail risk from correlated losses is the primary danger.** If you hold 50 No positions and a macro shock causes 10 to resolve Yes simultaneously, you lose approximately **$14,000 on a $100K bankroll** (assuming $1,400 average position size)—painful but survivable. The stress test matrix:

| Scenario | Positions Lost | Approximate Loss | % of $100K Bankroll |
|----------|---------------|-----------------|-------------------|
| Normal month (2–3 independent losses) | 2–3 | $2,800–$4,200 | 2.8–4.2% |
| Mild stress (5 correlated losses) | 5 | $7,000 | 7% |
| Severe stress (10 correlated losses) | 10 | $14,000 | 14% |
| Black swan (15+ correlated losses) | 15 | $21,000 | 21% |

The scenarios that trigger correlated longshot realization include: geopolitical shocks (war, assassination) cascading across political and economic contracts simultaneously; macro economic surprises affecting all Fed, CPI, and GDP markets; playoff upsets clustering within a single sports tournament; and platform operational risk (Kalshi experienced a payout dispute during January 2026 NFL markets that was only resolved after public pressure).

**Portfolio-level VaR for independent positions.** For N independent Bernoulli positions with average loss probability p and average loss amount L, the 95th percentile loss follows a Binomial distribution. With 50 positions at p=0.05 and L=$1,400: expected loss = **$3,500/period**, 95% VaR ≈ **$7,000**, 99% VaR ≈ **$9,800**. With modest correlation (ρ=0.15 average pairwise), multiply by approximately √(1 + (N−1)ρ), increasing 95% VaR to roughly **$10,000**. Set maximum drawdown tolerance at 20–25%; with quarter-Kelly sizing and 50+ diversified positions, this should be breached less than 5% of the time.

**Category concentration limits** provide the primary correlation firewall. No single category should exceed 15% of deployed capital. Within categories, map positions to shared macro factors and limit exposure to any single factor to 20% of the portfolio. Monthly stress tests should simulate simultaneous loss of all positions in the two largest correlated clusters—if the combined loss exceeds your drawdown tolerance, reduce the larger cluster.

---

## Transaction costs favor extreme prices

Kalshi's fee formula—**$0.07 × C × P × (1−P)** for takers, exactly one-quarter that for makers—creates a convex curve that works in the longshot seller's favor. The P×(1−P) term peaks at 50¢ and falls symmetrically toward zero at both extremes.

| Yes Price (P) | No Price | Taker Fee/Contract | Maker Fee/Contract | Fee as % of No Profit |
|--------------|----------|-------------------|-------------------|----------------------|
| 3¢ | 97¢ | $0.01 | $0.01 | 33% of 3¢ profit |
| 5¢ | 95¢ | $0.01 | $0.01 | 20% of 5¢ profit |
| 10¢ | 90¢ | $0.01 | $0.01 | 10% of 10¢ profit |
| 15¢ | 85¢ | $0.01 | $0.01 | 6.7% of 15¢ profit |
| 20¢ | 80¢ | $0.02 | $0.01 | 5–10% of 20¢ profit |

At 3¢ Yes contracts, the $0.01 fee consumes a full third of the 3¢ per-contract profit on a win—this is where fees become the binding constraint. **The breakeven analysis shows that contracts priced below approximately 3¢ have insufficient edge to overcome fees** unless the FLB is extremely pronounced (>50% mispricing, which Becker's data confirms at 1¢ but with near-zero volume). The practical fee-adjusted sweet spot remains **5–12¢ Yes contracts**.

**Interest income as a return floor.** Kalshi pays **3.25% APY** on both idle cash and collateral backing open positions, calculated daily and paid monthly. For a $100K account with $70K deployed in No positions, this generates approximately **$3,250/year** in risk-free income—a meaningful contribution that effectively subsidizes the strategy's base return. This interest rate has fluctuated between 3.25% and 4.05% over the past year, tracking Fed funds rate movements.

**Collateral return on mutually exclusive markets** is the most important capital efficiency mechanism. When buying No across multiple outcomes in a mutually exclusive group (e.g., "Which team will win the Super Bowl?"), Kalshi automatically returns excess collateral since at most one outcome can resolve Yes. Buying No on 10 teams at 95¢ each would normally lock up $9.50, but since your maximum loss is $1 (one team wins), Kalshi returns $8.50. This can increase effective capital efficiency by up to **10x** and should be aggressively exploited in sports tournament and multi-candidate markets.

---

## Building the infrastructure

A production longshot-selling system requires four components: scanner, execution engine, portfolio monitor, and risk dashboard. Multiple open-source implementations provide starting points.

**Scanner architecture.** Poll `GET /markets` every 5 minutes, filtering for `status=open` and `yes_price ≤ 15`. For each candidate, fetch `GET /markets/{ticker}/orderbook` to assess depth, and query your local database of historical base rates (bootstrapped from Becker's Parquet dataset) to estimate true probability. Score candidates using the five-factor ranking model described earlier. The official `kalshi_python` SDK handles authentication via RSA keys and provides typed wrappers for all endpoints.

**Execution engine.** Place maker (limit) orders via `POST /portfolio/orders`, targeting No prices 1–2¢ inside the best bid. Kalshi supports up to **200,000 simultaneous open orders** per account and batch operations via `POST /portfolio/orders/batch`. At the Basic API tier (20 reads/sec, 10 writes/sec), you can comfortably manage 50+ positions with margin. For serious deployment, apply for Advanced tier (30/30) or Premier tier (100/100).

**Portfolio monitor.** Track positions in a local SQLite or DuckDB database, syncing with `GET /portfolio/positions` and `GET /portfolio/fills` (which now includes `fee_cost` as of January 2026). Build alerts via Telegram Bot API or Discord webhooks for: price moves exceeding yellow/orange/red thresholds, approaching resolution times, new high-scoring scanner candidates, and daily P&L summaries. Open-source implementations worth examining include `ryanfrigo/kalshi-ai-trading-bot` (5-LLM ensemble with Kelly sizing, max 15 concurrent positions, SQLite tracking) and `OctagonAI/kalshi-deep-trading-bot` (with hedging built in at 0.25 hedge ratio and dry-run mode).

**Tax record-keeping.** Kalshi does not provide comprehensive broker-style reporting for trading activity—you must reconstruct contract-level gains and losses yourself. Maintain a complete transaction log with entry price, exit price/resolution, fees, and dates. The tax treatment remains legally uncertain: most advisors recommend reporting as ordinary income on Schedule 1, Line 8z. Under the One Big Beautiful Bill Act (signed July 4, 2025), if prediction market profits are classified as gambling income, loss deductions are capped at **90%** starting January 1, 2026—a potentially significant headwind. Section 1256 treatment (60% long-term/40% short-term capital gains) is theoretically supportable for CFTC-regulated contracts but requires willingness to defend under audit.

---

## The bias is real but the clock may be ticking

The most important question for strategy longevity is whether the FLB will persist as prediction markets mature. The evidence is mixed but cautiously optimistic for the medium term.

**Signs the bias is narrowing.** Bürgi et al.'s year-by-year analysis shows the ψ coefficient declining from 0.048 in 2024 to **0.021 in 2025** (though still statistically significant). Vandenbruaene et al. (2025) document a significant decrease in the FLB in sports betting between 2000 and 2023 as transaction costs dropped over 60%. Becker's data shows the maker-taker dynamic literally reversed between 2021–2023 (when takers earned +2.0%) and post-October 2024 (when makers earn +2.5%)—the entry of SIG and Jump Trading as institutional market makers coincided with a **5.3 percentage point swing** in who captures the surplus.

**Signs the bias will persist.** The behavioral mechanism (probability overweighting per Prospect Theory) is deeply rooted in human cognition, not correctable through market design alone. Meyer & Hundtofte (2023) showed the bias disappears when gambles are presented in isolation—but prediction market interfaces inherently emphasize payoffs over probabilities, perpetuating the framing that drives overpricing. More critically, the entry of DraftKings, FanDuel, Robinhood, and other retail platforms into prediction markets is bringing waves of new, unsophisticated longshot buyers. Kalshi's volume surged from $30M in Q3 2024 to **$820M in Q4 2024** and exceeded **$6 billion** in December 2025 alone—each new retail participant represents fresh longshot-buying flow.

**Notable exception: Polymarket.** Reichenbach & Walther (2025) analyzed 124 million Polymarket trades and found **no evidence of a general longshot bias**, suggesting that Polymarket's crypto-native, information-dense user base may already be sophisticated enough to eliminate the bias. This is a cautionary data point: if Kalshi's user base evolves similarly, the edge could disappear. However, Kalshi's integration with Robinhood (which contributes over 50% of trading volume) is pulling in a distinctly retail, entertainment-oriented demographic—the exact population most prone to longshot overpricing.

**Regulatory risk deserves attention.** Over 20 lawsuits and cease-and-desist orders from state regulators challenge Kalshi's sports contracts. Massachusetts issued a preliminary injunction in January 2026, and Nevada's appeals court denied Kalshi's request to block state action as of February 2026. On the federal side, new CFTC Chairman Selig announced support for prediction markets and withdrew proposed bans—but state-level restrictions could meaningfully reduce the addressable market, particularly for sports longshots which represent 89% of volume.

---

## Putting it all together: the complete parameter set

For a data scientist deploying $100K with a moderate risk tolerance, the following configuration balances empirical evidence with practical constraints:

| Parameter | Setting | Rationale |
|-----------|---------|-----------|
| Kelly fraction | 0.25× (quarter Kelly) | Tail probability estimation is inherently uncertain |
| Yes price range | 3–12¢ | Below 3¢: fees dominate; above 12¢: FLB insufficient |
| Minimum relative edge | 15% | Market at 5¢, true prob ≤ 4.25¢ |
| Max single position | $5,000 (5%) | Hard cap regardless of Kelly output |
| Max category exposure | 15% | Correlation firewall |
| Total capital deployed | 70% ($70K) | 30% reserve for opportunities and safety |
| Target positions | 40–60 concurrent | Diversification with manageable monitoring |
| Order type | Maker (limit) only | 75% fee reduction, better fills |
| Stop-loss | Exit 50% at Yes=30¢, 100% at Yes=50¢ | Preserves capital when thesis breaks |
| Rebalancing | Weekly scan + opportunistic | New positions as contracts resolve |
| Max drawdown tolerance | 20% ($20K) | Triggers strategy pause if breached |
| Expected annual return | 12–18% | After fees, losses, and interest income |

**The expected return calculation for the base case:** 50 positions averaging $1,400 each ($70K deployed), average No price 93¢, average edge 2.5 percentage points, 3-week average resolution enabling ~17 portfolio turns per year. Per position: 95% × $98 profit − 5% × $1,400 loss = **$23 expected profit** per position per cycle. Across 50 positions and 17 cycles: $23 × 50 × 17 = **$19,550 gross**. Subtract ~15% for estimation error and slippage: **$16,600 net trading profit**. Add $3,250 in interest income. **Total: approximately $19,850, or 19.9% on $100K.** In practice, conservative execution and occasional black swan losses will reduce this to the **12–18% range**.

## Conclusion

This strategy works because it sits at the intersection of a well-documented behavioral bias, a structural market feature (Kalshi's fee formula amplifies mispricing at extremes), and a practical advantage (capped binary losses eliminate the margin-call destruction mechanism that makes analogous options strategies dangerous). The three non-obvious insights that should shape implementation are: first, **being a maker matters more than picking the right contracts**—the 2.24 percentage point maker-taker gap dwarfs category selection effects; second, **collateral return on mutually exclusive markets is the key to capital efficiency**, potentially increasing effective returns by 3–5x in tournament-style markets; and third, **the strategy's biggest enemy is not a single black swan but correlated cluster losses**, making macro-factor mapping and concentration limits more important than any individual position's Kelly sizing. The window of peak opportunity is likely the next 2–3 years, as retail volume growth from Robinhood integration and new platform entrants continues to replenish the pool of biased longshot buyers faster than institutional market makers can drain it.