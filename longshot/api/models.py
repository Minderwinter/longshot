"""Pydantic v2 models for Kalshi API responses."""

from __future__ import annotations

from pydantic import BaseModel


class Market(BaseModel):
    ticker: str
    event_ticker: str
    title: str
    status: str
    market_type: str | None = None
    subtitle: str | None = None
    yes_sub_title: str | None = None
    no_sub_title: str | None = None
    series_ticker: str | None = None
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    last_price: float | None = None
    previous_yes_bid: float | None = None
    previous_yes_ask: float | None = None
    previous_price: float | None = None
    volume: int | None = None
    volume_24h: int | None = None
    open_interest: int | None = None
    notional_value: int | None = None
    close_time: str | None = None
    open_time: str | None = None
    expiration_time: str | None = None
    expected_expiration_time: str | None = None
    latest_expiration_time: str | None = None
    created_time: str | None = None
    updated_time: str | None = None
    result: str | None = None
    settlement_value: int | None = None
    can_close_early: bool | None = None
    strike_type: str | None = None
    rules_primary: str | None = None
    rules_secondary: str | None = None


class MarketsResponse(BaseModel):
    markets: list[Market]
    cursor: str | None = None


class Trade(BaseModel):
    trade_id: str
    ticker: str
    yes_price: float
    no_price: float
    count: int
    taker_side: str | None = None
    created_time: str | None = None
    ts: int | None = None


class TradesResponse(BaseModel):
    trades: list[Trade]
    cursor: str | None = None
