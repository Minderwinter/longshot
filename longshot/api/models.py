"""Pydantic v2 models for Kalshi API responses."""

from __future__ import annotations

from pydantic import BaseModel


class Market(BaseModel):
    ticker: str
    event_ticker: str
    title: str
    status: str
    yes_bid: float | None = None
    yes_ask: float | None = None
    no_bid: float | None = None
    no_ask: float | None = None
    last_price: float | None = None
    volume: int | None = None
    volume_24h: int | None = None
    open_interest: int | None = None
    close_time: str | None = None
    open_time: str | None = None
    category: str | None = None
    result: str | None = None
    created_time: str | None = None


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


class TradesResponse(BaseModel):
    trades: list[Trade]
    cursor: str | None = None
