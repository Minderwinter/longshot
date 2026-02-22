"""Load .env and expose typed Settings singleton."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    s3_prefix: str
    aws_region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    kalshi_api_key_id: str
    kalshi_private_key: str
    kalshi_base_url: str = "https://api.elections.kalshi.com/trade-api/v2"


def _load_settings() -> Settings:
    def _env(key: str) -> str:
        val = os.environ.get(key)
        if val is None:
            raise RuntimeError(f"Missing required env var: {key}")
        return val

    # .env stores RSA key with literal \n — python-dotenv does NOT convert them
    raw_key = _env("KALSHI_PRIVATE_KEY")
    private_key = raw_key.replace("\\n", "\n")

    return Settings(
        s3_bucket=_env("S3_BUCKET"),
        s3_prefix=_env("S3_PREFIX"),
        aws_region=_env("AWS_REGION"),
        aws_access_key_id=_env("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY"),
        kalshi_api_key_id=_env("KALSHI_API_KEY_ID"),
        kalshi_private_key=private_key,
    )


SETTINGS = _load_settings()
