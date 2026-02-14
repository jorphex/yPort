import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import time

def _parse_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    v = value.strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default

def _parse_int(value: str, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default

def _parse_time_hhmm(value: str, default: time) -> time:
    if not value:
        return default
    parts = value.strip().split(":")
    if len(parts) != 2:
        return default
    try:
        hour = int(parts[0])
        minute = int(parts[1])
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return time(hour=hour, minute=minute)
    except ValueError:
        return default
    return default

def _parse_decimal(value: str, default: Decimal) -> Decimal:
    if value is None:
        return default
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return default

@dataclass(frozen=True)
class Config:
    alchemy_api_key: str
    telegram_bot_token: str
    telegram_admin_chat_id: str
    discord_bot_token: str
    discord_public_channel_id: int
    discord_log_channel_id: int
    discord_admin_user_id: int
    cache_expiry_seconds: int
    rate_limit_seconds: int
    daily_report_time_utc: time
    enable_telegram: bool
    enable_discord: bool
    veyfi_deprecation_message: str
    db_path: str
    min_suggestion_tvl_usd: Decimal
    suggestion_apr_threshold: Decimal


def load_config() -> Config:
    cache_expiry = _parse_int(os.environ.get("CACHE_EXPIRY_SECONDS"), 3 * 60 * 60)
    rate_limit = _parse_int(os.environ.get("RATE_LIMIT_SECONDS"), 10)
    daily_time = _parse_time_hhmm(os.environ.get("DAILY_REPORT_TIME_UTC"), time(hour=6, minute=0))

    return Config(
        alchemy_api_key=os.environ.get("ALCHEMY_API_KEY", "").strip(),
        telegram_bot_token=os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
        telegram_admin_chat_id=os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "").strip(),
        discord_bot_token=os.environ.get("DISCORD_BOT_TOKEN", "").strip(),
        discord_public_channel_id=_parse_int(os.environ.get("DISCORD_PUBLIC_CHANNEL_ID"), 0),
        discord_log_channel_id=_parse_int(os.environ.get("DISCORD_LOG_CHANNEL_ID"), 0),
        discord_admin_user_id=_parse_int(os.environ.get("DISCORD_ADMIN_USER_ID"), 0),
        cache_expiry_seconds=cache_expiry,
        rate_limit_seconds=rate_limit,
        daily_report_time_utc=daily_time,
        enable_telegram=_parse_bool(os.environ.get("ENABLE_TELEGRAM"), True),
        enable_discord=_parse_bool(os.environ.get("ENABLE_DISCORD"), True),
        veyfi_deprecation_message=os.environ.get(
            "VEYFI_DEPRECATION_MESSAGE",
            "veYFI staking is deprecated. If you have Yearn gauge deposits, consider unstaking and migrating per Yearn guidance.",
        ).strip(),
        db_path=os.environ.get("DB_PATH", "yport.db"),
        min_suggestion_tvl_usd=_parse_decimal(os.environ.get("MIN_SUGGESTION_TVL_USD"), Decimal("50000")),
        suggestion_apr_threshold=_parse_decimal(os.environ.get("SUGGESTION_APR_THRESHOLD"), Decimal("5.0")),
    )
