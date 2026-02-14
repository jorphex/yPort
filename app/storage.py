import asyncio
import sqlite3
from datetime import datetime
from typing import Iterable, Optional, Dict, List


SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS users (
        platform TEXT NOT NULL,
        user_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (platform, user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS addresses (
        platform TEXT NOT NULL,
        user_id TEXT NOT NULL,
        address TEXT NOT NULL,
        ens_name TEXT,
        added_at TEXT NOT NULL,
        PRIMARY KEY (platform, user_id, address)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS user_settings (
        platform TEXT NOT NULL,
        user_id TEXT NOT NULL,
        daily_reports_enabled INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (platform, user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS usage_counters (
        date TEXT PRIMARY KEY,
        on_demand_reports INTEGER NOT NULL DEFAULT 0,
        daily_reports INTEGER NOT NULL DEFAULT 0
    )
    """,
]

class SQLiteStore:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()

    async def close(self) -> None:
        await asyncio.to_thread(self._conn.close)

    async def init(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._init_sync)

    def _init_sync(self) -> None:
        cursor = self._conn.cursor()
        for stmt in SCHEMA:
            cursor.execute(stmt)
        self._conn.commit()


    async def get_addresses(self, platform: str, user_id: str) -> List[dict]:
        async with self._lock:
            rows = await asyncio.to_thread(self._get_addresses_sync, platform, user_id)
        return [dict(row) for row in rows]

    def _get_addresses_sync(self, platform: str, user_id: str) -> List[sqlite3.Row]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT address, ens_name FROM addresses WHERE platform = ? AND user_id = ? ORDER BY added_at",
            (platform, user_id),
        )
        return cursor.fetchall()

    async def set_addresses(self, platform: str, user_id: str, addresses: Iterable[str], ens_map: Optional[Dict[str, str]] = None) -> None:
        ens_map = ens_map or {}
        timestamp = datetime.utcnow().isoformat()
        async with self._lock:
            await asyncio.to_thread(self._set_addresses_sync, platform, user_id, list(addresses), ens_map, timestamp)

    def _set_addresses_sync(self, platform: str, user_id: str, addresses: List[str], ens_map: Dict[str, str], timestamp: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO users (platform, user_id, created_at, updated_at) VALUES (?, ?, ?, ?)",
            (platform, user_id, timestamp, timestamp),
        )
        cursor.execute(
            "UPDATE users SET updated_at = ? WHERE platform = ? AND user_id = ?",
            (timestamp, platform, user_id),
        )
        cursor.execute(
            "DELETE FROM addresses WHERE platform = ? AND user_id = ?",
            (platform, user_id),
        )
        for address in addresses:
            cursor.execute(
                "INSERT OR REPLACE INTO addresses (platform, user_id, address, ens_name, added_at) VALUES (?, ?, ?, ?, ?)",
                (platform, user_id, address, ens_map.get(address), timestamp),
            )
        self._conn.commit()

    async def set_daily_reports(self, platform: str, user_id: str, enabled: bool) -> None:
        async with self._lock:
            await asyncio.to_thread(self._set_daily_reports_sync, platform, user_id, enabled)

    def _set_daily_reports_sync(self, platform: str, user_id: str, enabled: bool) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO user_settings (platform, user_id, daily_reports_enabled) VALUES (?, ?, ?)",
            (platform, user_id, 1 if enabled else 0),
        )
        self._conn.commit()

    async def get_daily_users(self, platform: str) -> List[dict]:
        async with self._lock:
            rows = await asyncio.to_thread(self._get_daily_users_sync, platform)
        return [dict(row) for row in rows]

    def _get_daily_users_sync(self, platform: str) -> List[sqlite3.Row]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT user_id FROM user_settings WHERE platform = ? AND daily_reports_enabled = 1",
            (platform,),
        )
        return cursor.fetchall()

    async def increment_usage(self, on_demand: int = 0, daily: int = 0) -> None:
        date_str = datetime.utcnow().date().isoformat()
        async with self._lock:
            await asyncio.to_thread(self._increment_usage_sync, date_str, on_demand, daily)

    def _increment_usage_sync(self, date_str: str, on_demand: int, daily: int) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            "INSERT OR IGNORE INTO usage_counters (date, on_demand_reports, daily_reports) VALUES (?, 0, 0)",
            (date_str,),
        )
        cursor.execute(
            "UPDATE usage_counters SET on_demand_reports = on_demand_reports + ?, daily_reports = daily_reports + ? WHERE date = ?",
            (on_demand, daily, date_str),
        )
        self._conn.commit()

    async def get_usage(self, date_str: str) -> Dict[str, int]:
        async with self._lock:
            row = await asyncio.to_thread(self._get_usage_sync, date_str)
        if not row:
            return {"on_demand_reports": 0, "daily_reports": 0}
        return dict(row)

    def _get_usage_sync(self, date_str: str) -> Optional[sqlite3.Row]:
        cursor = self._conn.cursor()
        cursor.execute("SELECT on_demand_reports, daily_reports FROM usage_counters WHERE date = ?", (date_str,))
        return cursor.fetchone()

    async def reset_usage(self, date_str: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._reset_usage_sync, date_str)

    def _reset_usage_sync(self, date_str: str) -> None:
        cursor = self._conn.cursor()
        cursor.execute(
            "UPDATE usage_counters SET on_demand_reports = 0, daily_reports = 0 WHERE date = ?",
            (date_str,),
        )
        self._conn.commit()
