import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Tuple

from web3 import Web3

from .abis import ONE_UP_GAUGE_ABI
from .http import SharedHttpClient
from .web3_utils import Web3Manager

logger = logging.getLogger(__name__)

ONE_UP_API_URL = "https://1up.s3.pl-waw.scw.cloud/aprs.json"
YDAEMON_URL = "https://ydaemon.yearn.fi/vaults/detected?limit=2000"
KONG_URL = "https://kong.yearn.farm/api/gql"

class YearnApi:
    def __init__(self, http_client: SharedHttpClient, web3_manager: Web3Manager, cache_expiry_seconds: int) -> None:
        self._http = http_client
        self._web3_manager = web3_manager
        self._cache_expiry_seconds = cache_expiry_seconds
        self._cache = {
            "ydaemon": {"data": None, "timestamp": 0},
            "kong": {"data": {}, "timestamp": 0},
            "1up": {"data": None, "timestamp": 0},
            "1up_gauge_map": {"data": {}, "timestamp": 0},
        }

    def _is_fresh(self, key: str) -> bool:
        now = datetime.utcnow().timestamp()
        return self._cache[key]["data"] is not None and (now - self._cache[key]["timestamp"] < self._cache_expiry_seconds)

    async def update_ydaemon_cache(self) -> bool:
        logger.info("Updating yDaemon cache")
        try:
            session = self._http.session
            async with session.get(YDAEMON_URL, timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    self._cache["ydaemon"]["data"] = data
                    self._cache["ydaemon"]["timestamp"] = datetime.utcnow().timestamp()
                    logger.info("yDaemon cache updated: %s vaults", len(data))
                    return True
                logger.error("yDaemon fetch failed: status %s", response.status)
        except Exception as exc:
            logger.error("yDaemon fetch failed: %s", exc)
        return False

    async def update_kong_cache(self, vaults_to_update: list[Tuple[int, str]]) -> None:
        if not vaults_to_update:
            logger.info("No vaults provided for Kong cache update")
            return

        logger.info("Updating Kong cache for %s vaults", len(vaults_to_update))
        semaphore = asyncio.Semaphore(200)
        new_kong_data: Dict[Tuple[int, str], list] = {}

        async def fetch_one(chain_id: int, address: str) -> None:
            async with semaphore:
                data = await self.fetch_historical_pricepershare_kong(address, chain_id)
                if data:
                    new_kong_data[(chain_id, address.lower())] = data

        tasks = [fetch_one(chain_id, address) for chain_id, address in vaults_to_update]
        await asyncio.gather(*tasks)

        self._cache["kong"]["data"] = new_kong_data
        self._cache["kong"]["timestamp"] = datetime.utcnow().timestamp()
        logger.info("Kong cache updated for %s vaults", len(new_kong_data))

    async def update_1up_cache(self) -> bool:
        logger.info("Updating 1UP cache")
        try:
            session = self._http.session
            async with session.get(ONE_UP_API_URL, timeout=15) as response:
                if response.status != 200:
                    logger.error("1UP fetch failed: status %s", response.status)
                    return False
                content_type = response.headers.get("Content-Type", "").lower()
                if "application/json" not in content_type:
                    text = await response.text()
                    logger.error("1UP unexpected content type: %s (%s)", content_type, text[:200])
                    return False
                data = await response.json()
                if isinstance(data, dict) and "gauges" in data and isinstance(data["gauges"], dict):
                    processed = data.copy()
                    processed["gauges"] = {k.lower(): v for k, v in data["gauges"].items()}
                    self._cache["1up"]["data"] = processed
                    self._cache["1up"]["timestamp"] = datetime.utcnow().timestamp()
                    logger.info("1UP cache updated: %s gauges", len(processed["gauges"]))
                    return True
                logger.error("Unexpected 1UP data structure")
        except Exception as exc:
            logger.error("1UP fetch failed: %s", exc)
        return False

    async def update_1up_gauge_map_cache(self) -> bool:
        one_up_data = self._cache["1up"].get("data")
        if not one_up_data or "gauges" not in one_up_data:
            logger.warning("Cannot update 1UP gauge map: missing 1UP data")
            return False

        w3 = self._web3_manager.get_instance(1)
        if not w3:
            logger.error("Cannot update 1UP gauge map: Ethereum Web3 unavailable")
            return False

        gauges = list(one_up_data["gauges"].keys())
        gauge_map: Dict[str, str] = {}
        semaphore = asyncio.Semaphore(10)

        async def fetch_asset(gauge_address: str) -> None:
            if not Web3.is_address(gauge_address):
                logger.warning("Invalid 1UP gauge address: %s", gauge_address)
                return
            try:
                checksum = Web3.to_checksum_address(gauge_address)
                contract = w3.eth.contract(address=checksum, abi=ONE_UP_GAUGE_ABI)
                loop = asyncio.get_running_loop()
                async with semaphore:
                    asset_address = await loop.run_in_executor(None, contract.functions.asset().call)
                if asset_address and Web3.is_address(asset_address):
                    gauge_map[checksum.lower()] = Web3.to_checksum_address(asset_address).lower()
            except Exception as exc:
                logger.error("Failed 1UP asset() for %s: %s", gauge_address, exc)

        await asyncio.gather(*[fetch_asset(addr) for addr in gauges])

        if gauge_map:
            self._cache["1up_gauge_map"]["data"] = gauge_map
            self._cache["1up_gauge_map"]["timestamp"] = datetime.utcnow().timestamp()
            logger.info("1UP gauge map updated for %s gauges", len(gauge_map))
            return True

        logger.info("1UP gauge map update yielded no data")
        return False

    async def update_all_caches(self) -> None:
        ydaemon_ok = await self.update_ydaemon_cache()
        oneup_ok = await self.update_1up_cache()
        if oneup_ok:
            await self.update_1up_gauge_map_cache()
        if ydaemon_ok and self._cache["ydaemon"]["data"]:
            vaults_for_kong = {
                (vault.get("chainID"), vault.get("address"))
                for vault in self._cache["ydaemon"]["data"]
                if vault.get("chainID") and vault.get("address")
            }
            await self.update_kong_cache(list(vaults_for_kong))

    def get_ydaemon_data(self) -> Optional[list]:
        if self._is_fresh("ydaemon"):
            return self._cache["ydaemon"]["data"]
        logger.warning("yDaemon cache stale")
        return self._cache["ydaemon"]["data"]

    async def get_kong_data(self, vault_address: str, chain_id: int) -> Optional[list]:
        cache_key = (chain_id, vault_address.lower())
        if self._is_fresh("kong") and cache_key in self._cache["kong"]["data"]:
            return self._cache["kong"]["data"][cache_key]
        data = await self.fetch_historical_pricepershare_kong(vault_address, chain_id)
        if data:
            self._cache["kong"]["data"][cache_key] = data
            self._cache["kong"]["timestamp"] = datetime.utcnow().timestamp()
        return data

    def get_1up_data(self) -> Optional[dict]:
        if self._is_fresh("1up"):
            return self._cache["1up"]["data"]
        logger.warning("1UP cache stale")
        return self._cache["1up"].get("data")

    def get_1up_gauge_map(self) -> Optional[dict]:
        if self._is_fresh("1up_gauge_map"):
            return self._cache["1up_gauge_map"]["data"]
        logger.warning("1UP gauge map cache stale")
        return self._cache["1up_gauge_map"].get("data")

    async def fetch_historical_pricepershare_kong(self, vault_address: str, chain_id: int, limit: int = 1000) -> Optional[list]:
        query = """
        query Query($label: String!, $chainId: Int, $address: String, $component: String, $limit: Int) {
          timeseries(label: $label, chainId: $chainId, address: $address, component: $component, limit: $limit) {
            time
            value
          }
        }
        """
        variables = {
            "label": "pps",
            "chainId": chain_id,
            "address": vault_address,
            "component": "humanized",
            "limit": limit,
        }
        try:
            session = self._http.session
            async with session.post(KONG_URL, json={"query": query, "variables": variables}, timeout=20) as response:
                if response.status != 200:
                    logger.error("Kong fetch failed: %s", response.status)
                    return None
                data = await response.json()
                if "data" in data and "timeseries" in data["data"]:
                    timeseries = data["data"]["timeseries"]
                    if isinstance(timeseries, list):
                        return timeseries
        except Exception as exc:
            logger.error("Kong fetch error for %s: %s", vault_address, exc)
        return None

    def cache_timestamps(self) -> Dict[str, float]:
        return {
            "ydaemon": self._cache["ydaemon"]["timestamp"],
            "kong": self._cache["kong"]["timestamp"],
            "1up": self._cache["1up"]["timestamp"],
        }

    def cache_expiry_hours(self) -> Decimal:
        return Decimal(self._cache_expiry_seconds) / Decimal(3600)
