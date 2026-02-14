import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, getcontext
from typing import Dict, List, Optional, Tuple

from web3 import Web3

from .balances import fetch_balances_for_eoa_on_chain
from .chains import CHAIN_NAMES, SUPPORTED_CHAINS
from .yearn_api import YearnApi
from .web3_utils import Web3Manager
from .http import SharedHttpClient
from .config import Config

logger = logging.getLogger(__name__)
getcontext().prec = 28


@dataclass
class VaultEntry:
    chain_id: int
    display_name: str
    token_symbol: str
    vault_url: str
    vault_usd_value: Decimal
    vault_apr_percent: Decimal
    yield_7d: Decimal
    yield_30d: Decimal
    usd_change_7d: Decimal
    usd_change_30d: Decimal
    staked_status: str
    staked_indicator_url: Optional[str]
    current_staking_apr_percent: Decimal
    current_staking_apr_source: str

@dataclass
class ChainReport:
    chain_id: int
    chain_name: str
    vaults: List[VaultEntry]
    total_usd: Decimal
    avg_apr: Decimal
    avg_yield_7d: Decimal
    avg_yield_30d: Decimal
    total_usd_change_7d: Decimal
    total_usd_change_30d: Decimal

@dataclass
class OverallSummary:
    total_usd: Decimal
    avg_apr: Decimal
    avg_yield_7d: Decimal
    avg_yield_30d: Decimal
    total_usd_change_7d: Decimal
    total_usd_change_30d: Decimal

@dataclass
class SuggestionEntry:
    chain_id: int
    chain_name: str
    display_name: str
    token_symbol: str
    vault_url: str
    base_apr: Decimal
    apr_difference: Decimal
    tvl: Decimal

@dataclass
class ReportData:
    chains: List[ChainReport]
    overall: OverallSummary
    suggestions: List[SuggestionEntry]
    cache_note: str
    has_yearn_gauge_deposit: bool
    empty: bool

class ReportService:
    def __init__(self, config: Config, yearn_api: YearnApi, web3_manager: Web3Manager, http_client: SharedHttpClient) -> None:
        self._config = config
        self._yearn = yearn_api
        self._web3 = web3_manager
        self._http = http_client

    async def generate(self, addresses: List[str]) -> ReportData:
        all_vaults = self._yearn.get_ydaemon_data()
        one_up_data = self._yearn.get_1up_data()
        one_up_gauge_map = self._yearn.get_1up_gauge_map()

        if not all_vaults:
            raise RuntimeError("Vault data unavailable")

        one_up_vault_to_gauge = {v: k for k, v in (one_up_gauge_map or {}).items()}

        balances_by_chain: Dict[int, Dict[str, Dict[str, str]]] = {}
        balance_tasks = []
        w3_instances: Dict[int, Web3] = {}

        for chain_id in SUPPORTED_CHAINS:
            balances_by_chain[chain_id] = {}
            if chain_id != 1:
                w3 = self._web3.get_instance(chain_id)
                if w3:
                    w3_instances[chain_id] = w3
            for eoa in addresses:
                balances_by_chain[chain_id][eoa] = {}
                balance_tasks.append(
                    fetch_balances_for_eoa_on_chain(
                        eoa=eoa,
                        chain_id=chain_id,
                        vaults_data=all_vaults,
                        w3_instance=w3_instances.get(chain_id),
                        session=self._http.session,
                        api_key=self._config.alchemy_api_key,
                    )
                )

        results = await asyncio.gather(*balance_tasks, return_exceptions=True)
        idx = 0
        for chain_id in SUPPORTED_CHAINS:
            for eoa in addresses:
                result = results[idx]
                if isinstance(result, dict):
                    balances_by_chain[chain_id][eoa] = result
                else:
                    balances_by_chain[chain_id][eoa] = {}
                idx += 1

        portfolio_by_chain: Dict[int, Dict[str, object]] = {}
        report_vaults_details = []
        vaults_requiring_kong: set[Tuple[int, str]] = set()
        has_yearn_gauge_deposit = False

        for vault in all_vaults:
            vault_address_lower = vault.get("address", "").lower()
            chain_id = vault.get("chainID")
            if not vault_address_lower or not chain_id or chain_id not in SUPPORTED_CHAINS:
                continue

            yearn_gauge_address_lower = None
            yearn_staking_available = vault.get("staking", {}).get("available", False)
            if yearn_staking_available and vault.get("staking", {}).get("address"):
                yg_addr = vault["staking"]["address"]
                if Web3.is_address(yg_addr):
                    yearn_gauge_address_lower = Web3.to_checksum_address(yg_addr).lower()

            one_up_gauge_address_lower = one_up_vault_to_gauge.get(vault_address_lower)
            one_up_staking_available = one_up_gauge_address_lower is not None

            vault_balance_hex = "0x0"
            yearn_gauge_balance_hex = "0x0"
            one_up_gauge_balance_hex = "0x0"

            for eoa in addresses:
                eoa_balances = balances_by_chain.get(chain_id, {}).get(eoa, {})
                bal = eoa_balances.get(vault_address_lower)
                if bal and int(bal, 16) > 0:
                    vault_balance_hex = hex(int(vault_balance_hex, 16) + int(bal, 16))
                if yearn_gauge_address_lower:
                    bal = eoa_balances.get(yearn_gauge_address_lower)
                    if bal and int(bal, 16) > 0:
                        yearn_gauge_balance_hex = hex(int(yearn_gauge_balance_hex, 16) + int(bal, 16))
                if one_up_gauge_address_lower:
                    bal = eoa_balances.get(one_up_gauge_address_lower)
                    if bal and int(bal, 16) > 0:
                        one_up_gauge_balance_hex = hex(int(one_up_gauge_balance_hex, 16) + int(bal, 16))

            potential_yearn_staking_apr = Decimal("0")
            if yearn_staking_available:
                rewards_list = vault.get("staking", {}).get("rewards", [])
                apr_extra = vault.get("apr", {}).get("extra", {})
                yrn_apr_val = None
                if rewards_list and rewards_list[0].get("apr") is not None:
                    yrn_apr_val = rewards_list[0]["apr"]
                elif apr_extra and apr_extra.get("stakingRewardsAPR") is not None:
                    yrn_apr_val = apr_extra["stakingRewardsAPR"]
                if yrn_apr_val is not None:
                    try:
                        potential_yearn_staking_apr = Decimal(yrn_apr_val) * Decimal("100")
                    except Exception:
                        pass

            potential_one_up_staking_apr = Decimal("0")
            if one_up_staking_available and one_up_data:
                gauge_data = one_up_data.get("gauges", {}).get(one_up_gauge_address_lower)
                if gauge_data:
                    one_up_apr_val = gauge_data.get("reward_apr")
                    if one_up_apr_val is not None:
                        try:
                            potential_one_up_staking_apr = Decimal(one_up_apr_val)
                        except Exception:
                            pass

            staked_status = "none"
            effective_balance_hex = "0x0"
            current_staking_apr_percent = Decimal("0")
            current_staking_apr_source = ""

            if int(yearn_gauge_balance_hex, 16) > 0:
                staked_status = "yearn"
                effective_balance_hex = yearn_gauge_balance_hex
                current_staking_apr_source = "Yearn (Max Boost)"
                current_staking_apr_percent = potential_yearn_staking_apr
                has_yearn_gauge_deposit = True
            elif int(one_up_gauge_balance_hex, 16) > 0:
                staked_status = "1up"
                effective_balance_hex = one_up_gauge_balance_hex
                current_staking_apr_source = "1UP"
                current_staking_apr_percent = potential_one_up_staking_apr
            elif int(vault_balance_hex, 16) > 0:
                staked_status = "none"
                effective_balance_hex = vault_balance_hex
            else:
                continue

            try:
                name = vault.get("name", "Unknown")
                display_name = vault.get("display_name", name)
                token_data = vault.get("token", {})
                token_symbol = token_data.get("display_name") or token_data.get("symbol") or "Asset"
                underlying_token_address = token_data.get("address", "").lower()
                decimals = int(vault.get("decimals", 18))
                price_per_share_str = vault.get("pricePerShare")
                if price_per_share_str is None:
                    continue
                price_per_share = Decimal(price_per_share_str) / (Decimal(10) ** decimals)
                underlying_token_price = Decimal(vault.get("tvl", {}).get("price") or "0")
                effective_balance_int = int(effective_balance_hex, 16)
                vault_usd_value = (Decimal(effective_balance_int) / (Decimal(10) ** decimals)) * price_per_share * underlying_token_price
                if vault_usd_value < Decimal("0.01"):
                    continue
                net_apr = vault.get("apr", {}).get("netAPR")
                vault_apr_percent = Decimal(net_apr or "0") * Decimal("100")

                vaults_requiring_kong.add((chain_id, vault.get("address")))

                if chain_id not in portfolio_by_chain:
                    portfolio_by_chain[chain_id] = {
                        "vaults": [],
                        "total_usd": Decimal("0"),
                        "weighted_apr": Decimal("0"),
                        "weighted_yield7d": Decimal("0"),
                        "weighted_yield30d": Decimal("0"),
                        "total_usd_change7d": Decimal("0"),
                        "total_usd_change30d": Decimal("0"),
                    }

                vault_info = {
                    "display_name": display_name,
                    "token_symbol": token_symbol,
                    "vault_url": f"https://yearn.fi/v3/{chain_id}/{vault.get('address')}",
                    "vault_usd_value": vault_usd_value,
                    "vault_apr_percent": vault_apr_percent,
                    "staked_status": staked_status,
                    "staked_indicator_url": None,
                    "current_staking_apr_percent": current_staking_apr_percent,
                    "current_staking_apr_source": current_staking_apr_source,
                    "yield_7d": Decimal("0"),
                    "yield_30d": Decimal("0"),
                    "usd_change_7d": Decimal("0"),
                    "usd_change_30d": Decimal("0"),
                    "address": vault.get("address"),
                    "address_lower": vault_address_lower,
                    "effective_balance_hex": effective_balance_hex,
                    "decimals": decimals,
                    "underlying_token_price": underlying_token_price,
                }

                if staked_status == "1up" and one_up_gauge_address_lower:
                    try:
                        checksum_1up = Web3.to_checksum_address(one_up_gauge_address_lower)
                        vault_info["staked_indicator_url"] = f"https://1up.tokyo/stake/ethereum/{checksum_1up}"
                    except Exception:
                        vault_info["staked_indicator_url"] = None

                portfolio_by_chain[chain_id]["vaults"].append(vault_info)
                portfolio_by_chain[chain_id]["total_usd"] += vault_usd_value
                portfolio_by_chain[chain_id]["weighted_apr"] += vault_apr_percent * vault_usd_value

                report_vaults_details.append(
                    {
                        "address": vault_address_lower,
                        "underlying_token_address": underlying_token_address,
                        "apr": vault_apr_percent,
                        "chainID": chain_id,
                        "name": name,
                        "symbol": token_symbol,
                    }
                )

            except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
                logger.error("Error processing vault %s: %s", vault_address_lower, exc)
                continue

        kong_results: Dict[Tuple[int, str], list] = {}
        kong_tasks = [self._yearn.get_kong_data(address, cid) for cid, address in vaults_requiring_kong]
        if kong_tasks:
            kong_responses = await asyncio.gather(*kong_tasks)
            for i, (cid, address) in enumerate(vaults_requiring_kong):
                if kong_responses[i]:
                    kong_results[(cid, address.lower())] = kong_responses[i]

        chains: List[ChainReport] = []
        grand_total_usd = Decimal("0")
        grand_total_weighted_apr = Decimal("0")
        grand_total_weighted_yield7d = Decimal("0")
        grand_total_weighted_yield30d = Decimal("0")
        grand_total_usd_change7d = Decimal("0")
        grand_total_usd_change30d = Decimal("0")

        for chain_id, chain_data in portfolio_by_chain.items():
            chain_name = CHAIN_NAMES.get(chain_id, f"Chain {chain_id}")
            vault_entries: List[VaultEntry] = []

            for vault_info in chain_data["vaults"]:
                timeseries = kong_results.get((chain_id, vault_info["address_lower"]))
                if timeseries:
                    current_pps, pps_7d, pps_30d = process_timeseries_data_with_decimal(timeseries)
                    yield_7d = calculate_yield_with_decimal(current_pps, pps_7d)
                    yield_30d = calculate_yield_with_decimal(current_pps, pps_30d)
                    effective_balance_tokens = Decimal(int(vault_info["effective_balance_hex"], 16)) / (
                        Decimal(10) ** vault_info["decimals"]
                    )
                    pps_change_7d = current_pps - pps_7d
                    pps_change_30d = current_pps - pps_30d
                    usd_change_7d = Decimal("0")
                    usd_change_30d = Decimal("0")
                    if vault_info["underlying_token_price"] > 0:
                        usd_change_7d = (effective_balance_tokens * pps_change_7d) * vault_info["underlying_token_price"]
                        usd_change_30d = (effective_balance_tokens * pps_change_30d) * vault_info["underlying_token_price"]
                else:
                    yield_7d = Decimal("0")
                    yield_30d = Decimal("0")
                    usd_change_7d = Decimal("0")
                    usd_change_30d = Decimal("0")

                chain_data["weighted_yield7d"] += yield_7d * vault_info["vault_usd_value"]
                chain_data["weighted_yield30d"] += yield_30d * vault_info["vault_usd_value"]
                chain_data["total_usd_change7d"] += usd_change_7d
                chain_data["total_usd_change30d"] += usd_change_30d

                vault_entries.append(
                    VaultEntry(
                        chain_id=chain_id,
                        display_name=vault_info["display_name"],
                        token_symbol=vault_info["token_symbol"],
                        vault_url=vault_info["vault_url"],
                        vault_usd_value=vault_info["vault_usd_value"],
                        vault_apr_percent=vault_info["vault_apr_percent"],
                        yield_7d=yield_7d,
                        yield_30d=yield_30d,
                        usd_change_7d=usd_change_7d,
                        usd_change_30d=usd_change_30d,
                        staked_status=vault_info["staked_status"],
                        staked_indicator_url=vault_info["staked_indicator_url"],
                        current_staking_apr_percent=vault_info["current_staking_apr_percent"],
                        current_staking_apr_source=vault_info["current_staking_apr_source"],
                    )
                )

            chain_total_usd = chain_data["total_usd"]
            if chain_total_usd > 0:
                avg_apr = chain_data["weighted_apr"] / chain_total_usd
                avg_yield7d = chain_data["weighted_yield7d"] / chain_total_usd
                avg_yield30d = chain_data["weighted_yield30d"] / chain_total_usd
                grand_total_usd += chain_total_usd
                grand_total_weighted_apr += chain_data["weighted_apr"]
                grand_total_weighted_yield7d += chain_data["weighted_yield7d"]
                grand_total_weighted_yield30d += chain_data["weighted_yield30d"]
                grand_total_usd_change7d += chain_data["total_usd_change7d"]
                grand_total_usd_change30d += chain_data["total_usd_change30d"]
            else:
                avg_apr = Decimal("0")
                avg_yield7d = Decimal("0")
                avg_yield30d = Decimal("0")

            chains.append(
                ChainReport(
                    chain_id=chain_id,
                    chain_name=chain_name,
                    vaults=vault_entries,
                    total_usd=chain_total_usd,
                    avg_apr=avg_apr,
                    avg_yield_7d=avg_yield7d,
                    avg_yield_30d=avg_yield30d,
                    total_usd_change_7d=chain_data["total_usd_change7d"],
                    total_usd_change_30d=chain_data["total_usd_change30d"],
                )
            )

        if grand_total_usd > 0:
            overall = OverallSummary(
                total_usd=grand_total_usd,
                avg_apr=grand_total_weighted_apr / grand_total_usd,
                avg_yield_7d=grand_total_weighted_yield7d / grand_total_usd,
                avg_yield_30d=grand_total_weighted_yield30d / grand_total_usd,
                total_usd_change_7d=grand_total_usd_change7d,
                total_usd_change_30d=grand_total_usd_change30d,
            )
        else:
            overall = OverallSummary(
                total_usd=Decimal("0"),
                avg_apr=Decimal("0"),
                avg_yield_7d=Decimal("0"),
                avg_yield_30d=Decimal("0"),
                total_usd_change_7d=Decimal("0"),
                total_usd_change_30d=Decimal("0"),
            )

        suggestions = self._generate_suggestions(report_vaults_details, all_vaults)

        timestamps = self._yearn.cache_timestamps()
        last_update_ts = max(timestamps.values())
        if last_update_ts:
            last_update_dt = datetime.fromtimestamp(last_update_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M %Z")
            cache_note = (
                f"Data cached every {self._yearn.cache_expiry_hours():.0f} hours. "
                f"Last update: {last_update_dt}."
            )
        else:
            cache_note = "Data freshness may vary."

        empty = overall.total_usd <= 0

        return ReportData(
            chains=sorted(chains, key=lambda c: c.chain_id),
            overall=overall,
            suggestions=suggestions,
            cache_note=cache_note,
            has_yearn_gauge_deposit=has_yearn_gauge_deposit,
            empty=empty,
        )

    def _generate_suggestions(self, user_vaults_details: list, all_vaults: list) -> List[SuggestionEntry]:
        if not user_vaults_details or not all_vaults:
            return []

        user_holdings_lookup: Dict[Tuple[int, str], List[Decimal]] = {}
        for detail in user_vaults_details:
            key = (detail["chainID"], detail["underlying_token_address"])
            if not detail["underlying_token_address"]:
                continue
            if key not in user_holdings_lookup:
                user_holdings_lookup[key] = []
            user_holdings_lookup[key].append(detail["apr"])

        if not user_holdings_lookup:
            return []

        suggestions: List[SuggestionEntry] = []
        suggested_set = set()

        for vault in all_vaults:
            try:
                chain_id = vault.get("chainID")
                vault_address = vault.get("address", "").lower()
                underlying_address = vault.get("token", {}).get("address", "").lower()
                if not chain_id or not vault_address or not underlying_address:
                    continue

                key = (chain_id, underlying_address)
                user_aprs = user_holdings_lookup.get(key)
                if not user_aprs:
                    continue

                is_already_held = any(
                    uv["address"] == vault_address and uv["chainID"] == chain_id for uv in user_vaults_details
                )
                if is_already_held:
                    continue

                tvl_usd = Decimal(vault.get("tvl", {}).get("tvl") or "0")
                if tvl_usd < self._config.min_suggestion_tvl_usd:
                    continue

                net_apr = vault.get("apr", {}).get("netAPR")
                base_apr = Decimal(net_apr or "0") * Decimal("100")
                apr_difference = base_apr - min(user_aprs)
                if apr_difference <= self._config.suggestion_apr_threshold:
                    continue

                suggestion_key = (chain_id, vault_address)
                if suggestion_key in suggested_set:
                    continue

                suggestions.append(
                    SuggestionEntry(
                        chain_id=chain_id,
                        chain_name=CHAIN_NAMES.get(chain_id, f"Chain {chain_id}"),
                        display_name=vault.get("display_name") or vault.get("name", "Vault"),
                        token_symbol=vault.get("token", {}).get("display_name") or vault.get("token", {}).get("symbol") or "Asset",
                        vault_url=f"https://yearn.fi/v3/{chain_id}/{vault.get('address')}",
                        base_apr=base_apr,
                        apr_difference=apr_difference,
                        tvl=tvl_usd,
                    )
                )
                suggested_set.add(suggestion_key)
            except Exception as exc:
                logger.error("Suggestion processing failed: %s", exc)
                continue

        suggestions.sort(key=lambda s: (s.chain_id, -s.base_apr))
        return suggestions


def process_timeseries_data_with_decimal(timeseries: list) -> Tuple[Decimal, Decimal, Decimal]:
    if not timeseries:
        return Decimal("0"), Decimal("0"), Decimal("0")
    try:
        sorted_timeseries = sorted(timeseries, key=lambda x: int(x["time"]))
        now = datetime.utcnow()
        ts_7d = int((now - timedelta(days=7)).timestamp())
        ts_30d = int((now - timedelta(days=30)).timestamp())

        def find_closest(target_ts: int) -> dict:
            entries = [entry for entry in sorted_timeseries if int(entry["time"]) <= target_ts]
            if not entries:
                return sorted_timeseries[0]
            return entries[-1]

        current = sorted_timeseries[-1]
        entry_7d = find_closest(ts_7d)
        entry_30d = find_closest(ts_30d)

        return Decimal(current["value"]), Decimal(entry_7d["value"]), Decimal(entry_30d["value"])
    except (KeyError, ValueError, TypeError, InvalidOperation) as exc:
        logger.error("Timeseries processing error: %s", exc)
        return Decimal("0"), Decimal("0"), Decimal("0")


def calculate_yield_with_decimal(current: Decimal, historical: Decimal) -> Decimal:
    if historical == Decimal("0"):
        return Decimal("0")
    try:
        return ((Decimal(current) / Decimal(historical)) - Decimal("1")) * Decimal("100")
    except (InvalidOperation, TypeError) as exc:
        logger.error("Yield calculation error: %s", exc)
        return Decimal("0")


def format_tvl(tvl: Decimal) -> str:
    if tvl >= 1_000_000:
        return f"${tvl / 1_000_000:.2f}M"
    if tvl >= 1_000:
        return f"${tvl / 1_000:.1f}K"
    return f"${tvl:.0f}"
