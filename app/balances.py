import asyncio
import logging
from typing import Dict, Optional

from web3 import Web3

from .abis import ERC20_ABI_SIMPLIFIED
from .chains import CHAIN_TO_ALCHEMY_PREFIX, CHAIN_TO_RPC_URL

logger = logging.getLogger(__name__)

async def fetch_alchemy_balances(session, api_key: str, eoa: str, chain_id: int) -> Dict[str, str]:
    prefix = CHAIN_TO_ALCHEMY_PREFIX.get(chain_id)
    if not prefix:
        if chain_id in CHAIN_TO_RPC_URL:
            return {}
        logger.warning("No Alchemy prefix for chain %s", chain_id)
        return {}

    if not api_key:
        logger.warning("Alchemy API key missing; skipping token balance fetch")
        return {}
    url = f"https://{prefix}.g.alchemy.com/v2/{api_key}"
    payload = {
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenBalances",
        "params": [eoa, "erc20"],
        "id": 1,
    }

    try:
        async with session.post(url, json=payload, timeout=10) as response:
            if response.status != 200:
                logger.error("Alchemy error %s for %s on chain %s", response.status, eoa, chain_id)
                return {}
            data = await response.json()
            if "result" in data and "tokenBalances" in data["result"]:
                balances = {
                    item["contractAddress"].lower(): item["tokenBalance"]
                    for item in data["result"]["tokenBalances"]
                    if item.get("tokenBalance") and item["tokenBalance"] != "0x0" and item.get("contractAddress")
                }
                return balances
            if "error" in data:
                logger.error("Alchemy API error for %s on chain %s: %s", eoa, chain_id, data["error"])
            return {}
    except Exception as exc:
        logger.error("Alchemy request failed for %s on chain %s: %s", eoa, chain_id, exc)
    return {}

async def fetch_balances_for_eoa_on_chain(
    eoa: str,
    chain_id: int,
    vaults_data: list,
    w3_instance: Optional[Web3],
    session,
    api_key: str,
    direct_call_concurrency: int = 20,
) -> Dict[str, str]:
    balances: Dict[str, str] = {}
    loop = asyncio.get_running_loop()

    try:
        alchemy_balances = await fetch_alchemy_balances(session, api_key, eoa, chain_id)
        if alchemy_balances:
            balances.update({k.lower(): v for k, v in alchemy_balances.items()})
    except Exception as exc:
        logger.error("Alchemy balance fetch failed for %s on chain %s: %s", eoa, chain_id, exc)

    if chain_id == 1:
        return balances

    if not w3_instance:
        logger.warning("No Web3 instance for chain %s, skipping direct balanceOf", chain_id)
        return balances

    semaphore = asyncio.Semaphore(direct_call_concurrency)

    async def direct_balance_call(vault_data: dict) -> Optional[tuple]:
        address = vault_data.get("address")
        if not address or not Web3.is_address(address):
            return None
        checksum = Web3.to_checksum_address(address)
        addr_lower = checksum.lower()
        if addr_lower in balances and balances[addr_lower] != "0x0":
            return None
        try:
            async with semaphore:
                def _call_balance() -> int:
                    contract = w3_instance.eth.contract(address=checksum, abi=ERC20_ABI_SIMPLIFIED)
                    return contract.functions.balanceOf(eoa).call()

                value = await loop.run_in_executor(None, _call_balance)
            if value > 0:
                return (addr_lower, hex(value))
        except Exception as exc:
            logger.error("Direct balanceOf failed for %s on chain %s: %s", checksum, chain_id, exc)
        return None

    vaults_on_chain = [v for v in vaults_data if v.get("chainID") == chain_id]
    tasks = [direct_balance_call(v) for v in vaults_on_chain]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, tuple):
            addr, value = result
            balances[addr] = value

    return balances
