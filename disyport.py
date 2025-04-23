import discord
import asyncio
import aiohttp
import requests
import json 
import logging
import re
from decimal import Decimal, getcontext, InvalidOperation
from datetime import datetime, timedelta, time, timezone
from discord.ext import commands, tasks
from discord import app_commands, Embed, Interaction 
from web3 import Web3
from ens import ENS
from pathlib import Path
import sys

sys.stdout = sys.stderr

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

logging.getLogger("discord").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Config
BOT_TOKEN = 'BOT_TOKEN_HERE'
API_KEY = "RPC_API_KEY_HERE"

PUBLIC_CHANNEL_ID = SPECIFY_PUBLIC_YPORT_CHANNEL
LOG_CHANNEL_ID = SPECIFY_CHANNEL_FOR_LOGS
ADMIN_USER_ID = ADMIN_USER_ID
RATE_LIMIT_SECONDS = 10
CACHE_EXPIRY_SECONDS = 3 * 60 * 60 
CACHE_FILE_YDAEMON = Path("cache_ydaemon.json") 
CACHE_FILE_KONG = Path("cache_kong.json")
CACHE_FILE_1UP = Path("cache_1up.json")
CACHE_FILE_1UP_MAP = Path("cache_1up_map.json")
ONE_UP_API_URL = "https://1up.s3.pl-waw.scw.cloud/aprs.json"
MIN_SUGGESTION_TVL_USD = Decimal('50000') 
SUGGESTION_APR_THRESHOLD = Decimal('5.0') 

getcontext().prec = 28

CHAIN_ID_TO_NAME = {
    1: "Ethereum",
    10: "Optimism",
    137: "Polygon",
    42161: "Arbitrum",
    8453: "Base"
}

CHAIN_NAME_TO_ID = {v.lower(): k for k, v in CHAIN_ID_TO_NAME.items()}

CHAIN_TO_ALCHEMY_PREFIX = {
    1: 'eth-mainnet',      
    10: 'opt-mainnet',     
    137: 'polygon-mainnet',
    42161: 'arb-mainnet',  
    8453: 'base-mainnet',   
}

SINGLE_ASSET_TOKENS = {
    'ethereum': {
        'weth': '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        'dai': '0x6B175474E89094C44Da98b954EedeAC495271d0F',
        'usdc': '0xA0b86991c6218b36c1d19D4a2e9eb0cE3606eb48',
        'usdt': '0xdAC17F958D2ee523a2206206994597C13D831ec7',
        'wbtc': '0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599',
    },
    'arbitrum': {
        'weth': '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',
        'dai': '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1',
        'usdc': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831',
        'usdc_e': '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8',
        'usdt': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9',
        'wbtc': '0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f',
    },
    'polygon': {
        'weth': '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619',
        'dai': '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063',
        'usdc': '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359',
        'usdc_e': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',
        'usdt': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F',
        'wbtc': '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6',
    },
    'base': {
        'weth': '0x4200000000000000000000000000000000000006',
        'dai': '0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb',
        'usdc': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
    },
    'optimism': {
        'weth': '0x4200000000000000000000000000000000000006',
        'dai': '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1',
        'usdc': '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85',
        'usdc_e': '0x7F5c764cBc14f9669B88837ca1490cCa17c31607',
        'usdt': '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58',
        'wbtc': '0x68f180fcce6836688e9084f035309e29bf0a2095',
    }
}

YEARN_GAUGE_ABI = json.loads('[{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"boostedBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"earned","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"rewardRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"asset","outputs":[{"internalType":"contract IERC20","name":"","type":"address"}],"stateMutability":"view","type":"function"}]') 
ONE_UP_GAUGE_ABI = json.loads('[{"stateMutability":"view","type":"function","name":"balanceOf","inputs":[{"name":"_account","type":"address"}],"outputs":[{"name":"","type":"uint256"}]},{"stateMutability":"view","type":"function","name":"asset","inputs":[],"outputs":[{"name":"","type":"address"}]}]') 
ERC20_ABI_SIMPLIFIED = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}]')

user_data = {}   
report_locks = {} 
last_report_times = {} 
daily_yport_usage_count = 0 
last_scheduled_report_message_id = None 

api_cache = {
    'ydaemon': {'data': None, 'timestamp': 0},
    'kong': {'data': {}, 'timestamp': 0}, 
    '1up': {'data': None, 'timestamp': 0}, 
    '1up_gauge_map': {'data': {}, 'timestamp': 0} 
}

web3_instances = {}
ns = None 


# Functions start here!

def get_web3_instance(chain_id):
    """Gets or creates a Web3 instance for a given chain ID."""
    global web3_instances
    if chain_id not in web3_instances:
        prefix = CHAIN_TO_ALCHEMY_PREFIX.get(chain_id)
        if prefix:
            try:
                w3 = Web3(Web3.HTTPProvider(f"https://{prefix}.g.alchemy.com/v2/{API_KEY}"))
                if w3.is_connected():
                    web3_instances[chain_id] = w3
                    logger.info(f"Initialized Web3 instance for chain {chain_id}")
                else:
                    logger.error(f"Failed to connect Web3 instance for chain {chain_id}")
                    return None
            except Exception as e:
                logger.error(f"Error initializing Web3 for chain {chain_id}: {e}")
                return None
        else:
            logger.warning(f"No Alchemy prefix for chain {chain_id}, cannot create Web3 instance.")
            return None
    return web3_instances.get(chain_id)

# Not sure if this works
def initialize_ens():
    global ns
    w3_eth = get_web3_instance(1) 
    if w3_eth:
        try:
            ns = ENS.from_web3(w3_eth)
            logger.info("ENS resolver initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize ENS: {e}")
            ns = None 
    else:
        logger.error("Failed to initialize Ethereum Web3 instance, ENS resolution will not work.")
        ns = None

async def update_ydaemon_cache():
    """Fetches latest yDaemon data and updates the cache."""
    global api_cache
    logger.info("Attempting to update yDaemon cache...")
    try:
        async with aiohttp.ClientSession() as session:

            async with session.get("https://ydaemon.yearn.fi/vaults/detected?limit=2000", timeout=30) as response:
                if response.status == 200:
                    data = await response.json()
                    api_cache['ydaemon']['data'] = data
                    api_cache['ydaemon']['timestamp'] = datetime.utcnow().timestamp()
                    logger.info(f"yDaemon cache updated successfully. {len(data)} vaults.")

                    return True
                else:
                    logger.error(f"Failed to fetch yDaemon data: Status {response.status}")
                    return False
    except asyncio.TimeoutError:
        logger.error("Timeout error fetching yDaemon data.")
        return False
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching yDaemon data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating yDaemon cache: {e}", exc_info=True)
        return False

async def fetch_historical_pricepershare_kong_live(vault_address, chain_id, limit=1000):
    url = "https://kong.yearn.farm/api/gql"
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
        "limit": limit
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"query": query, "variables": variables}, timeout=20) as response:
                if response.status == 200:
                    data = await response.json()
                    if "data" in data and "timeseries" in data["data"]:
                        if isinstance(data["data"]["timeseries"], list) and \
                           all(isinstance(item, dict) and 'time' in item and 'value' in item for item in data["data"]["timeseries"]):
                            return data["data"]["timeseries"]
                        else:
                            logger.warning(f"Unexpected timeseries format from Kong for {vault_address}: {data['data']['timeseries']}")
                            return None
                    else:
                        logger.warning(f"Unexpected response structure from Kong for {vault_address}: {data}")
                        return None
                else:
                    logger.error(f"Error fetching data from Kong for {vault_address}: Status {response.status}, Response: {await response.text()}")
                    return None
    except asyncio.TimeoutError:
        logger.error(f"Timeout error fetching Kong data for {vault_address} on chain {chain_id}.")
        return None
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching data from Kong for {vault_address}: {e}")
        return None
    except Exception as e:
        logger.error(f"Exception while fetching data from Kong for {vault_address}: {str(e)}", exc_info=True)
        return None

async def update_kong_cache(vaults_to_update):
    global api_cache
    if not vaults_to_update:
        logger.info("No vaults provided for Kong cache update.")
        return

    logger.info(f"Attempting to update Kong cache for {len(vaults_to_update)} vaults...")
    updated_count = 0
    tasks = []
    CONCURRENCY_LIMIT = 50 
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    new_kong_data = {} 

    async def fetch_and_store_kong(vault_address, chain_id):
        nonlocal updated_count
        cache_key = (chain_id, vault_address.lower())
        async with semaphore:
            logger.debug(f"Fetching Kong data for {vault_address} on chain {chain_id}")
            data = await fetch_historical_pricepershare_kong_live(vault_address, chain_id)
            if data:
                new_kong_data[cache_key] = data
                updated_count += 1

    for chain_id, address in vaults_to_update:
         tasks.append(fetch_and_store_kong(address, chain_id))

    if tasks:
        logger.info(f"Gathering results for {len(tasks)} Kong fetch tasks with concurrency limit {CONCURRENCY_LIMIT}...")
        await asyncio.gather(*tasks)
        api_cache['kong']['data'] = new_kong_data
        api_cache['kong']['timestamp'] = datetime.utcnow().timestamp()
        logger.info(f"Kong cache updated. Fetched/updated data for {updated_count} vaults.")

    else:
        logger.info("No Kong fetch tasks were created.")

async def update_1up_cache():
    global api_cache
    logger.info("Attempting to update 1UP APR cache...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(ONE_UP_API_URL, timeout=15) as response:
                logger.debug(f"1UP API response status: {response.status}")
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/json' not in content_type:
                        response_text = await response.text()
                        logger.error(f"Failed to fetch 1UP APR data: Unexpected content type '{content_type}'. Response text: {response_text[:500]}...")
                        return False
                    try:
                        data = await response.json()
                        if isinstance(data, dict) and 'gauges' in data and isinstance(data['gauges'], dict):
                            processed_gauges = {k.lower(): v for k, v in data['gauges'].items()}
                            processed_data = data.copy()
                            processed_data['gauges'] = processed_gauges
                            api_cache['1up']['data'] = processed_data
                            api_cache['1up']['timestamp'] = datetime.utcnow().timestamp()
                            logger.info(f"1UP APR cache updated successfully. Found {len(processed_data.get('gauges', {}))} gauges.")
                            return True
                        else:
                            logger.error(f"Unexpected 1UP API JSON structure. Data received: {str(data)[:500]}...")
                            return False
                    except aiohttp.ContentTypeError as json_err:
                        response_text = await response.text()
                        logger.error(f"Failed to decode 1UP APR JSON: {json_err}. Response text: {response_text[:500]}...", exc_info=True)
                        return False
                    except Exception as parse_err:
                         logger.error(f"Error processing 1UP JSON data: {parse_err}", exc_info=True)
                         return False
                else:
                    response_text = await response.text()
                    logger.error(f"Failed to fetch 1UP APR data: Status {response.status}. Response text: {response_text[:500]}...")
                    return False
    except asyncio.TimeoutError:
         logger.error("Timeout error fetching 1UP APR data.")
         return False
    except aiohttp.ClientError as net_err:
        logger.error(f"Network error fetching 1UP APR data: {net_err}", exc_info=True)
        return False
    except Exception as e:
        logger.error(f"Unexpected error during 1UP APR cache update: {e}", exc_info=True)
        return False

async def update_1up_gauge_map_cache():
    global api_cache
    one_up_data = api_cache['1up'].get('data')
    if not one_up_data or 'gauges' not in one_up_data:
        logger.warning("Cannot update 1UP gauge map: 1UP APR data not available.")
        return False

    logger.info("Attempting to update 1UP Gauge -> Vault mapping cache...")
    gauge_map = {}
    gauges_to_query = list(one_up_data['gauges'].keys())
    updated_count = 0
    chain_id_1up = 1 
    w3 = get_web3_instance(chain_id_1up)
    if not w3:
        logger.error(f"Cannot update 1UP gauge map: Web3 instance for chain {chain_id_1up} not available.")
        return False

    tasks = []
    semaphore = asyncio.Semaphore(10) 

    async def fetch_asset(gauge_address_str):
        nonlocal gauge_map, updated_count
        try:
            if not Web3.is_address(gauge_address_str):
                logger.warning(f"Skipping invalid address in 1UP data: {gauge_address_str}")
                return

            gauge_address = Web3.to_checksum_address(gauge_address_str)
            gauge_contract = w3.eth.contract(address=gauge_address, abi=ONE_UP_GAUGE_ABI)

            async with semaphore:
                loop = asyncio.get_running_loop()
                underlying_asset_address = await loop.run_in_executor(
                    None, 
                    gauge_contract.functions.asset().call
                )

            if underlying_asset_address and Web3.is_address(underlying_asset_address):
                gauge_map[gauge_address.lower()] = Web3.to_checksum_address(underlying_asset_address).lower()
                return True 
            else:
                logger.warning(f"Could not retrieve valid asset address for 1UP gauge: {gauge_address}")
                return False
        except Exception as e:
            logger.error(f"Error calling 'asset()' on 1UP gauge {gauge_address_str}: {e}")
            return False

    tasks = [fetch_asset(addr) for addr in gauges_to_query]
    results = await asyncio.gather(*tasks)
    updated_count = sum(1 for r in results if r is True)

    if updated_count > 0:
        api_cache['1up_gauge_map']['data'] = gauge_map
        api_cache['1up_gauge_map']['timestamp'] = datetime.utcnow().timestamp()
        logger.info(f"1UP Gauge -> Vault mapping cache updated for {updated_count} gauges.")
        return True
    else:
        logger.info("No new 1UP gauge mappings were successfully retrieved.")
        return False

def get_ydaemon_data():
    now = datetime.utcnow().timestamp()
    if api_cache['ydaemon']['data'] and (now - api_cache['ydaemon']['timestamp'] < CACHE_EXPIRY_SECONDS):
        return api_cache['ydaemon']['data']
    else:
        logger.warning("yDaemon cache miss or expired. Returning potentially stale data.")
        return api_cache['ydaemon']['data'] 

async def get_kong_data(vault_address, chain_id):
    now = datetime.utcnow().timestamp()
    cache_key = (chain_id, vault_address.lower())
    kong_cache = api_cache['kong']

    if kong_cache.get('data') and cache_key in kong_cache['data'] and (now - kong_cache.get('timestamp', 0) < CACHE_EXPIRY_SECONDS):
         return kong_cache['data'][cache_key]
    else:
        logger.info(f"Kong cache miss or expired for {vault_address} on chain {chain_id}. Fetching live.")
        live_data = await fetch_historical_pricepershare_kong_live(vault_address, chain_id)
        return live_data 

def get_1up_data():
    now = datetime.utcnow().timestamp()
    cache_entry = api_cache['1up']
    if cache_entry.get('data') and (now - cache_entry.get('timestamp', 0) < CACHE_EXPIRY_SECONDS):
        return cache_entry['data']
    else:
        logger.warning("1UP APR cache miss or expired. Returning potentially stale data.")
        return cache_entry.get('data')

def get_1up_gauge_map():
    now = datetime.utcnow().timestamp()
    cache_entry = api_cache['1up_gauge_map']
    if cache_entry.get('data') and (now - cache_entry.get('timestamp', 0) < CACHE_EXPIRY_SECONDS):
        return cache_entry['data']
    else:
        logger.warning("1UP gauge map cache miss or expired. Returning potentially stale data.")
        return cache_entry.get('data')

def get_token_balances(eoa, chain_id):
    prefix = CHAIN_TO_ALCHEMY_PREFIX.get(chain_id)
    if not prefix:
        logger.warning(f"No Alchemy prefix configured for chain ID: {chain_id}")
        return {}

    url = f"https://{prefix}.g.alchemy.com/v2/{API_KEY}"
    payload = {
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenBalances",
        "params": [eoa, "erc20"],
        "id": 1
    }

    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()

        if 'result' in data and 'tokenBalances' in data['result']:
            return {
                item['contractAddress'].lower(): item['tokenBalance']
                for item in data['result']['tokenBalances']
                if item.get('tokenBalance') and item['tokenBalance'] != '0x0' and item.get('contractAddress')
            }
        elif 'error' in data:
             logger.error(f"Alchemy API error for {eoa} on chain {chain_id}: {data['error']}")
             return {}
        else:
            logger.warning(f"Unexpected Alchemy response format for {eoa} on chain {chain_id}: {data}")
            return {}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching token balances via Alchemy for {eoa} on chain {chain_id}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error in get_token_balances for {eoa} on chain {chain_id}: {e}", exc_info=True)
        return {}

def process_timeseries_data_with_decimal(timeseries):
    if not timeseries or not isinstance(timeseries, list) or len(timeseries) == 0:
        return Decimal('0'), Decimal('0'), Decimal('0')
    try:
        sorted_timeseries = sorted(timeseries, key=lambda x: int(x['time']))
        now = datetime.utcnow()
        timestamp_7d_ago = int((now - timedelta(days=7)).timestamp())
        timestamp_30d_ago = int((now - timedelta(days=30)).timestamp())

        def find_closest_entry(target_timestamp):
            relevant_entries = [entry for entry in sorted_timeseries if int(entry['time']) <= target_timestamp]
            if not relevant_entries: return sorted_timeseries[0] 
            return relevant_entries[-1] 

        current_pps_entry = sorted_timeseries[-1]
        pps_7d_ago_entry = find_closest_entry(timestamp_7d_ago)
        pps_30d_ago_entry = find_closest_entry(timestamp_30d_ago)

        current_pps = Decimal(current_pps_entry["value"])
        pps_7d_ago = Decimal(pps_7d_ago_entry["value"])
        pps_30d_ago = Decimal(pps_30d_ago_entry["value"])

        return current_pps, pps_7d_ago, pps_30d_ago
    except (KeyError, ValueError, TypeError, InvalidOperation) as e:
        logger.error(f"Error processing timeseries data: {e} - Data sample: {timeseries[:5]}")
        return Decimal('0'), Decimal('0'), Decimal('0')

def calculate_yield_with_decimal(current_pps, historical_pps):
    if historical_pps == Decimal('0'): return Decimal('0')
    try:
        current_pps_dec = Decimal(current_pps)
        historical_pps_dec = Decimal(historical_pps)
        return ((current_pps_dec / historical_pps_dec) - Decimal('1')) * Decimal('100')
    except (InvalidOperation, TypeError) as e:
        logger.error(f"Error calculating yield: current={current_pps}, historical={historical_pps}, error={e}")
        return Decimal('0')

def format_tvl(tvl_decimal):
    if not isinstance(tvl_decimal, Decimal):
        try:
            tvl_decimal = Decimal(tvl_decimal)
        except (InvalidOperation, TypeError):
            return "$0.0K" 

    if tvl_decimal >= 1_000_000:
        return f"${tvl_decimal / 1_000_000:.1f}M"
    elif tvl_decimal >= 1_000:
        return f"${tvl_decimal / 1_000:.1f}K"
    else:
        return f"${tvl_decimal:.0f}"

def escape_markdown(text):
    escape_chars = r'([_*~`|\\])' 
    return re.sub(escape_chars, r'\\\1', str(text))

def truncate_message(content, limit=2000):
    if len(content) <= limit:
        return content
    else:
        return content[:limit - 25] + "\n\n[...] *(Report truncated)*"

async def generate_user_report(user_id: str, interaction: Interaction):
    global daily_yport_usage_count
    user_specific_data = user_data.get(user_id)

    if not user_specific_data or 'eoas' not in user_specific_data or not user_specific_data['eoas']:
        await interaction.followup.send("⚠️ No addresses found for you. Please send your wallet address(es) or ENS names to me in a Direct Message (DM) first.", ephemeral=True)
        return

    eoas = user_specific_data['eoas']
    logger.info(f"Generating report for user {user_id} (Discord ID) with EOAs: {eoas}")

    await interaction.followup.send("🔄 Generating your Yearn portfolio report...\n\nThis might take a minute...", ephemeral=True)

    generation_start_time = datetime.now()

    all_vaults_data = get_ydaemon_data()
    one_up_apr_data = get_1up_data()
    one_up_gauge_map = get_1up_gauge_map()

    if not all_vaults_data:
        logger.error(f"yDaemon data unavailable for report generation for user {user_id}.")
        await interaction.followup.send("🚨 Unable to retrieve core vault data at the moment. Cache might be updating. Please try again shortly.", ephemeral=True)
        return

    one_up_vault_to_gauge_map = {v: k for k, v in one_up_gauge_map.items()} if one_up_gauge_map else {}

    all_token_balances = {} 
    logger.info(f"Fetching balances for {len(eoas)} EOAs across {len(CHAIN_TO_ALCHEMY_PREFIX)} chains...")
    balance_fetch_start = datetime.now()
    fetch_errors = False
    for eoa in eoas:
        for chain_id in CHAIN_TO_ALCHEMY_PREFIX.keys():
            balances = get_token_balances(eoa, chain_id)

            if balances is None: 
                fetch_errors = True
                logger.warning(f"Balance fetch potentially failed for {eoa} on chain {chain_id}")

            if balances: 
                if chain_id not in all_token_balances: all_token_balances[chain_id] = {}
                if eoa not in all_token_balances[chain_id]: all_token_balances[chain_id][eoa] = {}

                all_token_balances[chain_id][eoa].update({k.lower(): v for k, v in balances.items()})

    logger.info(f"Balance fetching took: {datetime.now() - balance_fetch_start}")
    if fetch_errors:
         await interaction.followup.send("⚠️ Had trouble fetching some token balances. The report might be incomplete.", ephemeral=True)

    portfolio_by_chain = {} 
    report_vaults_details = [] 
    staking_opportunities = [] 
    vaults_requiring_kong = set() 

    logger.info(f"Processing {len(all_vaults_data)} vaults from yDaemon data...")
    processing_start = datetime.now()

    for vault_data in all_vaults_data:
        try:
            vault_address_lower = vault_data.get('address', '').lower()
            chain_id = vault_data.get('chainID')

            if not vault_address_lower or not chain_id or chain_id not in CHAIN_TO_ALCHEMY_PREFIX:
                continue 

            yearn_gauge_address_lower = None
            yearn_staking_available = vault_data.get('staking', {}).get('available', False)
            if yearn_staking_available and vault_data.get('staking', {}).get('address'):
                yg_addr = vault_data['staking']['address']
                if Web3.is_address(yg_addr):
                    yearn_gauge_address_lower = Web3.to_checksum_address(yg_addr).lower()

            one_up_gauge_address_lower = one_up_vault_to_gauge_map.get(vault_address_lower)
            one_up_staking_available = one_up_gauge_address_lower is not None

            vault_balance_hex = "0x0"
            yearn_gauge_balance_hex = "0x0"
            one_up_gauge_balance_hex = "0x0"
            total_balance_for_vault = Decimal('0') 

            if chain_id in all_token_balances:
                for eoa in eoas:
                    eoa_balances = all_token_balances[chain_id].get(eoa, {})
                    bal = eoa_balances.get(vault_address_lower)
                    
                    if bal:
                        bal_int = int(bal, 16)
                        vault_balance_hex = hex(int(vault_balance_hex, 16) + bal_int)
                        total_balance_for_vault += Decimal(bal_int)

                    if yearn_gauge_address_lower:
                        bal = eoa_balances.get(yearn_gauge_address_lower)
                        if bal:
                            bal_int = int(bal, 16)
                            yearn_gauge_balance_hex = hex(int(yearn_gauge_balance_hex, 16) + bal_int)
                            total_balance_for_vault += Decimal(bal_int)

                    if one_up_gauge_address_lower:
                        bal = eoa_balances.get(one_up_gauge_address_lower)
                        if bal:
                            bal_int = int(bal, 16)
                            one_up_gauge_balance_hex = hex(int(one_up_gauge_balance_hex, 16) + bal_int)
                            total_balance_for_vault += Decimal(bal_int)

            staked_status = "none"
            effective_balance_hex = "0x0" 
            current_staking_apr_percent = Decimal('0')
            current_staking_apr_source = ""

            potential_yearn_staking_apr = Decimal('0')
            potential_1up_staking_apr = Decimal('0')

            if yearn_staking_available:
                rewards_list = vault_data.get('staking', {}).get('rewards', [])
                apr_extra = vault_data.get('apr', {}).get('extra', {})
                yrn_apr_val = None

                if rewards_list and rewards_list[0].get('apr') is not None:
                    yrn_apr_val = rewards_list[0]['apr']

                elif apr_extra and apr_extra.get('stakingRewardsAPR') is not None:
                    yrn_apr_val = apr_extra['stakingRewardsAPR']

                if yrn_apr_val is not None:
                    try: potential_yearn_staking_apr = Decimal(yrn_apr_val) * Decimal('100')
                    except (InvalidOperation, TypeError): pass

            if one_up_staking_available and one_up_apr_data and one_up_gauge_address_lower:
                gauge_data = one_up_apr_data.get('gauges', {}).get(one_up_gauge_address_lower)
                if gauge_data:
                    one_up_apr_val = gauge_data.get('reward_apr')
                    if one_up_apr_val is not None:
                        try: potential_1up_staking_apr = Decimal(one_up_apr_val)
                        except (InvalidOperation, TypeError): pass

            if int(yearn_gauge_balance_hex, 16) > 0:
                staked_status = "yearn"
                effective_balance_hex = yearn_gauge_balance_hex
                current_staking_apr_source = "Yearn (Max Boost)" 
                current_staking_apr_percent = potential_yearn_staking_apr
            elif int(one_up_gauge_balance_hex, 16) > 0:
                staked_status = "1up"
                effective_balance_hex = one_up_gauge_balance_hex
                current_staking_apr_source = "1UP"
                current_staking_apr_percent = potential_1up_staking_apr
            elif int(vault_balance_hex, 16) > 0:
                staked_status = "none"
                effective_balance_hex = vault_balance_hex 
            else:
                continue 

            name = vault_data.get('name', 'Unknown')
            symbol = vault_data.get('symbol', 'yVault')
            decimals = int(vault_data.get('decimals', 18))
            display_name = vault_data.get('display_name', name)
            token_data = vault_data.get('token', {})
            token_symbol = escape_markdown(token_data.get('display_name') or token_data.get('symbol') or "Asset")
            underlying_token_address = token_data.get('address', '').lower()

            price_per_share_str = vault_data.get('pricePerShare')
            if price_per_share_str is None: continue 

            price_per_share = Decimal(price_per_share_str) / (Decimal(10) ** decimals)

            underlying_token_price = Decimal(vault_data.get('tvl', {}).get('price') or '0')

            total_balance_in_tokens = (total_balance_for_vault / (Decimal(10) ** decimals))
            total_balance_in_underlying = total_balance_in_tokens * price_per_share
            vault_usd_value = total_balance_in_underlying * underlying_token_price

            if vault_usd_value < Decimal('0.01'): continue

            net_apr = vault_data.get('apr', {}).get('netAPR')
            vault_apr_percent = Decimal(net_apr or '0') * Decimal('100')

            vaults_requiring_kong.add((chain_id, vault_data['address']))

            staked_indicator = ""
            if staked_status == "yearn": staked_indicator = " *(Staked: Yearn)*"
            elif staked_status == "1up": staked_indicator = " *(Staked: 1UP)*"

            vault_info = {
                'name': name, 'symbol': symbol, 'display_name': escape_markdown(display_name),
                'token_symbol': token_symbol, 'vault_usd_value': vault_usd_value,
                'vault_apr_percent': vault_apr_percent, 'staked_status': staked_status,
                'staked_indicator': staked_indicator,
                'current_staking_apr_percent': current_staking_apr_percent,
                'current_staking_apr_source': current_staking_apr_source,
                'vault_url': f"https://yearn.fi/v3/{chain_id}/{vault_data['address']}",
                'chain_id': chain_id, 'address': vault_data['address'], 
                'address_lower': vault_address_lower, 
                'underlying_token_address': underlying_token_address,

                'effective_balance_in_underlying': (Decimal(int(effective_balance_hex, 16)) / (Decimal(10)**decimals)) * price_per_share,
                'yield_7d': Decimal('0'), 'yield_30d': Decimal('0'),
                'usd_change_7d': Decimal('0'), 'usd_change_30d': Decimal('0'),

                'potential_yearn_staking_apr': potential_yearn_staking_apr if staked_status == "none" else Decimal('0'),
                'potential_1up_staking_apr': potential_1up_staking_apr if staked_status == "none" else Decimal('0'),
                '1up_gauge_address': one_up_gauge_address_lower, 
                'effective_balance_hex': effective_balance_hex, 
                'underlying_token_price': underlying_token_price, 
                'decimals': decimals 
            }

            if chain_id not in portfolio_by_chain:
                portfolio_by_chain[chain_id] = {
                    'vaults': [], 'total_usd': Decimal(0), 'weighted_apr': Decimal(0),
                    'weighted_yield7d': Decimal(0), 'weighted_yield30d': Decimal(0),
                    'total_usd_change7d': Decimal(0), 'total_usd_change30d': Decimal(0)
                 }
            portfolio_by_chain[chain_id]['vaults'].append(vault_info)
            portfolio_by_chain[chain_id]['total_usd'] += vault_usd_value

            portfolio_by_chain[chain_id]['weighted_apr'] += vault_apr_percent * vault_usd_value

            report_vaults_details.append({
                'address': vault_address_lower,
                'underlying_token_address': underlying_token_address,
                'apr': vault_apr_percent, 
                'chainID': chain_id,
                'name': name, 
                'symbol': symbol,
            })

            if vault_info['staked_status'] == "none" and \
               (vault_info['potential_yearn_staking_apr'] > 0 or vault_info['potential_1up_staking_apr'] > 0):
                staking_opportunities.append({
                    'chain_id': chain_id,
                    'display_name': vault_info['display_name'], 
                    'token_symbol': vault_info['token_symbol'], 
                    'yearn_apr': vault_info['potential_yearn_staking_apr'],
                    '1up_apr': vault_info['potential_1up_staking_apr'],
                    'yearn_url': vault_info['vault_url'], 
                    '1up_gauge_address': vault_info['1up_gauge_address'] 
                })

        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            logger.error(f"Error processing vault data for {vault_data.get('address', 'N/A')} on chain {vault_data.get('chainID', 'N/A')}: {e}", exc_info=True)
            continue 

    logger.info(f"Initial vault processing took: {datetime.now() - processing_start}")
    logger.info(f"Found {sum(len(cd['vaults']) for cd in portfolio_by_chain.values())} total vault positions held by user.")
    logger.info(f"Found {len(staking_opportunities)} potential staking opportunities for current holdings.")
    logger.info(f"Need to fetch/check Kong data for {len(vaults_requiring_kong)} unique vaults.")

    kong_fetch_start = datetime.now()
    kong_results = {} 
    kong_tasks = []

    kong_semaphore = asyncio.Semaphore(10)

    async def fetch_kong_wrapper(chain_id, vault_address):
        async with kong_semaphore:
            data = await get_kong_data(vault_address, chain_id) 
            if data:
                kong_results[(chain_id, vault_address.lower())] = data

    for cid, address in vaults_requiring_kong:
        kong_tasks.append(fetch_kong_wrapper(cid, address))

    if kong_tasks:
        await asyncio.gather(*kong_tasks)
    logger.info(f"Kong data fetching/retrieval took: {datetime.now() - kong_fetch_start}")

    yield_calc_start = datetime.now()
    grand_total_usd = Decimal('0')
    grand_total_weighted_apr = Decimal('0')
    grand_total_weighted_yield7d = Decimal('0')
    grand_total_weighted_yield30d = Decimal('0')
    grand_total_usd_change7d = Decimal('0')
    grand_total_usd_change30d = Decimal('0')
    report_lines_by_chain = {} 

    for chain_id, chain_data in portfolio_by_chain.items():
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, f"Chain {chain_id}")
        report_lines_by_chain[chain_id] = [f"--- **{chain_name}** ---"]

        for vault_info in chain_data['vaults']:
            vault_address_lower = vault_info['address_lower']
            timeseries_data = kong_results.get((chain_id, vault_address_lower))

            if timeseries_data:
                try:
                    balance_for_yield_calc = vault_info['effective_balance_in_underlying']
                    underlying_token_price = Decimal(vault_data.get('tvl', {}).get('price') or '0') 

                    if balance_for_yield_calc <= 0: 
                         logger.warning(f"[{vault_info['display_name']}] Balance for yield calculation is zero or negative.")

                         yield_7d = Decimal('0')
                         yield_30d = Decimal('0')
                         usd_change_7d = Decimal('0')
                         usd_change_30d = Decimal('0')
                    else:
                        current_pps_kong, pps_7d_ago, pps_30d_ago = process_timeseries_data_with_decimal(timeseries_data)

                        yield_7d = calculate_yield_with_decimal(current_pps_kong, pps_7d_ago)
                        yield_30d = calculate_yield_with_decimal(current_pps_kong, pps_30d_ago)

                        retrieved_underlying_price = vault_info.get('underlying_token_price', Decimal('0'))

                        if underlying_token_price is None or underlying_token_price <= Decimal('0'):
                             logger.warning(f"[{vault_info['display_name']}] Underlying token price is zero or missing. Cannot calculate USD change.")
                             usd_change_7d = Decimal('0')
                             usd_change_30d = Decimal('0')
                        else:

                            pps_change_7d = current_pps_kong - pps_7d_ago
                            pps_change_30d = current_pps_kong - pps_30d_ago

                            retrieved_decimals = vault_info.get('decimals', 18) 
                            retrieved_effective_balance_hex = vault_info.get('effective_balance_hex', '0x0')

                            effective_balance_tokens = Decimal(int(retrieved_effective_balance_hex, 16)) / (Decimal(10)**retrieved_decimals)

                            logger.info(f"[{vault_info['display_name']}] USD Change Calc Inputs:")
                            logger.info(f"  effective_balance_tokens: {effective_balance_tokens}")
                            logger.info(f"  pps_change_7d: {pps_change_7d}")
                            logger.info(f"  pps_change_30d: {pps_change_30d}")
                            logger.info(f"  underlying_token_price: {underlying_token_price}")

                            usd_change_7d = (effective_balance_tokens * pps_change_7d) * retrieved_underlying_price
                            usd_change_30d = (effective_balance_tokens * pps_change_30d) * retrieved_underlying_price

                            logger.info(f"  Calculated usd_change_7d: {usd_change_7d}")
                            logger.info(f"  Calculated usd_change_30d: {usd_change_30d}")

                    vault_info['yield_7d'] = yield_7d
                    vault_info['yield_30d'] = yield_30d
                    vault_info['usd_change_7d'] = usd_change_7d
                    vault_info['usd_change_30d'] = usd_change_30d

                    chain_data['weighted_yield7d'] += yield_7d * vault_info['vault_usd_value']
                    chain_data['weighted_yield30d'] += yield_30d * vault_info['vault_usd_value']
                    chain_data['total_usd_change7d'] += usd_change_7d
                    chain_data['total_usd_change30d'] += usd_change_30d

                except Exception as e:
                    logger.error(f"Error calculating yield for {vault_info['name']} ({vault_address_lower}): {e}", exc_info=True)
                    vault_info['yield_7d'] = Decimal('0')
                    vault_info['yield_30d'] = Decimal('0')
                    vault_info['usd_change_7d'] = Decimal('0')
                    vault_info['usd_change_30d'] = Decimal('0')

            else:
                 logger.warning(f"No Kong timeseries data found or used for {vault_info['name']} ({vault_address_lower}) on chain {chain_id}.")
                 vault_info['yield_7d'] = Decimal('0')
                 vault_info['yield_30d'] = Decimal('0')
                 vault_info['usd_change_7d'] = Decimal('0')
                 vault_info['usd_change_30d'] = Decimal('0')

            staking_line = ""
            if vault_info['staked_status'] != "none" and vault_info['current_staking_apr_percent'] > 0:
                staking_line = f"\n  Staking APR: {vault_info['current_staking_apr_percent']:.2f}% *({escape_markdown(vault_info['current_staking_apr_source'])})*"

            usd_7d_str = f"${vault_info['usd_change_7d']:,.2f}" if vault_info['usd_change_7d'] < 0 else f"+${vault_info['usd_change_7d']:,.2f}"
            usd_30d_str = f"${vault_info['usd_change_30d']:,.2f}" if vault_info['usd_change_30d'] < 0 else f"+${vault_info['usd_change_30d']:,.2f}"

            report_lines_by_chain[chain_id].append(
                f"**[{vault_info['display_name']} ({vault_info['token_symbol']})]({vault_info['vault_url']})**{vault_info['staked_indicator']}\n"
                f"  Value: ${vault_info['vault_usd_value']:,.2f}\n"
                f"  Vault APY: {vault_info['vault_apr_percent']:.2f}%"
                f"{staking_line}"
                f"\n  Yield: {vault_info['yield_7d']:.2f}% [7d] ({usd_7d_str}), {vault_info['yield_30d']:.2f}% [30d] ({usd_30d_str})"
            )

        chain_total_usd = chain_data['total_usd']
        if chain_total_usd > 0:
            avg_apr = chain_data['weighted_apr'] / chain_total_usd
            avg_yield7d = chain_data['weighted_yield7d'] / chain_total_usd
            avg_yield30d = chain_data['weighted_yield30d'] / chain_total_usd
            chain_usd_7d_str = f"${chain_data['total_usd_change7d']:,.2f}" if chain_data['total_usd_change7d'] < 0 else f"+${chain_data['total_usd_change7d']:,.2f}"
            chain_usd_30d_str = f"${chain_data['total_usd_change30d']:,.2f}" if chain_data['total_usd_change30d'] < 0 else f"+${chain_data['total_usd_change30d']:,.2f}"

            report_lines_by_chain[chain_id].append(
                f"  ---\n"
                f"  **Chain Total: ${chain_total_usd:,.2f}**\n"
                f"  Avg Vault APY: {avg_apr:.2f}%\n"
                f"  Avg Yield: {avg_yield7d:.2f}% [7d] ({chain_usd_7d_str}), {avg_yield30d:.2f}% [30d] ({chain_usd_30d_str})"
            )

            grand_total_usd += chain_total_usd
            grand_total_weighted_apr += chain_data['weighted_apr']
            grand_total_weighted_yield7d += chain_data['weighted_yield7d']
            grand_total_weighted_yield30d += chain_data['weighted_yield30d']
            grand_total_usd_change7d += chain_data['total_usd_change7d']
            grand_total_usd_change30d += chain_data['total_usd_change30d']
        else:
             report_lines_by_chain[chain_id].append("  *No holdings found on this chain.*")

    logger.info(f"Yield calculation and report line formatting took: {datetime.now() - yield_calc_start}")

    final_report_lines = ["**Your Yearn Portfolio Report**"]
    if not portfolio_by_chain:
         final_report_lines.append("\n*No Yearn vault holdings found for the provided addresses.*")
    else:
        for chain_id in sorted(report_lines_by_chain.keys()):
            final_report_lines.extend(report_lines_by_chain[chain_id])
            final_report_lines.append("") 

        final_report_lines.append("--- **Overall Portfolio** ---")
        if grand_total_usd > 0:
            overall_avg_apr = grand_total_weighted_apr / grand_total_usd
            overall_avg_yield7d = grand_total_weighted_yield7d / grand_total_usd
            overall_avg_yield30d = grand_total_weighted_yield30d / grand_total_usd
            overall_usd_7d_str = f"${grand_total_usd_change7d:,.2f}" if grand_total_usd_change7d < 0 else f"+${grand_total_usd_change7d:,.2f}"
            overall_usd_30d_str = f"${grand_total_usd_change30d:,.2f}" if grand_total_usd_change30d < 0 else f"+${grand_total_usd_change30d:,.2f}"

            final_report_lines.append(f"💰 **Total Value: ${grand_total_usd:,.2f}**")
            final_report_lines.append(f"📊 Avg Vault APY: {overall_avg_apr:.2f}%")
            final_report_lines.append(f"📈 Avg Yield: {overall_avg_yield7d:.2f}% [7d] ({overall_usd_7d_str}), {overall_avg_yield30d:.2f}% [30d] ({overall_usd_30d_str})")
        else:

             final_report_lines.append("*No Yearn vault holdings found for the provided addresses.*")

    ydaemon_ts = api_cache['ydaemon'].get('timestamp', 0)
    kong_ts = api_cache['kong'].get('timestamp', 0)
    oneup_ts = api_cache['1up'].get('timestamp', 0)
    last_update_ts = max(ydaemon_ts, kong_ts, oneup_ts)
    if last_update_ts > 0:
        last_update_dt_aware = datetime.fromtimestamp(last_update_ts, tz=timezone.utc)

        discord_timestamp = f"<t:{int(last_update_ts)}:R>"
        cache_note = f"\n\n*Data cached approx. every {CACHE_EXPIRY_SECONDS / 3600:.0f} hours. Last update: {discord_timestamp}. Staking APRs shown may be max rates.*"
        final_report_lines.append(cache_note)
    else:
         final_report_lines.append("\n\n*Data freshness may vary. Staking APRs shown may be max rates.*")

    report_string = "\n".join(final_report_lines)

    suggestions_string = await generate_vault_suggestions_content(
        user_id, report_vaults_details, all_vaults_data, one_up_apr_data, one_up_gauge_map
    )

    staking_opportunities_string = await generate_staking_opportunities_content(staking_opportunities)

    try:
        await interaction.followup.send(truncate_message(report_string), ephemeral=True, suppress_embeds=True)

        if suggestions_string:
            await interaction.followup.send(truncate_message(suggestions_string), ephemeral=True, suppress_embeds=True)

        if staking_opportunities_string:
            await interaction.followup.send(truncate_message(staking_opportunities_string), ephemeral=True, suppress_embeds=True)

        daily_yport_usage_count += 1
        logger.info(f"/yport usage count incremented to: {daily_yport_usage_count}")

    except discord.errors.HTTPException as e:
        logger.error(f"Failed to send report follow-up for user {user_id}: {e}")

        try:
            await interaction.followup.send("❌ An error occurred while sending the report details. Please try again later.", ephemeral=True)
        except Exception:
            logger.error(f"Failed even to send the error message for user {user_id}")
    except Exception as e:
        logger.exception(f"Unexpected error during final report sending for user {user_id}: {e}")
        try:
            await interaction.followup.send("❌ An unexpected error occurred. Please try again later.", ephemeral=True)
        except Exception:
             logger.error(f"Failed even to send the unexpected error message for user {user_id}")

    generation_time = datetime.now() - generation_start_time
    logger.info(f"Report generation and sending for user {user_id} took {generation_time}")

async def generate_vault_suggestions_content(user_id, user_vaults_details, all_vaults_data, one_up_apr_data, one_up_gauge_map):
    if not user_vaults_details or not all_vaults_data:
        logger.info(f"Skipping suggestions for user {user_id}: No user vaults or yDaemon data.")
        return None

    logger.info(f"Generating suggestions for user {user_id} based on {len(user_vaults_details)} held vaults.")
    suggestions_by_chain = {} 
    suggested_vaults_set = set() 

    user_holdings_lookup = {}
    for v_detail in user_vaults_details:
        lookup_key = (v_detail['chainID'], v_detail['underlying_token_address'])
        if not v_detail['underlying_token_address']: continue 
        if lookup_key not in user_holdings_lookup: user_holdings_lookup[lookup_key] = []
        user_holdings_lookup[lookup_key].append(v_detail['apr']) 

    if not user_holdings_lookup:
        logger.info(f"No user vaults with underlying addresses found for suggestions for user {user_id}.")
        return None

    one_up_vault_to_gauge_map = {v: k for k, v in one_up_gauge_map.items()} if one_up_gauge_map else {}
    suggestion_processing_start = datetime.now()

    for potential_vault_data in all_vaults_data:
        try:
            chain_id = potential_vault_data.get('chainID')
            potential_vault_address = potential_vault_data.get('address', '').lower()
            potential_underlying_address = potential_vault_data.get('token', {}).get('address', '').lower()

            if not chain_id or not potential_vault_address or not potential_underlying_address: continue

            lookup_key = (chain_id, potential_underlying_address)
            user_aprs_for_this_underlying = user_holdings_lookup.get(lookup_key)

            if user_aprs_for_this_underlying:

                is_already_held = any(uv['address'] == potential_vault_address and uv['chainID'] == chain_id for uv in user_vaults_details)
                if is_already_held: continue

                tvl_usd = Decimal(potential_vault_data.get('tvl', {}).get('tvl') or '0')
                if tvl_usd < MIN_SUGGESTION_TVL_USD:
                    continue 

                net_apr = potential_vault_data.get('apr', {}).get('netAPR')
                potential_base_apr = Decimal(net_apr or '0') * Decimal('100')

                min_user_apr = min(user_aprs_for_this_underlying)
                apr_difference = potential_base_apr - min_user_apr

                if apr_difference > SUGGESTION_APR_THRESHOLD:
                    suggestion_key = (chain_id, potential_vault_address)
                    if suggestion_key in suggested_vaults_set:
                        continue 

                    suggested_staking_apr_lines = []

                    yearn_staking_available = potential_vault_data.get('staking', {}).get('available')
                    if yearn_staking_available:
                        yrn_stake_apr = Decimal('0')
                        rewards = potential_vault_data.get('staking', {}).get('rewards', [])
                        if rewards and rewards[0].get('apr') is not None:
                            try: yrn_stake_apr = Decimal(rewards[0]['apr']) * Decimal('100')
                            except: pass
                        elif potential_vault_data.get('apr', {}).get('extra', {}).get('stakingRewardsAPR') is not None:
                            try: yrn_stake_apr = Decimal(potential_vault_data['apr']['extra']['stakingRewardsAPR']) * Decimal('100')
                            except: pass
                        if yrn_stake_apr > 0:
                             suggested_staking_apr_lines.append(f"    + Yearn Staking: {yrn_stake_apr:.2f}% *(Max Boost)*")

                    one_up_gauge_addr = one_up_vault_to_gauge_map.get(potential_vault_address)
                    if one_up_gauge_addr and one_up_apr_data:
                         gauge_data = one_up_apr_data.get('gauges', {}).get(one_up_gauge_addr)
                         if gauge_data:
                             one_up_stake_apr = Decimal(gauge_data.get('reward_apr', 0))
                             if one_up_stake_apr > 0:
                                 suggested_staking_apr_lines.append(f"    + 1UP Staking: {one_up_stake_apr:.2f}%")

                    vault_display_name = escape_markdown(potential_vault_data.get('display_name') or potential_vault_data.get('name') or "Vault")
                    token_display_name = escape_markdown(potential_vault_data.get('token', {}).get('display_name') or potential_vault_data.get('token', {}).get('symbol') or "Asset")
                    vault_url = f"https://yearn.fi/v3/{chain_id}/{potential_vault_data['address']}"
                    formatted_tvl = format_tvl(tvl_usd) 

                    suggestion_text = (
                        f"**[{vault_display_name} ({token_display_name})]({vault_url})**\n"
                        f"  Vault APY: {potential_base_apr:.2f}% (+{apr_difference:.2f}%)\n"
                        f"  TVL: {formatted_tvl}"
                    )
                    if suggested_staking_apr_lines:
                        suggestion_text += "\n" + "\n".join(suggested_staking_apr_lines)

                    if chain_id not in suggestions_by_chain: suggestions_by_chain[chain_id] = []
                    suggestions_by_chain[chain_id].append({'text': suggestion_text, 'apr': potential_base_apr})
                    suggested_vaults_set.add(suggestion_key)

        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            logger.error(f"Error processing suggestion for vault {potential_vault_data.get('address', 'N/A')}: {e}", exc_info=True)
            continue

    logger.info(f"Suggestion processing took: {datetime.now() - suggestion_processing_start}")
    if not suggestions_by_chain: return None

    final_suggestions_lines = [f"**💡 Vault Suggestions (Higher APY & >{format_tvl(MIN_SUGGESTION_TVL_USD)} TVL)**"]
    for chain_id in sorted(suggestions_by_chain.keys()):
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, f"Chain {chain_id}")
        final_suggestions_lines.append(f"\n--- *{chain_name}* ---")

        sorted_suggestions = sorted(suggestions_by_chain[chain_id], key=lambda x: x['apr'], reverse=True)
        for suggestion in sorted_suggestions:
            final_suggestions_lines.append(suggestion['text'])

    return "\n".join(final_suggestions_lines)

async def generate_staking_opportunities_content(staking_opportunities_list):
    """Formats the staking opportunities suggestions for Discord."""
    if not staking_opportunities_list:
        return None

    logger.info(f"Formatting {len(staking_opportunities_list)} staking opportunities.")
    lines_by_chain = {} 

    for opp in staking_opportunities_list:
        chain_id = opp['chain_id']
        if chain_id not in lines_by_chain: lines_by_chain[chain_id] = []

        entry_lines = []

        entry_lines.append(f"**{opp['display_name']} ({opp['token_symbol']})**")

        if opp['yearn_apr'] > 0:
            entry_lines.append(f"  [veYFI]({opp['yearn_url']}): {opp['yearn_apr']:.2f}% APR *(Max Boost)*")

        if opp['1up_apr'] > 0 and opp['1up_gauge_address']:
            try:
                checksum_gauge_address = Web3.to_checksum_address(opp['1up_gauge_address'])

                chain_name_1up = "ethereum"
                one_up_url = f"https://1up.tokyo/stake/{chain_name_1up}/{checksum_gauge_address}"
                entry_lines.append(f"  [1UP]({one_up_url}): {opp['1up_apr']:.2f}% APR")
            except ValueError:
                 logger.warning(f"Could not checksum 1UP gauge address for URL: {opp['1up_gauge_address']}")
                 entry_lines.append(f"  1UP Staking: {opp['1up_apr']:.2f}% APR (URL Error)")
            except Exception as e:
                 logger.error(f"Error creating 1UP URL for {opp['1up_gauge_address']}: {e}")
                 entry_lines.append(f"  1UP Staking: {opp['1up_apr']:.2f}% APR (URL Error)")
        elif opp['1up_apr'] > 0: 
             entry_lines.append(f"  1UP Staking: {opp['1up_apr']:.2f}% APR (URL unavailable)")

        if len(entry_lines) > 1:
            lines_by_chain[chain_id].append("\n".join(entry_lines))

    if not lines_by_chain: return None

    final_lines = ["**⚡ Staking Opportunities (for your current holdings)**"]
    for chain_id in sorted(lines_by_chain.keys()):
        chain_name = CHAIN_ID_TO_NAME.get(chain_id, f"Chain {chain_id}")
        final_lines.append(f"\n--- *{chain_name}* ---")
        final_lines.extend(lines_by_chain[chain_id]) 

    return "\n".join(final_lines)

intents = discord.Intents.default()
intents.messages = True         
intents.message_content = True  
intents.guilds = True           

bot = commands.Bot(command_prefix="!", intents=intents) 

@bot.event
async def on_ready():
    logger.info(f'{bot.user} has connected to Discord!')
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} commands.")
    except Exception as e:
        logger.error(f"Failed to sync commands: {e}")

    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if log_channel:
        await log_channel.send(f'{bot.user} is now online and ready! Synced {len(synced)} commands.')

    initialize_ens()

    if not scheduled_cache_update_task.is_running():
        scheduled_cache_update_task.start()
    if not daily_top_vaults_report_task.is_running():
        daily_top_vaults_report_task.start()
    if not send_daily_usage_report_task.is_running():
        send_daily_usage_report_task.start()

@bot.event
async def on_message(message: discord.Message):
    if message.author == bot.user:
        return 

    if isinstance(message.channel, discord.DMChannel):
        logger.info(f"Received DM from user {message.author.id}")
        inputs = message.content.split()
        valid_addresses = []
        invalid_inputs = []
        resolved_ens = {} 

        if not inputs:
            await message.channel.send("Please send one or more Ethereum wallet addresses (like `0x...`) or ENS names (like `yourname.eth`), separated by spaces.")
            return

        for item in inputs:
            item = item.strip()
            if not item: continue

            try:
                if Web3.is_address(item):
                    checksummed = Web3.to_checksum_address(item)
                    valid_addresses.append(checksummed)
                    logger.debug(f"Validated address: {checksummed}")

                elif (".eth" in item.lower() or ".xyz" in item.lower()) and ns:
                    resolved_address = await resolve_ens_async(item)
                    if resolved_address:
                        checksummed = Web3.to_checksum_address(resolved_address)
                        valid_addresses.append(checksummed)
                        resolved_ens[item] = checksummed
                        logger.info(f"Resolved ENS {item} to {checksummed}")
                    else:
                        invalid_inputs.append(f"{item} (ENS not resolved)")
                        logger.warning(f"Could not resolve ENS: {item}")
                elif (".eth" in item.lower() or ".xyz" in item.lower()) and not ns:
                     invalid_inputs.append(f"{item} (ENS resolver unavailable)")
                     logger.warning(f"Skipping ENS {item} because resolver is unavailable.")
                else:
                    invalid_inputs.append(f"{item} (Invalid format)")
                    logger.warning(f"Invalid input format: {item}")
            except Exception as e:
                logger.error(f"Error processing input '{item}': {e}", exc_info=True)
                invalid_inputs.append(f"{item} (Processing error)")

        user_id = str(message.author.id)
        response_lines = []

        if valid_addresses:
            unique_addresses = sorted(list(set(valid_addresses))) 

            user_data[user_id] = {'eoas': unique_addresses}
            logger.info(f"Saved/Replaced {len(unique_addresses)} addresses for user {user_id}.")

            response_lines.append(f"✅ Addresses received and saved ({len(unique_addresses)} unique):")
            for addr in unique_addresses:

                original_ens = next((ens for ens, res_addr in resolved_ens.items() if res_addr == addr), None)
                if original_ens:
                    response_lines.append(f"- `{addr}` (from {original_ens})")
                else:
                    response_lines.append(f"- `{addr}`")

            response_lines.append(f"\nYou can now use the `/yport` command in https://discord.com/channels/734804446353031319/1279431421760507976.")
            response_lines.append("\n*Sending new addresses will replace the current list.*")

        if invalid_inputs:
             response_lines.append(f"\n⚠️ Some inputs could not be processed:")
             for invalid in invalid_inputs:
                 response_lines.append(f"  - {invalid}")

        if not valid_addresses and not invalid_inputs:
             response_lines.append("No valid addresses or ENS names found in your message.")

        await message.channel.send("\n".join(response_lines), suppress_embeds=True)

    await bot.process_commands(message)

@bot.tree.command(name="yport", description="Generate your Yearn Vaults report (requires addresses sent via DM)")
async def yport_command(interaction: Interaction):
    user_id = str(interaction.user.id)
    now = datetime.utcnow()

    last_time = last_report_times.get(user_id)
    if last_time and now - last_time < timedelta(seconds=RATE_LIMIT_SECONDS):
        await interaction.response.send_message(f"⏳ Please wait {RATE_LIMIT_SECONDS} seconds before requesting another report.", ephemeral=True)
        return
    last_report_times[user_id] = now

    if user_id not in report_locks:
        report_locks[user_id] = asyncio.Lock()
    lock = report_locks[user_id]

    if lock.locked():
        await interaction.response.send_message("⏳ Your previous report request is still processing. Please wait.", ephemeral=True)
        return

    async with lock:
        try:
            await interaction.response.defer(ephemeral=True, thinking=True) 
            await generate_user_report(user_id, interaction)

        except Exception as e:
            logger.exception(f"Error during /yport command for user {user_id}: {e}")
            try:
                await interaction.followup.send("🚨 An unexpected error occurred while generating your report. Please try again later or contact support.", ephemeral=True)
            except discord.errors.InteractionResponded:
                 logger.warning(f"Interaction already responded to for user {user_id}, cannot send error followup.")
            except Exception as followup_e:
                 logger.error(f"Failed to send error followup message for user {user_id}: {followup_e}")

async def resolve_ens_async(ens_name):
    if not ns:
        logger.warning("ENS resolver (ns) is not initialized.")
        return None
    try:
        loop = asyncio.get_running_loop()
        address = await loop.run_in_executor(None, ns.address, ens_name)
        return address
    except Exception as e:
        logger.error(f"ENS resolution failed for '{ens_name}': {e}")
        return None

@tasks.loop(seconds=CACHE_EXPIRY_SECONDS) 
async def scheduled_cache_update_task():
    logger.info("Running scheduled cache update...")
    ydaemon_updated = await update_ydaemon_cache()

    one_up_updated = await update_1up_cache()

    if one_up_updated:
        await update_1up_gauge_map_cache()
    else:
        logger.warning("Skipping 1UP gauge map update because 1UP APR update failed.")

    if ydaemon_updated and api_cache['ydaemon'].get('data'):
        vaults_for_kong = set()
        for vault in api_cache['ydaemon']['data']:

            if vault.get('chainID') and vault.get('address'):
                vaults_for_kong.add((vault['chainID'], vault['address']))
        if vaults_for_kong:
            logger.info(f"Attempting to update Kong cache for {len(vaults_for_kong)} vaults...")
            await update_kong_cache(list(vaults_for_kong))
        else:
            logger.info("No vaults with chainID/address found in yDaemon cache for Kong update.")
    else:
        logger.warning("Skipping Kong cache update because yDaemon update failed or returned no data.")
    logger.info("Scheduled cache update finished.")

@scheduled_cache_update_task.before_loop
async def before_scheduled_cache_update():
    await bot.wait_until_ready()
    logger.info("Bot is ready, starting initial cache update...")
    await asyncio.sleep(5) 
    await scheduled_cache_update_task() 
    logger.info("Initial cache update finished.")

@tasks.loop(hours=3) 
async def daily_top_vaults_report_task():
    global last_scheduled_report_message_id
    logger.info("Generating scheduled top vaults report...")

    try:
        url = "https://ydaemon.yearn.fi/vaults/detected?limit=2000"
        async with aiohttp.ClientSession() as session:
             async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    vaults_data = await response.json()
                else:
                    logger.error(f"Failed to fetch yDaemon data for top vaults report: Status {response.status}")
                    return 

        if not vaults_data:
            logger.warning("No vault data received for the top vaults report.")
            return

        single_asset_addresses_lower = {
            addr.lower() for chain_tokens in SINGLE_ASSET_TOKENS.values() for addr in chain_tokens.values()
        }

        filtered_vaults = []
        for vault in vaults_data:
            try: 

                if not vault.get('address') or not vault.get('chainID') or not vault.get('token', {}).get('address'):
                    continue

                if vault.get('info', {}).get('retired', False):
                    continue

                if vault.get('kind') == 'Legacy':
                    continue

                if vault['token']['address'].lower() not in single_asset_addresses_lower:
                    continue

                if Decimal(vault.get('tvl', {}).get('tvl', 0)) < Decimal('50000'):
                     continue

                apr_data = vault.get('apr', {})
                points_data = apr_data.get('points', {})

                net_apr = apr_data.get('netAPR') 
                week_ago_yield = points_data.get('weekAgo') 
                month_ago_yield = points_data.get('monthAgo') 

                primary_apr = 0.0 
                if net_apr is not None:
                    primary_apr = float(net_apr)
                elif week_ago_yield is not None:
                    primary_apr = float(week_ago_yield)
                elif month_ago_yield is not None:
                    primary_apr = float(month_ago_yield)

                if abs(primary_apr) < 0.000001: 
                    continue 

                vault['_sort_apr'] = primary_apr 
                filtered_vaults.append(vault)

            except Exception as filter_err:
                 logger.error(f"Error filtering vault {vault.get('address', 'N/A')}: {filter_err}", exc_info=True)
                 continue 
                
        try:
            filtered_vaults.sort(key=lambda v: float(v.get('_sort_apr', 0.0)), reverse=True)
        except Exception as sort_err:
             logger.error(f"Error sorting filtered vaults: {sort_err}", exc_info=True)
             return 

        top_vaults = filtered_vaults[:5]

        if not top_vaults:
            logger.info("No suitable vaults found for the top 5 report after filtering.")
            return

        yearn_tvl = await fetch_yearn_tvl_llama() 
        formatted_tvl = format_tvl(Decimal(yearn_tvl)) if yearn_tvl else "Unavailable"

        embed = Embed(
            title="🏆 Top 5 Single-Asset Vaults by APY",
            description=f"*Based on available APY data. Total Yearn TVL: {formatted_tvl}*",
            color=discord.Color.blue() 
        )

        for idx, vault in enumerate(top_vaults, start=1):
            name = escape_markdown(vault.get('display_name') or vault.get('name', 'Unknown'))
            token = escape_markdown(vault.get('token', {}).get('symbol', '?'))
            chain_id = vault.get('chainID')
            chain = escape_markdown(CHAIN_ID_TO_NAME.get(chain_id, '?'))
            vault_address = vault.get('address')

            apr_value = vault.get('_sort_apr', 0)
            apr_percent = Decimal(apr_value) * Decimal('100')
            tvl_vault = format_tvl(Decimal(vault.get('tvl', {}).get('tvl', 0)))
            vault_url = f"https://yearn.fi/v3/{chain_id}/{vault_address}"

            embed.add_field(
                name=f"{idx}. {name}",
                value=f"[{token} / {chain}]({vault_url})\nAPY: **{apr_percent:.2f}%** | TVL: {tvl_vault}",
                inline=False
            )

        now_ts = int(datetime.utcnow().timestamp())
        embed.set_footer(text=f"Updated every 3 hours.")
        embed.timestamp = datetime.utcnow() 

        channel = bot.get_channel(PUBLIC_CHANNEL_ID)
        if channel:
            if last_scheduled_report_message_id:
                try:
                    old_message = await channel.fetch_message(last_scheduled_report_message_id)
                    await old_message.delete()
                    logger.info(f"Deleted previous scheduled report message: {last_scheduled_report_message_id}")
                except discord.NotFound:
                    logger.warning(f"Previous report message {last_scheduled_report_message_id} not found.")
                except discord.Forbidden:
                     logger.error(f"Missing permissions to delete message {last_scheduled_report_message_id} in channel {PUBLIC_CHANNEL_ID}.")
                except Exception as e:
                    logger.error(f"Failed to delete previous report message {last_scheduled_report_message_id}: {e}")

            new_message = await channel.send(embed=embed)
            last_scheduled_report_message_id = new_message.id
            logger.info(f"Sent scheduled top vaults report. New message ID: {new_message.id}")
        else:
            logger.error(f"Could not find public channel with ID: {PUBLIC_CHANNEL_ID}")

    except Exception as e:
        logger.exception(f"Error generating the scheduled top vaults report: {e}")
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            await log_channel.send(f"🚨 Error generating the scheduled top vaults report: {e}")

@daily_top_vaults_report_task.before_loop
async def before_daily_top_vaults_report():
    await bot.wait_until_ready()

async def fetch_yearn_tvl_llama():
    url = "https://api.llama.fi/tvl/yearn"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    tvl = await response.json()
                    return float(tvl) 
                else:
                    logger.error(f"Error fetching Yearn TVL from DeFiLlama: Status {response.status}")
                    return 0
    except asyncio.TimeoutError:
        logger.error("Timeout fetching Yearn TVL from DeFiLlama.")
        return 0
    except Exception as e:
        logger.error(f"Exception occurred while fetching Yearn TVL: {str(e)}")
        return 0

@tasks.loop(time=time(hour=0, minute=1, tzinfo=timezone.utc)) 
async def send_daily_usage_report_task():
    global daily_yport_usage_count
    logger.info("Preparing daily usage report.")

    if daily_yport_usage_count > 0:
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            try:
                await log_channel.send(f"📊 Daily Bot Usage Report:\n`/yport` commands used: {daily_yport_usage_count}")
                logger.info(f"Sent daily usage report to log channel {LOG_CHANNEL_ID}.")
            except Exception as e:
                 logger.error(f"Failed to send daily usage report to log channel {LOG_CHANNEL_ID}: {e}")
        else:
             logger.warning(f"Log channel {LOG_CHANNEL_ID} not found for usage report.")
    else:
        logger.info("Skipping daily usage report as count is zero.")

    daily_yport_usage_count = 0
    logger.info("Usage counter reset.")

@send_daily_usage_report_task.before_loop
async def before_send_daily_usage_report():
    await bot.wait_until_ready()

if __name__ == "__main__":
    if BOT_TOKEN == 'x' or API_KEY == 'x':
         print("ERROR: BOT_TOKEN or API_KEY is not set. Please configure them.")
    else:
        try:
            logger.info("Starting bot...")
            bot.run(BOT_TOKEN, log_handler=None) 
        except discord.LoginFailure:
            logger.error("Failed to log in: Improper token provided.")
        except Exception as e:
            logger.critical(f"An error occurred while running the bot: {e}", exc_info=True)
