import os
import requests
import sys
import aiohttp
import json 
from datetime import datetime, timedelta, time, timezone
from web3 import Web3
from ens import ENS
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CallbackQuery 
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    CallbackQueryHandler,
    JobQueue,
)
import logging
import asyncio
import re
from decimal import Decimal, getcontext, InvalidOperation
from pathlib import Path 

# Config
sys.stdout = sys.stderr

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO 
)

logging.getLogger("httpx").setLevel(logging.WARNING) 

logger = logging.getLogger(__name__)

BOT_TOKEN = 'BOT_TOKEN'
ADMIN_CHAT_ID = 'CHAT_ID'
API_KEY = 'API_KEY'

if BOT_TOKEN == 'YOUR_BOT_TOKEN' or API_KEY == 'YOUR_ALCHEMY_API_KEY':
    logger.warning("Bot token or Alchemy API key not set via environment variables.")

getcontext().prec = 28  

# Constants 
CALLBACK_YPORT = "action:yport"
CALLBACK_START_DAILY = "action:start_daily"
CALLBACK_STOP_DAILY = "action:stop_daily"

CACHE_FILE_YDAEMON = Path("cache_ydaemon.json")
CACHE_FILE_KONG = Path("cache_kong.json")
CACHE_EXPIRY_SECONDS = 3 * 60 * 60 

CACHE_FILE_1UP = Path("cache_1up.json")
ONE_UP_API_URL = "https://1up.s3.pl-waw.scw.cloud/aprs.json"

MIN_SUGGESTION_TVL_USD = Decimal('50000') 

# ABIs
YEARN_GAUGE_ABI = json.loads('[{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"boostedBalanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"_account","type":"address"}],"name":"earned","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"rewardRate","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"asset","outputs":[{"internalType":"contract IERC20","name":"","type":"address"}],"stateMutability":"view","type":"function"}]') 
ONE_UP_GAUGE_ABI = json.loads('[{"stateMutability":"view","type":"function","name":"balanceOf","inputs":[{"name":"_account","type":"address"}],"outputs":[{"name":"","type":"uint256"}]},{"stateMutability":"view","type":"function","name":"asset","inputs":[],"outputs":[{"name":"","type":"address"}]}]') 
ERC20_ABI_SIMPLIFIED = json.loads('[{"constant":true,"inputs":[{"name":"_owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"balance","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"}]') 

# Data
user_states = {} 
user_data = {}   
report_locks = {} 
daily_report_schedules = {} 

on_demand_report_count = 0
automated_daily_report_count = 0

api_cache = {
    'ydaemon': {'data': None, 'timestamp': 0},
    'kong': {'data': {}, 'timestamp': 0},
    '1up': {'data': None, 'timestamp': 0}, 
    '1up_gauge_map': {'data': {}, 'timestamp': 0}
}

# Chains
CHAIN_TO_ALCHEMY_PREFIX = {
    1: 'eth-mainnet',      
    10: 'opt-mainnet',     
    137: 'polygon-mainnet',
    42161: 'arb-mainnet',  
    8453: 'base-mainnet',   

}
CHAIN_NAMES = {
    1: 'Ethereum',
    10: 'Optimism',
    137: 'Polygon',
    42161: 'Arbitrum',
    8453: 'Base',
}

# Web3
web3_instances = {}
def get_web3_instance(chain_id):
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

w3_eth = get_web3_instance(1) 
ns = None
if w3_eth:
    try:
        ns = ENS.from_web3(w3_eth)
    except Exception as e:
        logger.error(f"Failed to initialize ENS: {e}")
else:
    logger.error("Failed to initialize Ethereum Web3 instance, ENS and 1UP asset() calls may fail.")

async def update_ydaemon_cache():
    """Fetches latest yDaemon data and updates the cache."""
    global api_cache
    logger.info("Attempting to update yDaemon cache...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://ydaemon.yearn.fi/vaults/detected?limit=2000") as response:
                if response.status == 200:
                    data = await response.json()
                    api_cache['ydaemon']['data'] = data
                    api_cache['ydaemon']['timestamp'] = datetime.utcnow().timestamp()
                    logger.info(f"yDaemon cache updated successfully. {len(data)} vaults.")

                    return True
                else:
                    logger.error(f"Failed to fetch yDaemon data: Status {response.status}")
                    return False
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching yDaemon data: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating yDaemon cache: {e}")
        return False

async def update_kong_cache(vaults_to_update):
    """Fetches historical PPS data for given vaults concurrently and updates the Kong cache."""
    global api_cache
    if not vaults_to_update:
        logger.info("No vaults provided for Kong cache update.")
        return

    logger.info(f"Attempting to update Kong cache for {len(vaults_to_update)} vaults...")
    updated_count = 0
    tasks = []

    CONCURRENCY_LIMIT = 200
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    kong_cache_data = api_cache['kong'].get('data', {})
    new_kong_data = {} 

    async def fetch_and_store_kong(vault_address, chain_id):
        """Helper to fetch, manage semaphore, and store result."""
        nonlocal updated_count
        cache_key = (chain_id, vault_address.lower())

        async with semaphore:
            logger.debug(f"Fetching Kong data for {vault_address} on chain {chain_id} (Semaphore acquired)")

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
        logger.info("No Kong fetch tasks were created (perhaps all data was considered up-to-date).")

async def update_1up_cache():
    """Fetches 1UP APR data and updates the cache."""
    global api_cache
    logger.info("Attempting to update 1UP APR cache...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(ONE_UP_API_URL, timeout=15) as response:
                logger.debug(f"1UP API response status: {response.status}")
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    logger.debug(f"1UP API response content-type: {content_type}")
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

                            if '1up' in api_cache:
                                logger.debug(f"Attempting to assign data to api_cache['1up']. Current api_cache keys: {list(api_cache.keys())}")
                                api_cache['1up']['data'] = processed_data
                                api_cache['1up']['timestamp'] = datetime.utcnow().timestamp()
                                logger.info(f"1UP APR cache updated successfully. Found {len(processed_data.get('gauges', {}))} gauges.")

                                return True
                            else:

                                logger.critical("CRITICAL ERROR: api_cache dictionary is missing the '1up' key during update! Cache state might be corrupted.")
                                return False

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
    except aiohttp.ClientError as net_err:
        logger.error(f"Network error fetching 1UP APR data: {net_err}", exc_info=True)
        return False
    except asyncio.TimeoutError:
         logger.error("Timeout error fetching 1UP APR data.")
         return False
    except Exception as e:

        logger.error(f"Unexpected error during 1UP APR cache update. Type: {type(e)}, Error: {e}", exc_info=True)
        return False

async def update_1up_gauge_map_cache():
    """Fetches underlying asset for 1UP gauges and updates the mapping cache."""
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

    for gauge_address_str in gauges_to_query:
        try:
            if not Web3.is_address(gauge_address_str):
                logger.warning(f"Skipping invalid address in 1UP data: {gauge_address_str}")
                continue

            gauge_address = Web3.to_checksum_address(gauge_address_str)
            gauge_contract = w3.eth.contract(address=gauge_address, abi=ONE_UP_GAUGE_ABI)

            underlying_asset_address = gauge_contract.functions.asset().call()

            if underlying_asset_address and Web3.is_address(underlying_asset_address):
                gauge_map[gauge_address.lower()] = Web3.to_checksum_address(underlying_asset_address).lower()
                updated_count += 1
            else:
                logger.warning(f"Could not retrieve valid asset address for 1UP gauge: {gauge_address}")

        except Exception as e:

            logger.error(f"Error calling 'asset()' on 1UP gauge {gauge_address}: {e}")
            continue 

    if updated_count > 0:
        api_cache['1up_gauge_map']['data'] = gauge_map
        api_cache['1up_gauge_map']['timestamp'] = datetime.utcnow().timestamp()
        logger.info(f"1UP Gauge -> Vault mapping cache updated for {updated_count} gauges.")

        return True
    else:
        logger.info("No new 1UP gauge mappings were successfully retrieved.")

        return False

async def scheduled_cache_update(context: CallbackContext):
    """Job to periodically update all caches."""
    logger.info("Running scheduled cache update...")
    ydaemon_updated = await update_ydaemon_cache()

    one_up_updated = await update_1up_cache()

    if one_up_updated:
        await update_1up_gauge_map_cache()
    else:
        logger.warning("Skipping 1UP gauge map update because 1UP APR update failed.")

    if ydaemon_updated and api_cache['ydaemon']['data']:
        vaults_for_kong = set()
        for vault in api_cache['ydaemon']['data']:
            if vault.get('chainID') and vault.get('address'):
                vaults_for_kong.add((vault['chainID'], vault['address']))
        await update_kong_cache(list(vaults_for_kong))
    else:
        logger.warning("Skipping Kong cache update because yDaemon update failed or returned no data.")

def get_1up_data():
    """Returns 1UP APR data, checking cache first."""
    now = datetime.utcnow().timestamp()
    cache_entry = api_cache['1up']
    if cache_entry.get('data') and (now - cache_entry.get('timestamp', 0) < CACHE_EXPIRY_SECONDS):
        logger.debug("Using cached 1UP APR data.")
        return cache_entry['data']
    else:
        logger.warning("1UP APR cache miss or expired.")
        return cache_entry.get('data') 

def get_1up_gauge_map():
    """Returns 1UP Gauge -> Vault mapping, checking cache first."""
    now = datetime.utcnow().timestamp()
    cache_entry = api_cache['1up_gauge_map']
    if cache_entry.get('data') and (now - cache_entry.get('timestamp', 0) < CACHE_EXPIRY_SECONDS):
        logger.debug("Using cached 1UP gauge map.")
        return cache_entry['data']
    else:
        logger.warning("1UP gauge map cache miss or expired.")
        return cache_entry.get('data') 



async def fetch_detailed_vault_data(chain_id, vault_address):
    """Fetches detailed data for a single vault from yDaemon."""
    url = f"https://ydaemon.yearn.fi/{chain_id}/vaults/{vault_address}?strategiesDetails=withDetails&strategiesCondition=inQueue"
    logger.debug(f"Fetching detailed data for vault {vault_address} on chain {chain_id}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    content_type = response.headers.get('Content-Type', '').lower()
                    if 'application/json' in content_type:
                        try:
                            data = await response.json()
                            return data 
                        except aiohttp.ContentTypeError as json_err:
                            logger.error(f"Failed to decode detailed vault JSON for {vault_address}: {json_err}", exc_info=True)
                            return None
                    else:
                         logger.warning(f"Detailed vault fetch for {vault_address} returned non-JSON content type: {content_type}")
                         return None
                else:
                    logger.warning(f"Failed to fetch detailed vault data for {vault_address}: Status {response.status}")
                    return None
    except asyncio.TimeoutError:
        logger.warning(f"Timeout fetching detailed vault data for {vault_address}")
        return None
    except aiohttp.ClientError as net_err:
        logger.warning(f"Network error fetching detailed vault data for {vault_address}: {net_err}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching detailed vault data for {vault_address}: {e}", exc_info=True)
        return None

def get_ydaemon_data():
    """Returns yDaemon data, checking cache first."""
    now = datetime.utcnow().timestamp()
    if api_cache['ydaemon']['data'] and (now - api_cache['ydaemon']['timestamp'] < CACHE_EXPIRY_SECONDS):
        logger.debug("Using cached yDaemon data.")
        return api_cache['ydaemon']['data']
    else:
        logger.warning("yDaemon cache miss or expired. Need to update.")

        return api_cache['ydaemon']['data'] 

async def get_kong_data(vault_address, chain_id):
    """Returns Kong timeseries data, checking cache first."""
    now = datetime.utcnow().timestamp()
    cache_key = (chain_id, vault_address.lower())
    kong_cache = api_cache['kong']

    if kong_cache['data'] and cache_key in kong_cache['data'] and (now - kong_cache['timestamp'] < CACHE_EXPIRY_SECONDS):
         logger.debug(f"Using cached Kong data for {vault_address} on chain {chain_id}.")
         return kong_cache['data'][cache_key]
    else:
        logger.info(f"Kong cache miss or expired for {vault_address} on chain {chain_id}. Fetching live.")

        live_data = await fetch_historical_pricepershare_kong_live(vault_address, chain_id)
        if live_data:

            kong_cache['data'][cache_key] = live_data
            kong_cache['timestamp'] = now 
            api_cache['kong'] = kong_cache 

        return live_data

async def fetch_historical_pricepershare_kong_live(vault_address, chain_id, limit=1000):
    """Fetches historical pricePerShare data directly from Kong API."""
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
            async with session.post(url, json={"query": query, "variables": variables}) as response:
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
    except aiohttp.ClientError as e:
        logger.error(f"Network error fetching data from Kong for {vault_address}: {e}")
        return None
    except Exception as e:
        logger.error(f"Exception while fetching data from Kong for {vault_address}: {str(e)}")
        return None



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
        logger.error(f"Unexpected error in get_token_balances for {eoa} on chain {chain_id}: {e}")
        return {}



def process_timeseries_data_with_decimal(timeseries):
    """Calculates current, 7d ago, and 30d ago PPS from timeseries data."""
    if not timeseries or not isinstance(timeseries, list) or len(timeseries) == 0:
        return Decimal('0'), Decimal('0'), Decimal('0')

    try:

        sorted_timeseries = sorted(timeseries, key=lambda x: int(x['time'])) 

        now = datetime.utcnow()
        timestamp_7d_ago = int((now - timedelta(days=7)).timestamp())
        timestamp_30d_ago = int((now - timedelta(days=30)).timestamp())

        def find_closest_entry(target_timestamp):

            relevant_entries = [entry for entry in sorted_timeseries if int(entry['time']) <= target_timestamp]
            if not relevant_entries:

                return sorted_timeseries[0]

            return relevant_entries[-1]

        current_pps_entry = sorted_timeseries[-1] 
        pps_7d_ago_entry = find_closest_entry(timestamp_7d_ago)
        pps_30d_ago_entry = find_closest_entry(timestamp_30d_ago)

        current_pps = Decimal(current_pps_entry["value"])
        pps_7d_ago = Decimal(pps_7d_ago_entry["value"])
        pps_30d_ago = Decimal(pps_30d_ago_entry["value"])

        return current_pps, pps_7d_ago, pps_30d_ago

    except (KeyError, ValueError, TypeError, InvalidOperation) as e:
        logger.error(f"Error processing timeseries data: {e} - Data: {timeseries[:5]}") 
        return Decimal('0'), Decimal('0'), Decimal('0')

def calculate_yield_with_decimal(current_pps, historical_pps):
    """Calculates yield percentage."""
    if historical_pps == Decimal('0'):
        return Decimal('0') 
    try:

        current_pps_dec = Decimal(current_pps)
        historical_pps_dec = Decimal(historical_pps)
        return ((current_pps_dec / historical_pps_dec) - Decimal('1')) * Decimal('100')
    except (InvalidOperation, TypeError) as e:
        logger.error(f"Error calculating yield: current={current_pps}, historical={historical_pps}, error={e}")
        return Decimal('0')



def truncate_html_message(message, max_length=4096):
    """Truncates HTML message safely, closing open tags."""
    if len(message) <= max_length:
        return message

    effective_max_length = max_length - 30
    truncated_message = message[:effective_max_length]

    last_lt = truncated_message.rfind('<')
    last_gt = truncated_message.rfind('>')
    if last_lt > last_gt: 
        truncated_message = truncated_message[:last_lt]

    open_tags = []

    tag_matches = re.finditer(r'<(/?)(\w+)(?:[^>]*?(/)?)>', truncated_message)

    for match in tag_matches:
        is_closing_tag = match.group(1) == '/'
        tag_name = match.group(2).lower()
        is_self_closing = match.group(3) == '/'

        if is_self_closing:
            continue 

        if is_closing_tag:

            if tag_name in open_tags:

                for i in range(len(open_tags) - 1, -1, -1):
                    if open_tags[i] == tag_name:
                        open_tags.pop(i)
                        break
        else:

            open_tags.append(tag_name)

    for tag in reversed(open_tags):
        truncated_message += f'</{tag}>'

    return truncated_message + "\n\n[...] <i>(Report truncated)</i>"



async def generate_report_content(user_id):
    """
    Generates the main report, vault suggestions data, and staking opportunities data.
    Returns: (report_string, suggestions_string, staking_opportunities_list)
    """
    user_specific_data = user_data.get(str(user_id))
    if not user_specific_data or 'eoas' not in user_specific_data:
        logger.warning(f"No EOAs found for user {user_id} during report generation.")

        return ("⚠️ No addresses found. Please use /start to add addresses.", None, [])

    eoas = user_specific_data['eoas']
    logger.info(f"Generating report for user {user_id} with EOAs: {eoas}")

    all_vaults_data = get_ydaemon_data()
    one_up_apr_data = get_1up_data()
    one_up_gauge_map = get_1up_gauge_map()

    if not all_vaults_data:
        logger.error(f"yDaemon data unavailable for report generation for user {user_id}.")
        return ("🚨 Unable to retrieve vault data. Cache might be updating. Please try again shortly.", None, [])

    one_up_vault_to_gauge_map = {v: k for k, v in one_up_gauge_map.items()} if one_up_gauge_map else {}

    all_token_balances = {}
    logger.info(f"Fetching balances for {len(eoas)} EOAs across {len(CHAIN_TO_ALCHEMY_PREFIX)} chains...")
    balance_fetch_start = datetime.now()

    for eoa in eoas:
        for chain_id in CHAIN_TO_ALCHEMY_PREFIX.keys():
            balances = get_token_balances(eoa, chain_id)
            if balances:
                if chain_id not in all_token_balances: all_token_balances[chain_id] = {}
                if eoa not in all_token_balances[chain_id]: all_token_balances[chain_id][eoa] = {}
                all_token_balances[chain_id][eoa].update({k.lower(): v for k, v in balances.items()})
    logger.info(f"Balance fetching took: {datetime.now() - balance_fetch_start}")

    portfolio_by_chain = {}
    report_vaults_details = [] 
    staking_opportunities = [] 
    kong_fetch_tasks = []
    vaults_requiring_kong = set()

    logger.info(f"Processing {len(all_vaults_data)} vaults from yDaemon data...")
    processing_start = datetime.now()

    for vault_data in all_vaults_data:
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
        if chain_id in all_token_balances:
            for eoa in eoas:
                eoa_balances = all_token_balances[chain_id].get(eoa, {})
                bal = eoa_balances.get(vault_address_lower)
                if bal: vault_balance_hex = hex(int(vault_balance_hex, 16) + int(bal, 16))
                if yearn_gauge_address_lower:
                    bal = eoa_balances.get(yearn_gauge_address_lower)
                    if bal: yearn_gauge_balance_hex = hex(int(yearn_gauge_balance_hex, 16) + int(bal, 16))
                if one_up_gauge_address_lower:
                    bal = eoa_balances.get(one_up_gauge_address_lower)
                    if bal: one_up_gauge_balance_hex = hex(int(one_up_gauge_balance_hex, 16) + int(bal, 16))

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

        if one_up_staking_available and one_up_apr_data:
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

        try:
            name = vault_data.get('name', 'Unknown')
            symbol = vault_data.get('symbol', 'yVault')
            decimals = int(vault_data.get('decimals', 18))
            display_name = vault_data.get('display_name', name)
            token_data = vault_data.get('token', {})
            token_symbol = token_data.get('display_name') or token_data.get('symbol') or "Asset"
            underlying_token_address = token_data.get('address', '').lower()

            price_per_share_str = vault_data.get('pricePerShare')
            if price_per_share_str is None: continue
            price_per_share = Decimal(price_per_share_str) / (Decimal(10) ** decimals)

            underlying_token_price = Decimal(vault_data.get('tvl', {}).get('price') or '0')

            effective_balance_int = int(effective_balance_hex, 16)
            effective_balance_in_underlying = (Decimal(effective_balance_int) / (Decimal(10) ** decimals)) * price_per_share
            vault_usd_value = effective_balance_in_underlying * underlying_token_price

            if vault_usd_value < Decimal('0.01'): continue

            net_apr = vault_data.get('apr', {}).get('netAPR')
            vault_apr_percent = Decimal(net_apr or '0') * Decimal('100')

            vaults_requiring_kong.add((chain_id, vault_data['address']))

            staked_indicator = ""
            if staked_status == "yearn": staked_indicator = " <i>(Staked: Yearn)</i>"
            elif staked_status == "1up": staked_indicator = " <i>(Staked: 1UP)</i>"

            vault_info = {
                'name': name, 'symbol': symbol, 'display_name': display_name,
                'token_symbol': token_symbol, 'vault_usd_value': vault_usd_value,
                'vault_apr_percent': vault_apr_percent, 'staked_status': staked_status,
                'staked_indicator': staked_indicator,
                'current_staking_apr_percent': current_staking_apr_percent,
                'current_staking_apr_source': current_staking_apr_source,
                'vault_url': f"https://yearn.fi/v3/{chain_id}/{vault_data['address']}",
                'chain_id': chain_id, 'address': vault_data['address'], 

                'yield_7d': Decimal('0'), 'yield_30d': Decimal('0'),
                'usd_change_7d': Decimal('0'), 'usd_change_30d': Decimal('0'),
                'potential_yearn_staking_apr': potential_yearn_staking_apr if staked_status == "none" else Decimal('0'),
                'potential_1up_staking_apr': potential_1up_staking_apr if staked_status == "none" else Decimal('0'),
                '1up_gauge_address': one_up_gauge_address_lower,

                'effective_balance_hex': effective_balance_hex,
                'decimals': decimals,
                'underlying_token_price': underlying_token_price 

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
                'address': vault_address_lower, 'underlying_token_address': underlying_token_address,
                'apr': vault_apr_percent, 'chainID': chain_id, 'name': name, 'symbol': symbol,
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
            logger.error(f"Error processing vault data for {vault_address_lower} on chain {chain_id}: {e}")
            continue

    logger.info(f"Initial vault processing took: {datetime.now() - processing_start}")
    logger.info(f"Found {sum(len(cd['vaults']) for cd in portfolio_by_chain.values())} total vault positions held by user.")
    logger.info(f"Found {len(staking_opportunities)} potential staking opportunities for current holdings.")
    logger.info(f"Need to fetch/check Kong data for {len(vaults_requiring_kong)} unique vaults.")

    kong_fetch_start = datetime.now()
    kong_results = {}
    kong_tasks = [get_kong_data(address, cid) for cid, address in vaults_requiring_kong]
    if kong_tasks:
        results = await asyncio.gather(*kong_tasks)
        for i, (cid, address) in enumerate(vaults_requiring_kong):
            if results[i]: kong_results[(cid, address.lower())] = results[i]
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
        report_lines_by_chain[chain_id] = []
        chain_name = CHAIN_NAMES.get(chain_id, f"Chain {chain_id}")
        report_lines_by_chain[chain_id].append(f"--- <b>{chain_name}</b> ---")

        for vault_info in chain_data['vaults']:
            vault_address_lower = vault_info['address'].lower()
            timeseries_data = kong_results.get((chain_id, vault_address_lower))

            if timeseries_data:
                try:
                    current_pps_kong, pps_7d_ago, pps_30d_ago = process_timeseries_data_with_decimal(timeseries_data)

                    yield_7d = calculate_yield_with_decimal(current_pps_kong, pps_7d_ago)
                    yield_30d = calculate_yield_with_decimal(current_pps_kong, pps_30d_ago)

                    retrieved_effective_balance_hex = vault_info.get('effective_balance_hex', '0x0')
                    retrieved_decimals = vault_info.get('decimals', 18) 
                    retrieved_underlying_price = vault_info.get('underlying_token_price', Decimal('0'))

                    effective_balance_tokens = Decimal(int(retrieved_effective_balance_hex, 16)) / (Decimal(10)**retrieved_decimals)

                    pps_change_7d = current_pps_kong - pps_7d_ago
                    pps_change_30d = current_pps_kong - pps_30d_ago

                    if retrieved_underlying_price > 0:
                        usd_change_7d = (effective_balance_tokens * pps_change_7d) * retrieved_underlying_price
                        usd_change_30d = (effective_balance_tokens * pps_change_30d) * retrieved_underlying_price
                    else:

                        logger.warning(f"Underlying price is zero or missing for {vault_info['display_name']}, setting USD change to 0.")
                        usd_change_7d = Decimal('0')
                        usd_change_30d = Decimal('0')

                    logger.info(f"[{vault_info['display_name']}] USD Change Calc:")
                    logger.info(f"  PPS Curr: {current_pps_kong}, 7d: {pps_7d_ago}, 30d: {pps_30d_ago}")
                    logger.info(f"  PPS Change 7d: {pps_change_7d}, 30d: {pps_change_30d}")
                    logger.info(f"  Balance Hex: {retrieved_effective_balance_hex}, Decimals: {retrieved_decimals}")
                    logger.info(f"  Effective Tokens: {effective_balance_tokens}")
                    logger.info(f"  Underlying Price: {retrieved_underlying_price}")
                    logger.info(f"  USD Change 7d: {usd_change_7d}, 30d: {usd_change_30d}")

                    vault_info['yield_7d'] = yield_7d
                    vault_info['yield_30d'] = yield_30d
                    vault_info['usd_change_7d'] = usd_change_7d
                    vault_info['usd_change_30d'] = usd_change_30d

                    chain_data['weighted_yield7d'] += yield_7d * vault_info['vault_usd_value']
                    chain_data['weighted_yield30d'] += yield_30d * vault_info['vault_usd_value']
                    chain_data['total_usd_change7d'] += usd_change_7d
                    chain_data['total_usd_change30d'] += usd_change_30d

                except Exception as e:
                    logger.error(f"Error calculating yield for {vault_info['name']} ({vault_info['address']}): {e}", exc_info=True)

                    vault_info['yield_7d'] = Decimal('0'); vault_info['yield_30d'] = Decimal('0')
                    vault_info['usd_change_7d'] = Decimal('0'); vault_info['usd_change_30d'] = Decimal('0')
            else:
                 logger.warning(f"No Kong timeseries data found for {vault_info['name']} ({vault_info['address']}) on chain {chain_id}.")

                 vault_info['yield_7d'] = Decimal('0'); vault_info['yield_30d'] = Decimal('0')
                 vault_info['usd_change_7d'] = Decimal('0'); vault_info['usd_change_30d'] = Decimal('0')

            staking_line = ""

            if vault_info['staked_status'] != "none" and vault_info['current_staking_apr_percent'] > 0:
                staking_line = f"\n  Staking APR: {vault_info['current_staking_apr_percent']:.2f}% <i>({vault_info['current_staking_apr_source']})</i>"

            report_lines_by_chain[chain_id].append(
                f"<b><a href='{vault_info['vault_url']}'>{vault_info['display_name']} ({vault_info['token_symbol']})</a></b>{vault_info['staked_indicator']}\n"
                f"  Value: ${vault_info['vault_usd_value']:,.2f}\n"
                f"  Vault APY: {vault_info['vault_apr_percent']:.2f}%"
                f"{staking_line}"
                f"\n  Yield: {vault_info['yield_7d']:.2f}% [7d] (${vault_info['usd_change_7d']:,.2f}), {vault_info['yield_30d']:.2f}% [30d] (${vault_info['usd_change_30d']:,.2f})"
            )

        chain_total_usd = chain_data['total_usd']
        if chain_total_usd > 0:
            avg_apr = chain_data['weighted_apr'] / chain_total_usd
            avg_yield7d = chain_data['weighted_yield7d'] / chain_total_usd
            avg_yield30d = chain_data['weighted_yield30d'] / chain_total_usd
            report_lines_by_chain[chain_id].append(
                f"  ---\n"
                f"  <b>Chain Total: ${chain_total_usd:,.2f}</b>\n"
                f"  Avg Vault APY: {avg_apr:.2f}%\n"
                f"  Avg Yield: {avg_yield7d:.2f}% [7d] (${chain_data['total_usd_change7d']:,.2f}), {avg_yield30d:.2f}% [30d] (${chain_data['total_usd_change30d']:,.2f})"
            )
            grand_total_usd += chain_total_usd
            grand_total_weighted_apr += chain_data['weighted_apr']
            grand_total_weighted_yield7d += chain_data['weighted_yield7d']
            grand_total_weighted_yield30d += chain_data['weighted_yield30d']
            grand_total_usd_change7d += chain_data['total_usd_change7d']
            grand_total_usd_change30d += chain_data['total_usd_change30d']
        else:
             report_lines_by_chain[chain_id].append("  <i>No holdings found on this chain.</i>")

    logger.info(f"Yield calculation and report line formatting took: {datetime.now() - yield_calc_start}")

    final_report_lines = []
    for chain_id in sorted(report_lines_by_chain.keys()):
        final_report_lines.extend(report_lines_by_chain[chain_id])
        final_report_lines.append("")
    final_report_lines.append("--- <b>Overall Portfolio</b> ---")
    if grand_total_usd > 0:
        overall_avg_apr = grand_total_weighted_apr / grand_total_usd
        overall_avg_yield7d = grand_total_weighted_yield7d / grand_total_usd
        overall_avg_yield30d = grand_total_weighted_yield30d / grand_total_usd
        final_report_lines.append(f"💰 <b>Total Value: ${grand_total_usd:,.2f}</b>")
        final_report_lines.append(f"📊 Avg Vault APY: {overall_avg_apr:.2f}%")
        final_report_lines.append(f"📈 Avg Yield: {overall_avg_yield7d:.2f}% [7d] (${grand_total_usd_change7d:,.2f}), {overall_avg_yield30d:.2f}% [30d] (${grand_total_usd_change30d:,.2f})")
    else:
        final_report_lines.append("<i>No Yearn vault holdings found for the provided addresses.</i>")

    ydaemon_ts = api_cache['ydaemon'].get('timestamp', 0)
    kong_ts = api_cache['kong'].get('timestamp', 0)
    oneup_ts = api_cache['1up'].get('timestamp', 0)
    last_update_ts = max(ydaemon_ts, kong_ts, oneup_ts)
    if last_update_ts > 0:
        last_update_dt = datetime.fromtimestamp(last_update_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M %Z') 
        cache_note = f"\n\n<i>Data cached approx. every {CACHE_EXPIRY_SECONDS / 3600:.0f} hours. Last update: {last_update_dt}. Staking APRs shown may be max rates.</i>"
        final_report_lines.append(cache_note)
    else:
         final_report_lines.append("\n\n<i>Data freshness may vary. Staking APRs shown may be max rates.</i>")

    report_string = "\n".join(final_report_lines)

    suggestions_string = await generate_vault_suggestions_content(
        user_id, report_vaults_details, all_vaults_data, kong_results, one_up_apr_data, one_up_gauge_map
    )

    return report_string, suggestions_string, staking_opportunities



def format_tvl(tvl_decimal):
    """Formats TVL into a readable string like $1.2M or $75.5K."""
    if tvl_decimal >= 1_000_000:
        return f"${tvl_decimal / 1_000_000:.2f}M"
    elif tvl_decimal >= 1_000:
        return f"${tvl_decimal / 1_000:.1f}K"
    else:
        return f"${tvl_decimal:.0f}" 



async def generate_vault_suggestions_content(user_id, user_vaults_details, all_vaults_data, kong_results, one_up_apr_data, one_up_gauge_map):
    """Generates vault suggestions based on user's current holdings, comparing same underlying assets and filtering by TVL."""
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

                APR_THRESHOLD = Decimal('5.0')
                if apr_difference > APR_THRESHOLD:
                    suggestion_key = (chain_id, potential_vault_address)
                    if suggestion_key in suggested_vaults_set:
                        continue

                    suggested_staking_apr_line = ""

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
                             suggested_staking_apr_line += f"\n    + Yearn Staking: {yrn_stake_apr:.2f}% <i>(Max Boost)</i>"

                    one_up_gauge_addr = one_up_vault_to_gauge_map.get(potential_vault_address)
                    if one_up_gauge_addr and one_up_apr_data:
                         gauge_data = one_up_apr_data.get('gauges', {}).get(one_up_gauge_addr)
                         if gauge_data:
                             one_up_stake_apr = Decimal(gauge_data.get('reward_apr', 0))
                             if one_up_stake_apr > 0:
                                 suggested_staking_apr_line += f"\n    + 1UP Staking: {one_up_stake_apr:.2f}%"

                    vault_display_name = potential_vault_data.get('display_name') or potential_vault_data.get('name') or "Vault"
                    token_display_name = potential_vault_data.get('token', {}).get('display_name') or potential_vault_data.get('token', {}).get('symbol') or "Asset"
                    vault_url = f"https://yearn.fi/v3/{chain_id}/{potential_vault_data['address']}"
                    formatted_tvl = format_tvl(tvl_usd) 

                    suggestion_text = (
                        f"<b><a href='{vault_url}'>{vault_display_name} ({token_display_name})</a></b>\n"
                        f"  Vault APY: {potential_base_apr:.2f}% (+{apr_difference:.2f}%)\n"
                        f"  TVL: {formatted_tvl}" 
                        f"{suggested_staking_apr_line}" 
                    )

                    if chain_id not in suggestions_by_chain: suggestions_by_chain[chain_id] = []
                    suggestions_by_chain[chain_id].append({'text': suggestion_text, 'apr': potential_base_apr})
                    suggested_vaults_set.add(suggestion_key)

        except (KeyError, ValueError, TypeError, InvalidOperation) as e:
            logger.error(f"Error processing suggestion for vault {potential_vault_data.get('address', 'N/A')}: {e}")
            continue

    logger.info(f"Suggestion processing took: {datetime.now() - suggestion_processing_start}")
    if not suggestions_by_chain: return None

    final_suggestions_lines = ["<b>💡 Vault Suggestions</b>"] 
    for chain_id in sorted(suggestions_by_chain.keys()):
        chain_name = CHAIN_NAMES.get(chain_id, f"Chain {chain_id}")
        final_suggestions_lines.append(f"\n--- <i>{chain_name}</i> ---")
        sorted_suggestions = sorted(suggestions_by_chain[chain_id], key=lambda x: x['apr'], reverse=True)
        for suggestion in sorted_suggestions:
            final_suggestions_lines.append(suggestion['text'])

    return "\n".join(final_suggestions_lines)

async def generate_staking_opportunities_content(staking_opportunities_list):
    """Formats the staking opportunities suggestions."""
    if not staking_opportunities_list:
        return None

    logger.info(f"Formatting {len(staking_opportunities_list)} staking opportunities.")
    lines_by_chain = {}

    for opp in staking_opportunities_list:
        chain_id = opp['chain_id']
        if chain_id not in lines_by_chain: lines_by_chain[chain_id] = []

        entry_lines = []
        entry_lines.append(f"<b>{opp['display_name']} ({opp['token_symbol']})</b>")

        if opp['yearn_apr'] > 0:
            entry_lines.append(f"  <a href='{opp['yearn_url']}'>veYFI</a>: {opp['yearn_apr']:.2f}% APR <i>(Max Boost)</i>")

        if opp['1up_apr'] > 0 and opp['1up_gauge_address']:
            try:

                checksum_gauge_address = Web3.to_checksum_address(opp['1up_gauge_address'])

                chain_name_1up = "ethereum" 
                one_up_url = f"https://1up.tokyo/stake/{chain_name_1up}/{checksum_gauge_address}" 
                entry_lines.append(f"  <a href='{one_up_url}'>1UP</a>: {opp['1up_apr']:.2f}% APR")
            except ValueError: 
                 logger.warning(f"Could not checksum 1UP gauge address: {opp['1up_gauge_address']}")
                 entry_lines.append(f"  1UP: {opp['1up_apr']:.2f}% APR (URL Invalid Addr)")
            except Exception as e: 
                 logger.error(f"Error creating 1UP URL for {opp['1up_gauge_address']}: {e}")
                 entry_lines.append(f"  1UP: {opp['1up_apr']:.2f}% APR (URL Error)")

        elif opp['1up_apr'] > 0:
             entry_lines.append(f"  1UP: {opp['1up_apr']:.2f}% APR (URL unavailable - Missing Addr)")

        lines_by_chain[chain_id].append("\n".join(entry_lines))

    final_lines = ["<b>⚡ Staking Opportunities (for your current holdings)</b>"]
    for chain_id in sorted(lines_by_chain.keys()):
        chain_name = CHAIN_NAMES.get(chain_id, f"Chain {chain_id}")
        final_lines.append(f"\n--- <i>{chain_name}</i> ---")
        final_lines.extend(lines_by_chain[chain_id])

    return "\n".join(final_lines)



async def start(update: Update, context: CallbackContext):
    """Handles the /start command."""
    user_id = str(update.effective_chat.id)
    logger.info(f"Received /start command from user {user_id}")

    user_states[user_id] = 'awaiting_eoas'
    user_data[user_id] = {'eoas': []}

    if user_id in daily_report_schedules:
        remove_daily_job(user_id, context.job_queue)
        del daily_report_schedules[user_id]
        logger.info(f"Cleared daily report schedule for user {user_id} due to /start.")

    keyboard = get_main_keyboard() 
    await update.message.reply_text(
        "👋 Welcome!\n\n"
        "I can track your Yearn Finance vault positions.\n\n"
        "➡️ **Please send your Ethereum wallet address(es) or ENS names** (separated by spaces).\n\n"
        "Once saved, use the buttons below:",
        reply_markup=keyboard
    )

def get_main_keyboard():
    """Returns the standard InlineKeyboardMarkup for main actions."""
    keyboard = [
        [InlineKeyboardButton("📊 Generate Report (yPort)", callback_data=CALLBACK_YPORT)],
        [
            InlineKeyboardButton("🔔 Start Daily Reports", callback_data=CALLBACK_START_DAILY),
            InlineKeyboardButton("🔕 Stop Daily Reports", callback_data=CALLBACK_STOP_DAILY),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)

async def handle_message(update: Update, context: CallbackContext):
    """Handles text messages: processes addresses/ENS, replacing any existing ones."""
    user_id = str(update.effective_chat.id)
    text = update.message.text.strip()
    original_input_text = update.message.text

    logger.info(f"Received potential addresses/ENS from user {user_id} (will replace existing): {text}")
    eoas_input = text.split()
    checksummed_eoas = []
    valid_input_found = False
    error_messages = []

    if user_id not in user_data:
        user_data[user_id] = {'eoas': []}
    user_states[user_id] = 'awaiting_eoas' 

    ens_needed = any(".eth" in eoa.lower() or ".xyz" in eoa.lower() for eoa in eoas_input)
    if ens_needed and not ns:
         await update.message.reply_text("⚠️ ENS resolution is currently unavailable. Please use wallet addresses only for now.")

         return

    if not eoas_input:
        valid_input_found = False
    else:
        for eoa in eoas_input:
            is_potential_address = eoa.startswith("0x") and len(eoa) == 42
            is_potential_ens = ".eth" in eoa.lower() or ".xyz" in eoa.lower()

            if is_potential_address or is_potential_ens:
                valid_input_found = True
                resolved_address = None
                try:
                    if ns and is_potential_ens:
                        resolved_address = ns.address(eoa)
                        if resolved_address:
                            checksummed_eoa = Web3.to_checksum_address(resolved_address)
                            checksummed_eoas.append(checksummed_eoa)
                            logger.info(f"Resolved ENS {eoa} to {checksummed_eoa}")
                        else:
                            error_messages.append(f"ENS name '{eoa}' could not be resolved.")
                    elif Web3.is_address(eoa):
                        checksummed_eoa = Web3.to_checksum_address(eoa)
                        checksummed_eoas.append(checksummed_eoa)
                    else:
                         error_messages.append(f"Input '{eoa}' looks like an address/ENS but is invalid or could not be resolved.")
                except Exception as e:
                    logger.error(f"Error processing address/ENS '{eoa}' for user {user_id}: {e}")
                    error_messages.append(f"Error processing '{eoa}'.")
            else:
                pass 

    keyboard = get_main_keyboard()

    if checksummed_eoas: 
        unique_eoas = list(set(checksummed_eoas))

        user_data[user_id]['eoas'] = unique_eoas
        user_states[user_id] = 'ready' 

        logger.info(f"Saved/Replaced valid EOAs for user {user_id}: {unique_eoas}")

        success_message = f"✅ Addresses saved ({len(unique_eoas)} unique)."
        if error_messages:
            success_message += "\n\n⚠️ Some inputs could not be processed:\n- " + "\n- ".join(error_messages)

        await update.message.reply_text(success_message + "\n\nUse the buttons below:", reply_markup=keyboard)

    elif not checksummed_eoas and valid_input_found:

        await update.message.reply_text(
             "❌ Couldn't validate the new address(es)/ENS name(s) you sent. Your previously saved addresses (if any) remain unchanged. Please check the errors and try again:\n- " + "\n- ".join(error_messages),
             reply_markup=keyboard
         )

    elif not checksummed_eoas and not valid_input_found:

        input_preview = original_input_text[:50]
        if len(original_input_text) > 50: input_preview += "..."

        existing_addresses = user_data.get(user_id, {}).get('eoas', [])
        if existing_addresses:
             help_message = (
                 f"ℹ️  Let's try that again.\n\n"
                 f"Your currently saved addresses ({len(existing_addresses)}) are still active. "
                 "To replace them, just send the new address(es)/ENS name(s)."
             )
        else:
             help_message = (
                f"👋 Hi there! Let's try that again.\n\n"
                "➡️ **To get started, please send your wallet address(es) or ENS names** (like `0x...` or `yourname.eth`), separated by spaces."
             )

        await update.message.reply_text(help_message + "\n\nUse the buttons below:", reply_markup=keyboard)

async def button_handler(update: Update, context: CallbackContext):
    """Handles inline button presses."""
    query = update.callback_query
    await query.answer() 

    if query.message:
        user_id = str(query.message.chat_id)
    else:
        logger.warning("CallbackQuery received without an associated message.")
        return

    action = query.data
    logger.info(f"Received button action '{action}' from user {user_id}")

    if action in [CALLBACK_YPORT, CALLBACK_START_DAILY]:
        if user_id not in user_data or not user_data.get(user_id, {}).get('eoas'):

            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ Please send your address(es) or ENS name(s) first. You can just type them in the chat."
            )

            return

    if action == CALLBACK_YPORT:
        await yport_action(query, context)
    elif action == CALLBACK_START_DAILY:
        await start_daily_action(query, context)
    elif action == CALLBACK_STOP_DAILY:
        await stop_daily_action(query, context)
    else:
        logger.warning(f"Unhandled callback query data: {action}")

        await context.bot.send_message(chat_id=user_id, text="Unknown action.")

async def yport_action(query: CallbackQuery, context: CallbackContext):
    """Handles the 'Generate Report' button action."""
    if query.message:
        user_id = str(query.message.chat_id)
    else:
        logger.warning("yport_action received CallbackQuery without an associated message.")
        return

    global on_demand_report_count 

    if report_locks.get(user_id, False):
        await context.bot.send_message(chat_id=user_id, text="⏳ A report is already being generated. Please wait.", disable_web_page_preview=True)
        return

    report_locks[user_id] = True
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text="🔄 Generating your Yearn portfolio report...\n\nThis might take a minute...",
            disable_web_page_preview=True
        )

        generation_start_time = datetime.now()

        report_content, suggestions_content, staking_opportunities_list = await generate_report_content(user_id)

        staking_opportunities_content = await generate_staking_opportunities_content(staking_opportunities_list)

        generation_time = datetime.now() - generation_start_time
        logger.info(f"Report generation for user {user_id} took {generation_time}")

        on_demand_report_count += 1
        logger.info(f"On-demand report count incremented to: {on_demand_report_count}")

        keyboard = get_main_keyboard()
        last_message_markup = None 

        truncated_report = truncate_html_message(report_content)

        if not suggestions_content and not staking_opportunities_content:
            last_message_markup = keyboard
        await context.bot.send_message(
            chat_id=user_id, text=truncated_report, parse_mode='HTML',
            disable_web_page_preview=True, reply_markup=last_message_markup
        )

        if suggestions_content:
            truncated_suggestions = truncate_html_message(suggestions_content)

            if not staking_opportunities_content:
                last_message_markup = keyboard
            else:
                last_message_markup = None 
            await context.bot.send_message(
                chat_id=user_id, text=truncated_suggestions, parse_mode='HTML',
                disable_web_page_preview=True, reply_markup=last_message_markup
            )

        if staking_opportunities_content:
            truncated_staking_opps = truncate_html_message(staking_opportunities_content)

            last_message_markup = keyboard
            await context.bot.send_message(
                chat_id=user_id, text=truncated_staking_opps, parse_mode='HTML',
                disable_web_page_preview=True, reply_markup=last_message_markup
            )

    except Exception as e:
        logger.exception(f"Error during yport_action for user {user_id}: {e}")
        await context.bot.send_message(chat_id=user_id, text="❌ An error occurred while generating your report. Please try again later.", disable_web_page_preview=True)
    finally:
        report_locks[user_id] = False



async def daily_send_report_for_user(context: CallbackContext):
    """Scheduled job function to send a report to a specific user."""
    job = context.job
    user_id = job.data['user_id']
    logger.info(f"Running daily report job for user {user_id}")

    global automated_daily_report_count 

    if user_id not in user_data or not user_data.get(user_id, {}).get('eoas'):
        logger.warning(f"Skipping daily report for {user_id}: No user data or EOAs found.")

        return

    try:

        report_content, suggestions_content, staking_opportunities_list = await generate_report_content(user_id)
        staking_opportunities_content = await generate_staking_opportunities_content(staking_opportunities_list)

        await context.bot.send_message(chat_id=user_id, text="--- Your Daily Yearn Report ---", parse_mode='HTML', disable_web_page_preview=True)

        truncated_report = truncate_html_message(report_content)
        await context.bot.send_message(chat_id=user_id, text=truncated_report, parse_mode='HTML', disable_web_page_preview=True)

        if suggestions_content:
            truncated_suggestions = truncate_html_message(suggestions_content)
            await context.bot.send_message(chat_id=user_id, text=truncated_suggestions, parse_mode='HTML', disable_web_page_preview=True)

        if staking_opportunities_content:
             truncated_staking_opps = truncate_html_message(staking_opportunities_content)
             await context.bot.send_message(chat_id=user_id, text=truncated_staking_opps, parse_mode='HTML', disable_web_page_preview=True)

        automated_daily_report_count += 1
        logger.info(f"Automated daily report count incremented to: {automated_daily_report_count}")

    except Exception as e:
        logger.error(f"Error sending daily report to user {user_id}: {e}")
        try:

            await context.bot.send_message(chat_id=user_id, text="❌ Failed to generate your daily report today. Please try generating manually later.")
        except Exception as notify_e:
            logger.error(f"Failed to notify user {user_id} about daily report failure: {notify_e}")

async def daily_usage_report(context: CallbackContext):
    """Sends the daily usage counts to the admin and resets them."""
    global on_demand_report_count, automated_daily_report_count

    logger.info("Preparing daily usage report.")

    if on_demand_report_count > 0 or automated_daily_report_count > 0:
        message = (
            f"📊 Daily Bot Usage Report:\n\n"
            f"🔹 On-Demand Reports Generated: {on_demand_report_count}\n"
            f"🔹 Automated Daily Reports Sent: {automated_daily_report_count}"
        )
        try:
            await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=message)
            logger.info(f"Sent daily usage report to admin {ADMIN_CHAT_ID}.")
        except Exception as e:
            logger.error(f"Failed to send daily usage report to admin {ADMIN_CHAT_ID}: {e}")
    else:
        logger.info("Skipping daily usage report as counts are zero.")

    on_demand_report_count = 0
    automated_daily_report_count = 0
    logger.info("Usage counters reset.")



def remove_daily_job(user_id: str, job_queue: JobQueue):
    """Removes the daily report job for a user."""
    job_name = f"daily_report_{user_id}"
    current_jobs = job_queue.get_jobs_by_name(job_name)
    if not current_jobs:
        logger.info(f"No daily report job found for user {user_id} to remove.")
        return False
    for job in current_jobs:
        job.schedule_removal()
        logger.info(f"Removed daily report job '{job_name}' for user {user_id}")
    return True

async def start_daily_action(query: CallbackQuery, context: CallbackContext):
    """Handles the 'Start Daily Reports' button action."""
    if query.message:
        user_id = str(query.message.chat_id)
    else:
        logger.warning("start_daily_action received CallbackQuery without an associated message.")
        return

    job_queue = context.job_queue
    job_name = f"daily_report_{user_id}"
    message_text = "" 

    if job_queue.get_jobs_by_name(job_name):
        message_text = "✅ Daily reports are already enabled."
    else:
        report_time = time(hour=6, minute=0, second=0, tzinfo=timezone.utc)
        job_queue.run_daily(
            daily_send_report_for_user,
            time=report_time,
            name=job_name,
            data={'user_id': user_id}
        )
        daily_report_schedules[user_id] = True
        logger.info(f"Scheduled daily report job '{job_name}' for user {user_id} at {report_time}.")
        message_text = f"✅ Daily reports enabled! You'll receive a report around {report_time.strftime('%H:%M %Z')} each day."

    await context.bot.send_message(chat_id=user_id, text=message_text)

async def stop_daily_action(query: CallbackQuery, context: CallbackContext):
    """Handles the 'Stop Daily Reports' button action."""
    if query.message:
        user_id = str(query.message.chat_id)
    else:
        logger.warning("stop_daily_action received CallbackQuery without an associated message.")
        return

    job_queue = context.job_queue
    message_text = ""

    if remove_daily_job(user_id, job_queue):
        if user_id in daily_report_schedules:
             del daily_report_schedules[user_id]
        message_text = "✅ Daily reports have been stopped."
    else:
        message_text = "ℹ️ Daily reports were not active."

    await context.bot.send_message(chat_id=user_id, text=message_text)



def main():
    """Starts the bot."""

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    job_queue = app.job_queue

    job_queue.run_once(scheduled_cache_update, when=timedelta(seconds=1), name="initial_cache_update")
    logger.info("Scheduled initial cache update job.")

    cache_update_interval_seconds = CACHE_EXPIRY_SECONDS
    job_queue.run_repeating(scheduled_cache_update, interval=cache_update_interval_seconds, first=cache_update_interval_seconds + 60) 
    logger.info(f"Scheduled periodic cache update every {cache_update_interval_seconds / 3600:.1f} hours.")

    usage_report_time = time(hour=0, minute=0, second=0, tzinfo=timezone.utc)
    job_queue.run_daily(daily_usage_report, time=usage_report_time, name="daily_usage_report")
    logger.info(f"Scheduled daily usage report job for {usage_report_time.strftime('%H:%M %Z')}.")

    app.add_handler(CommandHandler('start', start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_handler)) 

    logger.info("Starting bot polling...")
    app.run_polling() 
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()
