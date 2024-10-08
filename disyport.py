import discord
import asyncio
import aiohttp
import requests
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, time
from discord.ext import commands, tasks
from discord import app_commands, Embed
from web3 import Web3
from ens import ENS
import sys

sys.stdout = sys.stderr

BOT_TOKEN = 'BOT_TOKEN_HERE'
API_KEY = "RPC_API_KEY_HERE"

PUBLIC_CHANNEL_ID = SPECIFY_PUBLIC_YPORT_CHANNEL
LOG_CHANNEL_ID = SPECIFY_CHANNEL_FOR_LOGS
ADMIN_USER_ID = ADMIN_USER_ID
RATE_LIMIT_SECONDS = 10
last_report_message_id = None
daily_usage_count = 0

getcontext().prec = 28

CHAIN_ID_TO_NAME = {
    1: "Ethereum",
    10: "Optimism",
    137: "Polygon",
    42161: "Arbitrum",
    8453: "Base"
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
        'usdc': '0xaf88d065e77c8cC2239327C5EDb3A432268e5831',  # Native USDC
        'usdc_e': '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8',  # Bridged USDC.e
        'usdt': '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9',
        'wbtc': '0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f',
    },
    'polygon': {
        'weth': '0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619',
        'dai': '0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063',
        'usdc': '0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359',  # Native USDC
        'usdc_e': '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',  # Bridged USDC.e
        'usdt': '0xc2132D05D31c914a87C6611C10748AEb04B58e8F',
        'wbtc': '0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6',
    },
    'base': {
        'weth': '0x4200000000000000000000000000000000000006',
        'dai': '0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb',
        'usdc': '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
        # No USDT or WBTC on Base
    },
    'optimism': {
        'weth': '0x4200000000000000000000000000000000000006',
        'dai': '0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1',
        'usdc': '0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85',  # Native USDC
        'usdc_e': '0x7F5c764cBc14f9669B88837ca1490cCa17c31607',  # Bridged USDC.e
        'usdt': '0x94b008aA00579c1307B0EF2c499aD98a8ce58e58',
        'wbtc': '0x68f180fcce6836688e9084f035309e29bf0a2095',
    }
}

CHAIN_TO_ALCHEMY_PREFIX = {
    'ethereum': 'eth-mainnet',
    'base': 'base-mainnet',
    'optimism': 'opt-mainnet',
    'polygon': 'polygon-mainnet',
    'arbitrum': 'arb-mainnet'
}

intents = discord.Intents.default()
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix="/", intents=intents)

user_data = {}
last_report_time = None
report_lock = asyncio.Lock()

async def fetch_historical_pricepershare_kong(vault_address, chain_id, limit=1000):
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
                        return data["data"]["timeseries"]
                    else:
                        print(f"Error: Unexpected response format: {data}")
                        return None
                else:
                    print(f"Error fetching data from Kong: {response.status}")
                    return None
    except Exception as e:
        print(f"Exception occurred while fetching historical data from Kong: {str(e)}")
        return None

def process_timeseries_data_with_decimal(timeseries):
    reversed_timeseries = timeseries[::-1]

    now = datetime.utcnow()
    timestamp_7d_ago = int((now - timedelta(days=7)).timestamp())
    timestamp_30d_ago = int((now - timedelta(days=30)).timestamp())

    def find_closest_entry(timestamp):
        return min(reversed_timeseries, key=lambda x: abs(int(x['time']) - timestamp))

    current_pps = Decimal(reversed_timeseries[0]["value"])
    pps_7d_ago = Decimal(find_closest_entry(timestamp_7d_ago)["value"])
    pps_30d_ago = Decimal(find_closest_entry(timestamp_30d_ago)["value"])

    return current_pps, pps_7d_ago, pps_30d_ago

def calculate_yield_with_decimal(current_pps, historical_pps):
    current_pps = Decimal(str(current_pps))
    historical_pps = Decimal(str(historical_pps))
    return ((current_pps / historical_pps) - Decimal('1')) * Decimal('100')

def get_alchemy_url(chain):
    network = CHAIN_TO_ALCHEMY_PREFIX.get(chain)
    if network:
        return f"https://{network}.g.alchemy.com/v2/{API_KEY}"
    return None

def get_token_balances(eoa, chain):
    url = get_alchemy_url(chain)
    
    if not url:
        print(f"Alchemy URL not found for chain: {chain}")
        return {}

    payload = {
        "jsonrpc": "2.0",
        "method": "alchemy_getTokenBalances",
        "params": [eoa, "erc20"],
        "id": 1
    }

    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()

        if 'result' in data:
            return {item['contractAddress'].lower(): item['tokenBalance'] for item in data['result']['tokenBalances'] if item['tokenBalance'] != '0x0'}
        else:
            return {}

    except Exception as e:
        print(f"Error fetching token balances for {eoa} on {chain}: {e}")
        return {}

async def fetch_yearn_vault_data():
    url = "https://ydaemon.yearn.fi/vaults/detected?limit=1000"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Error: Received status code {response.status} from Yearn API.")
                    return []
    except Exception as e:
        print(f"Error fetching Yearn vault data: {e}")
        return []

@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f'{bot.user} has connected to Discord!')
    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if log_channel:
        await log_channel.send(f'{bot.user} is now online and ready!')

    daily_top_vaults_report.start()
    send_daily_usage_report.start()

@bot.tree.command(name="yport", description="Generate your Yearn Vaults report")
async def yport(interaction: discord.Interaction):
    global last_report_time, daily_usage_count

    if interaction.channel_id != PUBLIC_CHANNEL_ID:
        await interaction.response.send_message("⚠️ This command can only be used in the specified public channel.", ephemeral=True)
        return

    async with report_lock:
        now = datetime.utcnow()
        if last_report_time and now - last_report_time < timedelta(seconds=RATE_LIMIT_SECONDS):
            await interaction.response.send_message("⚠️ Please wait a few seconds before requesting another report.", ephemeral=True)
            return
        last_report_time = now

    await interaction.response.defer(ephemeral=True)

    try:
        user_id = interaction.user.id
        
        await generate_user_report(user_id, interaction)
        
        daily_usage_count += 1

    except Exception as e:
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            await log_channel.send(f'Error while generating report for user {interaction.user}: {e}')
        await interaction.followup.send("🚨 An error occurred while generating your report. Please try again later.", ephemeral=True)

@tasks.loop(time=time(hour=0, minute=0))
async def send_daily_usage_report():
    global daily_usage_count

    log_channel = bot.get_channel(LOG_CHANNEL_ID)
    if log_channel:
        await log_channel.send(f"Uses today: {daily_usage_count}")

    daily_usage_count = 0

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    if message.guild is None:
        inputs = message.content.split()

        valid_addresses = []
        invalid_inputs = []

        for item in inputs:
            try:
                if Web3.is_address(item):
                    valid_addresses.append(Web3.to_checksum_address(item))
                else:
                    ens_address = await resolve_ens(item)
                    if ens_address:
                        valid_addresses.append(ens_address)
                    else:
                        invalid_inputs.append(item)
            except Exception as e:
                print(f"Error processing input '{item}': {e}")
                invalid_inputs.append(item)

        try:
            if valid_addresses:
                user_id = str(message.author.id)
                user_data[user_id] = {'eoas': valid_addresses, 'vaults': []}
                await message.channel.send("✅ Addresses received.\nYou can now use the `/yport` command in https://discord.com/channels/734804446353031319/1279431421760507976.\n\nIf you want to start over, just send another batch of addresses in this DM at any time.")
            if invalid_inputs:
                await message.channel.send(f"⚠️ Invalid input detected: {', '.join(invalid_inputs)}")
        except Exception as e:
            print(f"Error saving data or sending message: {e}")
            await message.channel.send("🚨 An error occurred while processing your request. Please try again later.")

    else:
        try:
            if message.channel.id == PUBLIC_CHANNEL_ID:
                if message.author.id not in [ADMIN_USER_ID, bot.user.id]:
                    await message.delete()
        except Exception as e:
            print(f"Error processing message in public channel: {e}")

    await bot.process_commands(message)

async def resolve_ens(ens_name):
    try:
        w3 = Web3(Web3.HTTPProvider(f'https://eth-mainnet.g.alchemy.com/v2/{API_KEY}'))

        ns = ENS.from_web3(w3)
        address = ns.address(ens_name)
        if address:
            return address
        else:
            return None
    except Exception as e:
        print(f"ENS resolution failed for {ens_name}: {e}")
        return None

def is_valid_address(address):
    return Web3.is_address(address) and Web3.is_checksum_address(Web3.to_checksum_address(address))

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("Command not found!", delete_after=5)
    elif isinstance(error, commands.MissingPermissions):
        await ctx.send("You don't have permission to do that!", delete_after=5)
    elif isinstance(error, commands.CheckFailure):
        await ctx.send("You can't do that!", delete_after=5)
    else:
        await ctx.send("An error occurred. Please try again later.", delete_after=5)
        log_channel = bot.get_channel(LOG_CHANNEL_ID)
        if log_channel:
            await log_channel.send(f'Error: {str(error)}')

async def generate_user_report(user_id, interaction):
    try:
        user_specific_data = user_data.get(str(user_id))

        if not user_specific_data:
            await interaction.followup.send("⚠️ No data found. Please start by sending me your EOAs in a DM.", ephemeral=True)
            return

        eoas = user_data.get(str(user_id), {}).get('eoas', [])
        report_lines = []
        total_usd_value = Decimal('0')
        total_apr = Decimal('0')
        total_balance_usd = Decimal('0')

        total_yield_7d = Decimal('0')
        total_yield_30d = Decimal('0')
        total_usd_change_7d = Decimal('0')
        total_usd_change_30d = Decimal('0')

        vaults_data = await fetch_yearn_vault_data()
        if not vaults_data:
            await interaction.followup.send("🚨 Unable to retrieve data at the moment. Please try again later.", ephemeral=True)
            return

        token_balances = {}
        for eoa in eoas:
            for chain in ['ethereum', 'base', 'optimism', 'polygon', 'arbitrum']:
                balances = get_token_balances(eoa, chain)
                if not balances:
                    await interaction.followup.send("🚨 Unable to retrieve data at the moment. Please try again later.", ephemeral=True)
                    return
                token_balances.update(balances)

        user_vault_tokens = {}
        for vault_data in vaults_data:
            vault_address_lower = vault_data['address'].lower()

            if vault_address_lower in token_balances:
                balance_hex = token_balances[vault_address_lower]
                vault_balance = int(balance_hex, 16)

                if vault_balance == 0:
                    continue

                name = vault_data.get('name', 'Unknown')
                symbol = vault_data.get('symbol', 'Unknown')

                price_per_share = Decimal(vault_data.get('pricePerShare') or 0) / (10 ** (vault_data.get('decimals', 18) or 18))
                underlying_token_price = Decimal(vault_data.get('tvl', {}).get('price') or 0)

                apr_data = vault_data.get('apr', {})
                forward_apr = apr_data.get('forwardAPR', {}).get('netAPR', 0)
                apr = Decimal(forward_apr if forward_apr else apr_data.get('netAPR', 0)) * Decimal('100')

                vault_balance_in_tokens = Decimal(vault_balance) * price_per_share / (10 ** vault_data.get('decimals', 18))
                vault_usd_value = vault_balance_in_tokens * underlying_token_price

                if vault_usd_value < Decimal('0.0001'):
                    continue

                total_usd_value += vault_usd_value
                total_apr += apr * vault_usd_value
                total_balance_usd += vault_usd_value

                underlying_token_address = vault_data['token']['address'].lower()
                chain_id = vault_data.get('chainID')

                user_vault_tokens[vault_address_lower] = {
                    'token_address': underlying_token_address,
                    'apr': apr,
                    'chain_id': chain_id
                }

                timeseries_data = await fetch_historical_pricepershare_kong(vault_data['address'], chain_id)

                if timeseries_data:
                    current_pps, pps_7d_ago, pps_30d_ago = process_timeseries_data_with_decimal(timeseries_data)
                    yield_7d = calculate_yield_with_decimal(current_pps, pps_7d_ago)
                    yield_30d = calculate_yield_with_decimal(current_pps, pps_30d_ago)

                    usd_change_7d = vault_balance_in_tokens * (current_pps - pps_7d_ago)
                    usd_change_30d = vault_balance_in_tokens * (current_pps - pps_30d_ago)
                else:
                    yield_7d, yield_30d = Decimal('0'), Decimal('0')
                    usd_change_7d, usd_change_30d = Decimal('0'), Decimal('0')

                total_yield_7d += yield_7d * vault_usd_value
                total_yield_30d += yield_30d * vault_usd_value
                total_usd_change_7d += usd_change_7d
                total_usd_change_30d += usd_change_30d

                vault_url = f"<https://yearn.fi/v3/{vault_data['chainID']}/{vault_data['address']}>"

                report_lines.append(
                    f"**[{name} ({symbol})]({vault_url})**\n"
                    f"💵 Value: ${vault_usd_value:,.2f}\n"
                    f"📊 APR: {apr:.2f}%\n"
                    f"📈 Est. Yield: {yield_7d:.2f}% (7d / ${usd_change_7d:,.2f}), {yield_30d:.2f}% (30d / ${usd_change_30d:,.2f})\n"
                )

        average_apr = total_apr / total_balance_usd if total_balance_usd > 0 else Decimal('0')
        average_yield_7d = total_yield_7d / total_usd_value if total_usd_value > 0 else Decimal('0')
        average_yield_30d = total_yield_30d / total_usd_value if total_usd_value > 0 else Decimal('0')

        report_lines.append(f"💼 Total Value: ${total_usd_value:,.2f}")
        report_lines.append(f"📊 Avg. APR: {average_apr:.2f}%")
        report_lines.append(f"📈 Avg. Est. Yield: {average_yield_7d:.2f}% (7d / ${total_usd_change_7d:,.2f}), {average_yield_30d:.2f}% (30d / ${total_usd_change_30d:,.2f})")

        report_content = "\n".join(report_lines)

        filtered_vaults = filter_vault_suggestions(user_vault_tokens, vaults_data)

        suggested_vaults_lines = []
        if filtered_vaults:
            for vault_data in filtered_vaults:
                apr_data = vault_data.get('apr', {})
                forward_apr = apr_data.get('forwardAPR', {}).get('netAPR', 0)
                vault_apr = Decimal(forward_apr if forward_apr else apr_data.get('netAPR', 0)) * Decimal('100')

                timeseries_data = await fetch_historical_pricepershare_kong(vault_data['address'], vault_data['chainID'])
                if timeseries_data:
                    current_pps, pps_7d_ago, pps_30d_ago = process_timeseries_data_with_decimal(timeseries_data)
                    yield_7d = calculate_yield_with_decimal(current_pps, pps_7d_ago)
                    yield_30d = calculate_yield_with_decimal(current_pps, pps_30d_ago)
                else:
                    yield_7d, yield_30d = Decimal('0'), Decimal('0')

                vault_url = f"https://yearn.fi/v3/{vault_data['chainID']}/{vault_data['address']}"
                suggested_vaults_lines.append(
                    f"**[{vault_data['name']} ({vault_data['token']['symbol']})](<{vault_url}>)**\n"
                    f"📊 APR: {vault_apr:.2f}%\n"
                    f"📈 Est. Yield: {yield_7d:.2f}% (7d), {yield_30d:.2f}% (30d)\n"
                )

        suggestions_content = "\n".join(suggested_vaults_lines)

        if report_content.strip():
            await interaction.followup.send(report_content, ephemeral=True)

        if suggestions_content.strip():
            await interaction.followup.send(suggestions_content, ephemeral=True)

    except Exception as e:
        print(f"Error while generating report for user {interaction.user}: {e}")
        await interaction.followup.send("🚨 An error occurred while generating your report. Please try again later.", ephemeral=True)

def filter_vault_suggestions(user_vault_tokens, vaults_data):
    suggested_vaults = []
    for vault_data in vaults_data:
        vault_token_address = vault_data['token']['address'].lower()
        vault_apr = Decimal(vault_data['apr'].get('forwardAPR', {}).get('netAPR') or 0) * Decimal('100')
        vault_chain_id = vault_data.get('chainID')

        for user_vault in user_vault_tokens.values():
            if vault_token_address == user_vault['token_address'] and vault_chain_id == user_vault['chain_id']:
                apr_difference = vault_apr - user_vault['apr']
                if apr_difference > Decimal('3.0'):
                    suggested_vaults.append(vault_data)
    return suggested_vaults

def truncate_report(report_lines, limit=1970, footer=""):
    """Truncate report lines and add a footer if provided."""
    while len("\n".join(report_lines)) > limit:
        report_lines.pop()
    
    truncated_content = "\n".join(report_lines)
    if footer:
        truncated_content += f"\n{footer}"
    return truncated_content

def format_tvl(tvl_value):
    try:
        tvl_value = float(tvl_value)
        if tvl_value >= 1e6:
            return f"{tvl_value / 1e6:.2f}M"
        elif tvl_value >= 1e3:
            return f"{tvl_value / 1e3:.2f}K"
        else:
            return f"{tvl_value:.2f}"
    except ValueError:
        return "0.00"

async def fetch_yearn_tvl():
    url = "https://api.llama.fi/tvl/yearn"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    tvl = await response.json()
                    return float(tvl)
                else:
                    print(f"Error fetching Yearn TVL: {response.status}")
                    return 0
    except Exception as e:
        print(f"Exception occurred while fetching Yearn TVL: {str(e)}")
        return 0

@tasks.loop(hours=12)
async def daily_top_vaults_report():
    global last_report_message_id

    while True:
        now = datetime.utcnow()

        next_run_hour = (now.hour // 12 + 1) * 12 % 24
        next_run = now.replace(hour=next_run_hour, minute=0, second=0, microsecond=0)

        if next_run < now:
            next_run += timedelta(days=1)

        time_until_next_run = (next_run - now).total_seconds()

        await asyncio.sleep(time_until_next_run)

        try:
            vaults_data = await fetch_yearn_vault_data()
            if not vaults_data:
                print("🚨 Unable to retrieve data for the top 5 report.")
                continue

            filtered_vaults = [
                vault for vault in vaults_data
                if not (
                    vault['apr'].get('forwardAPR', {}).get('netAPR', 0) == 0 and
                    vault['apr'].get('points', {}).get('weekAgo', 0) == 0
                )
                and vault.get('kind') != 'Legacy' 
                and not vault.get('info', {}).get('isRetired') 
                and vault.get('token', {}).get('address', '').lower() in {
                    SINGLE_ASSET_TOKENS['ethereum']['weth'].lower(),
                    SINGLE_ASSET_TOKENS['ethereum']['dai'].lower(),
                    SINGLE_ASSET_TOKENS['ethereum']['usdc'].lower(),
                    SINGLE_ASSET_TOKENS['ethereum']['usdt'].lower(),
                    SINGLE_ASSET_TOKENS['ethereum']['wbtc'].lower(),
                    SINGLE_ASSET_TOKENS['arbitrum']['weth'].lower(),
                    SINGLE_ASSET_TOKENS['arbitrum']['dai'].lower(),
                    SINGLE_ASSET_TOKENS['arbitrum']['usdc'].lower(),
                    SINGLE_ASSET_TOKENS['arbitrum']['usdc_e'].lower(),
                    SINGLE_ASSET_TOKENS['arbitrum']['usdt'].lower(),
                    SINGLE_ASSET_TOKENS['arbitrum']['wbtc'].lower(),
                    SINGLE_ASSET_TOKENS['polygon']['weth'].lower(),
                    SINGLE_ASSET_TOKENS['polygon']['dai'].lower(),
                    SINGLE_ASSET_TOKENS['polygon']['usdc'].lower(),
                    SINGLE_ASSET_TOKENS['polygon']['usdc_e'].lower(),
                    SINGLE_ASSET_TOKENS['polygon']['usdt'].lower(),
                    SINGLE_ASSET_TOKENS['polygon']['wbtc'].lower(),
                    SINGLE_ASSET_TOKENS['base']['weth'].lower(),
                    SINGLE_ASSET_TOKENS['base']['dai'].lower(),
                    SINGLE_ASSET_TOKENS['base']['usdc'].lower(),
                    SINGLE_ASSET_TOKENS['optimism']['weth'].lower(),
                    SINGLE_ASSET_TOKENS['optimism']['dai'].lower(),
                    SINGLE_ASSET_TOKENS['optimism']['usdc'].lower(),
                    SINGLE_ASSET_TOKENS['optimism']['usdc_e'].lower(),
                    SINGLE_ASSET_TOKENS['optimism']['usdt'].lower(),
                    SINGLE_ASSET_TOKENS['optimism']['wbtc'].lower(),
                }
                and vault.get('tvl', {}).get('tvl', 0) >= 100
            ]

            filtered_vaults.sort(
                key=lambda v: (
                    v['apr'].get('forwardAPR', {}).get('netAPR', 0),
                    v['apr'].get('netAPR', 0),
                    v['apr'].get('points', {}).get('weekAgo', 0),
                    v['apr'].get('points', {}).get('monthAgo', 0)
                ),
                reverse=True
            )

            top_vaults = filtered_vaults[:5]

            async with aiohttp.ClientSession() as session:
                async with session.get("https://api.llama.fi/tvl/yearn") as resp:
                    if resp.status == 200:
                        yearn_tvl = await resp.json()
                        formatted_tvl = f"${yearn_tvl / 1e6:.2f}M"
                    else:
                        formatted_tvl = "Unavailable"

            embed = Embed(
                title="Top 5 Single-Asset Vaults by APR",
                description=f"Total Yearn TVL: {formatted_tvl}",
                color=0x00ff00
            )

            for idx, vault in enumerate(top_vaults, start=1):
                name = vault.get('name', 'Unknown')
                token = vault.get('token', {}).get('symbol', 'Unknown')
                chain_id = vault.get('chainID', 'Unknown')
                chain = CHAIN_ID_TO_NAME.get(chain_id, 'Unknown')
                vault_address = vault.get('address', 'Unknown')
                apr = (
                    vault['apr'].get('forwardAPR', {}).get('netAPR', 0) or
                    vault['apr'].get('netAPR', 0) or
                    vault['apr'].get('points', {}).get('weekAgo', 0) or
                    vault['apr'].get('points', {}).get('monthAgo', 0)
                ) * 100
                tvl = format_tvl(vault.get('tvl', {}).get('tvl', 0))

                vault_url = f"https://yearn.fi/v3/{chain_id}/{vault_address}"

                embed.add_field(name=f"{idx}. {name}", value=f"[{token} / {chain}]({vault_url}) | APR: {apr:.2f}% | TVL: ${tvl}", inline=False)

            embed.set_footer(text="Updated every twelve hours.")

            channel = bot.get_channel(PUBLIC_CHANNEL_ID)
            if channel:
                if last_report_message_id:
                    try:
                        old_message = await channel.fetch_message(last_report_message_id)
                        await old_message.delete()
                    except Exception as e:
                        print(f"Failed to delete previous report message: {e}")

                new_message = await channel.send(embed=embed)
                last_report_message_id = new_message.id

        except Exception as e:
            log_channel = bot.get_channel(LOG_CHANNEL_ID)
            if log_channel:
                await log_channel.send(f"Error generating the daily top 5 vaults report: {e}")

if __name__ == "__main__":
    try:
        bot.run(BOT_TOKEN)
    except Exception as e:
        print(f"An error occurred while running the bot: {e}")
