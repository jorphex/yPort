import os
import requests
import sys
import aiohttp
from datetime import datetime, timedelta, time
from web3 import Web3
from ens import ENS
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
import logging
import asyncio
import re
from decimal import Decimal, getcontext

sys.stdout = sys.stderr

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)
logger = logging.getLogger(__name__)

BOT_TOKEN = 'BOT_TOKEN_HERE'
API_KEY = "RPC_API_KEY_HERE"

getcontext().prec = 28

daily_usage_count = 0
ADMIN_CHAT_ID = XXXXXXXXXX

# Data storage (in-memory)
user_states = {}
user_data = {}
report_locks = {}

CHAIN_TO_ALCHEMY_PREFIX = {
    'ethereum': 'eth-mainnet',
    'base': 'base-mainnet',
    'optimism': 'opt-mainnet',
    'polygon': 'polygon-mainnet',
    'arbitrum': 'arb-mainnet'
}

def get_token_balances(eoa, chain):
    url = f"https://{CHAIN_TO_ALCHEMY_PREFIX.get(chain)}.g.alchemy.com/v2/{API_KEY}"
    
    if not url:
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
        return {}

def fetch_yearn_vault_data():
    try:
        response = requests.get(f"https://ydaemon.yearn.fi/vaults/detected?limit=1000")
        if response.status_code == 200:
            return response.json()
        else:
            return []
    except Exception:
        return []

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
                        print(f"Unexpected response format: {data}")
                        return None
                else:
                    print(f"Error fetching data from Kong: {response.status}")
                    return None
    except Exception as e:
        print(f"Exception while fetching data from Kong: {str(e)}")
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

# Start command handler
async def start(update: Update, context: CallbackContext):
    user_id = str(update.effective_chat.id)
    user_states[user_id] = 'awaiting_eoas'
    user_data[user_id] = {'eoas': []}
    await update.message.reply_text("👋 Welcome! Please send addresses or ENS names separated by spaces.")
async def handle_message(update: Update, context: CallbackContext):
    user_id = str(update.effective_chat.id)
    text = update.message.text.strip()

    if user_id not in user_states:
        await update.message.reply_text("⚠️ Please start by sending /start")
        return

    state = user_states[user_id]

    if state == 'awaiting_eoas':
        eoas = text.split()
        checksummed_eoas = []

        web3 = Web3(Web3.HTTPProvider(f"https://eth-mainnet.g.alchemy.com/v2/{API_KEY}"))
        ns = ENS.from_web3(web3)

        for eoa in eoas:
            try:
                if ".eth" in eoa.lower():
                    resolved_address = ns.address(eoa)
                    if resolved_address:
                        checksummed_eoa = Web3.to_checksum_address(resolved_address)
                        checksummed_eoas.append(checksummed_eoa)
                    else:
                        await update.message.reply_text(f"⚠️ ENS name {eoa} could not be resolved.")
                        return
                else:
                    checksummed_eoa = Web3.to_checksum_address(eoa)
                    checksummed_eoas.append(checksummed_eoa)
            except Exception:
                await update.message.reply_text(f"⚠️ Invalid address or ENS: {eoa}. Please resend valid addresses or ENS names.")
                return

        user_data[user_id]['eoas'] = checksummed_eoas
        user_states[user_id] = 'tracking'
        await update.message.reply_text("✅ Addresses saved.")
        await yport(update, context)
    else:
        await update.message.reply_text("⚙️ I'm currently tracking your data. To reset, send /start again.")

async def generate_report(user_id):
    user_specific_data = user_data.get(user_id)

    if not user_specific_data:
        return "⚠️ No data found. Please start by sending /start."

    eoas = user_specific_data['eoas']
    report_lines = []
    total_usd_value = Decimal('0')
    total_apr = Decimal('0')
    total_balance_usd = Decimal('0')
    total_yield_7d = Decimal('0')
    total_yield_30d = Decimal('0')
    total_usd_change_7d = Decimal('0')
    total_usd_change_30d = Decimal('0')

    vaults_data = fetch_yearn_vault_data()
    if not vaults_data:
        logger.error(f"Error fetching Yearn vault data for user {user_id}")
        return "🚨 Unable to retrieve data at the moment. Please try again later."

    token_balances = {}
    for eoa in eoas:
        for chain in ['ethereum', 'base', 'optimism', 'polygon', 'arbitrum']:
            balances = get_token_balances(eoa, chain)
            if not balances:
                logger.error(f"Error fetching token balances for EOA: {eoa} on chain: {chain}")
                return "🚨 Unable to retrieve data at the moment. Please try again later."
            token_balances.update(balances)

    report_vaults_data = []

    for vault_data in vaults_data:
        vault_address_lower = vault_data['address'].lower()

        if vault_address_lower in token_balances:
            balance_hex = token_balances[vault_address_lower]
            vault_balance = int(balance_hex, 16)

            if vault_balance == 0:
                continue

            name = vault_data.get('name', 'Unknown')
            symbol = vault_data.get('symbol', 'Unknown')
            price_per_share = Decimal(vault_data.get('pricePerShare') or 0) / (10 ** vault_data.get('decimals', 18))
            underlying_token_price = Decimal(vault_data.get('tvl', {}).get('price', 0))
            underlying_token_address = vault_data.get('token', {}).get('address', '').lower()

            apr = vault_data.get('apr', {}).get('forwardAPR', {}).get('netAPR', 0)
            if apr is None or apr == 0:
                apr = vault_data.get('apr', {}).get('netAPR', 0)
            apr = Decimal(apr or 0) * Decimal('100')

            vault_balance_in_tokens = Decimal(vault_balance) * price_per_share / (10 ** vault_data.get('decimals', 18))
            vault_usd_value = vault_balance_in_tokens * underlying_token_price

            if vault_usd_value < Decimal('0.0001'):
                continue

            total_usd_value += vault_usd_value
            total_apr += apr * vault_usd_value
            total_balance_usd += vault_usd_value

            chain_id = vault_data.get('chainID')
            timeseries_data = await fetch_historical_pricepershare_kong(vault_data['address'], chain_id)

            if timeseries_data:
                current_pps, pps_7d_ago, pps_30d_ago = process_timeseries_data_with_decimal(timeseries_data)
                yield_7d = calculate_yield_with_decimal(current_pps, pps_7d_ago)
                yield_30d = calculate_yield_with_decimal(current_pps, pps_30d_ago)

                usd_change_7d = vault_balance_in_tokens * (current_pps - pps_7d_ago)
                usd_change_30d = vault_balance_in_tokens * (current_pps - pps_30d_ago)

                total_yield_7d += yield_7d * vault_usd_value
                total_yield_30d += yield_30d * vault_usd_value
                total_usd_change_7d += usd_change_7d
                total_usd_change_30d += usd_change_30d
            else:
                yield_7d, yield_30d = Decimal('0'), Decimal('0')
                usd_change_7d, usd_change_30d = Decimal('0'), Decimal('0')

            vault_url = f"https://yearn.fi/v3/{vault_data['chainID']}/{vault_data['address']}"
            report_lines.append(
                f"<b><a href='{vault_url}'>{name} ({symbol})</a></b>\n"
                f"💵 Value: ${vault_usd_value:,.2f}\n"
                f"📊 APR: {apr:.2f}%\n"
                f"📈 Est. Yield: {yield_7d:.2f}% (7d / ${usd_change_7d:,.2f}), {yield_30d:.2f}% (30d / ${usd_change_30d:,.2f})\n"
            )

            report_vaults_data.append({
                'address': vault_data['address'].lower(),
                'underlying_token_address': underlying_token_address,
                'apr': apr,
                'chainID': vault_data['chainID'],
            })

    average_apr = total_apr / total_balance_usd if total_balance_usd > 0 else Decimal('0')
    average_yield_7d = total_yield_7d / total_usd_value if total_usd_value > 0 else Decimal('0')
    average_yield_30d = total_yield_30d / total_usd_value if total_usd_value > 0 else Decimal('0')

    report_lines.append(f"💼 Total Value: ${total_usd_value:,.2f}")
    report_lines.append(f"📊 Avg. APR: {average_apr:.2f}%")
    report_lines.append(f"📈 Avg. Est. Yield: {average_yield_7d:.2f}% (7d / ${total_usd_change_7d:,.2f}), {average_yield_30d:.2f}% (30d / ${total_usd_change_30d:,.2f})")

    report = "\n".join(report_lines)

    if len(report) > 4066:
        report = report[:4066] + "\n... Truncated due to length."

    return report, report_vaults_data

async def generate_vault_suggestions(user_id, report_vaults_data):
    user_specific_data = user_data.get(user_id)

    if not user_specific_data:
        return None

    eoas = user_specific_data['eoas']
    vaults_data = fetch_yearn_vault_data()
    if not vaults_data:
        return None

    token_balances = {}
    for eoa in eoas:
        for chain in ['ethereum', 'base', 'optimism', 'polygon', 'arbitrum']:
            balances = get_token_balances(eoa, chain)
            if balances:
                token_balances.update(balances)

    matched_vaults = [
        vault_data for vault_data in vaults_data
        if any(
            user_vault['underlying_token_address'].lower() == vault_data.get('token', {}).get('address', '').lower() and
            user_vault['chainID'] == vault_data['chainID']
            for user_vault in report_vaults_data
        )
    ]

    suggested_vaults = []
    suggested_vaults_set = set()

    for vault_data in matched_vaults:
        vault_token_address = vault_data.get('token', {}).get('address', '').lower()
        vault_apr = Decimal(vault_data.get('apr', {}).get('forwardAPR', {}).get('netAPR') or 0) * Decimal('100')
        vault_chain_id = vault_data['chainID']
        vault_address = vault_data['address'].lower()
        vault_symbol = vault_data.get('symbol', 'Unknown')

        for user_vault in report_vaults_data:
            if vault_token_address == user_vault['underlying_token_address'].lower() and vault_chain_id == user_vault['chainID']:
                apr_difference = vault_apr - user_vault['apr']

                if apr_difference > Decimal('3.0'):
                    vault_balance_in_tokens = int(token_balances.get(vault_address, '0x0'), 16)
                    price_per_share = Decimal(vault_data.get('pricePerShare') or 0) / (10 ** (vault_data.get('decimals') or 18))
                    underlying_token_price = Decimal(vault_data.get('tvl', {}).get('price') or 0)
                    vault_usd_value = vault_balance_in_tokens * price_per_share * underlying_token_price

                    if vault_address in suggested_vaults_set:
                        continue

                    timeseries_data = await fetch_historical_pricepershare_kong(vault_data['address'], vault_chain_id)
                    if timeseries_data:
                        current_pps, pps_7d_ago, pps_30d_ago = process_timeseries_data_with_decimal(timeseries_data)
                        yield_7d = calculate_yield_with_decimal(current_pps, pps_7d_ago)
                        yield_30d = calculate_yield_with_decimal(current_pps, pps_30d_ago)
                    else:
                        yield_7d, yield_30d = Decimal('0'), Decimal('0')

                    vault_url = f"https://yearn.fi/v3/{vault_chain_id}/{vault_data['address']}"
                    suggested_vaults.append(
                        f"<b><a href='{vault_url}'>{vault_data['name']} ({vault_symbol})</a></b>\n"
                        f"📊 APR: {vault_apr:.2f}%\n"
                        f"📈 Est. Yield: {yield_7d:.2f}% (7d), {yield_30d:.2f}% (30d)\n"
                    )

                    suggested_vaults_set.add(vault_address)

                break

    if suggested_vaults:
        suggested_vaults_report = "<b>Vault Suggestions Based on Your Current Deposits:</b>\n\n" + "\n".join(suggested_vaults)

        while len(suggested_vaults_report) > 4066:
            suggested_vaults.pop()
            suggested_vaults_report = "<b>Vault Suggestions Based on Your Current Deposits:</b>\n" + "\n".join(suggested_vaults)

        truncated_suggestions = truncate_html_message(suggested_vaults_report)
        truncated_suggestions += "\n<i>Only vaults with APRs at least 3% higher than your current vaults are shown.</i>"
        return truncated_suggestions

    return None

def truncate_html_message(message, max_length=4096):
    if len(message) <= max_length:
        return message

    truncated_message = message[:max_length - 30]
    open_tags = []
    tag_matches = re.finditer(r'<(/?)([a-zA-Z]+)[^>]*>', truncated_message)

    for match in tag_matches:
        if match.group(1) == '/':
            if match.group(2) in open_tags:
                open_tags.remove(match.group(2))
        else:
            open_tags.append(match.group(2))

    for tag in reversed(open_tags):
        truncated_message += f'</{tag}>'

    return truncated_message + "\n... Truncated due to length."

async def daily_send_reports(context: CallbackContext):
    if not user_data:
        return

    for user_id in user_data.keys():
        try:
            report, report_vaults_data = await generate_report(user_id)
            vault_suggestions = await generate_vault_suggestions(user_id, report_vaults_data)
            await context.bot.send_message(chat_id=user_id, text=report, parse_mode='HTML', disable_web_page_preview=True)
            if vault_suggestions:
                await context.bot.send_message(chat_id=user_id, text=vault_suggestions, parse_mode='HTML', disable_web_page_preview=True)
        except Exception as e:
            print(f"Error sending daily report to user {user_id}: {str(e)}")

async def daily_usage_report(context: CallbackContext):
    global daily_usage_count

    if daily_usage_count > 0:
        await context.bot.send_message(chat_id=ADMIN_CHAT_ID, text=f"Uses today: {daily_usage_count}")
    daily_usage_count = 0

async def yport(update: Update, context: CallbackContext):
    global daily_usage_count
    user_id = str(update.effective_chat.id)

    if user_id not in user_data or not user_data[user_id].get('eoas'):
        await update.message.reply_text("⚠️ Please start by sending /start.")
        return

    if report_locks.get(user_id, False):
        await update.message.reply_text("⚠️ A report is already being generated. Please wait.", disable_web_page_preview=True)
        return

    report_locks[user_id] = True
    try:
        await update.message.reply_text("🔄 Generating your report. Please wait...", disable_web_page_preview=True)

        report, report_vaults_data = await generate_report(user_id)
        truncated_report = truncate_html_message(report)

        vault_suggestions = await generate_vault_suggestions(user_id, report_vaults_data)
        truncated_suggestions = truncate_html_message(vault_suggestions) if vault_suggestions else None

        await update.message.reply_text(truncated_report, parse_mode='HTML', disable_web_page_preview=True)

        if truncated_suggestions:
            await update.message.reply_text(truncated_suggestions, parse_mode='HTML', disable_web_page_preview=True)

        daily_usage_count += 1
    finally:
        report_locks[user_id] = False

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('yport', yport))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    job_queue = app.job_queue
    job_queue.run_daily(daily_usage_report, time=time(hour=0, minute=0, second=0))
    job_queue.run_daily(daily_send_reports, time=time(hour=0, minute=0, second=0))

    app.run_polling()

if __name__ == '__main__':
    main()
