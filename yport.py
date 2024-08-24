import os
import json
import requests
import sys
from datetime import datetime, timedelta, time
from web3 import Web3
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, CallbackContext
import logging

sys.stdout = sys.stderr

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.ERROR
)
logger = logging.getLogger(__name__)

BOT_TOKEN = 'BOT_TOKEN_HERE'

API_KEY = "RPC_API_KEY_HERE"

DATA_FILE = "data.json"

user_states = {}
user_data = {}
history_data = {}

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as file:
            data = json.load(file)
            return data.get('user_data', {}), data.get('history', {})
    return {}, {}

def save_data(user_data, history_data=None, key=None):
    data = {'user_data': user_data, 'history': history_data or {}}

    if history_data:
        today = datetime.utcnow().date()
        seven_days_ago = today - timedelta(days=7)
        
        for user_id, user_history in history_data.items():
            keys_to_remove = [date for date in user_history if datetime.strptime(date, '%Y-%m-%d').date() < seven_days_ago]
            for date in keys_to_remove:
                del history_data[user_id][date]

    with open(DATA_FILE, 'w') as file:
        json.dump(data, file, indent=4)

def get_token_balances(eoa, chain):
    alchemy_urls = {
        'ethereum': f"https://eth-mainnet.g.alchemy.com/v2/{API_KEY}",
        'base': f"https://base-mainnet.g.alchemy.com/v2/{API_KEY}",
        'optimism': f"https://opt-mainnet.g.alchemy.com/v2/{API_KEY}",
        'polygon': f"https://polygon-mainnet.g.alchemy.com/v2/{API_KEY}",
        'arbitrum': f"https://arb-mainnet.g.alchemy.com/v2/{API_KEY}"
    }
    
    url = alchemy_urls.get(chain)

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
        response_v2 = requests.get(f"https://ydaemon.yearn.fi/vaults/v2?limit=520&skip=0")
        response_v3 = requests.get(f"https://ydaemon.yearn.fi/vaults/v3?limit=200&skip=0")

        if response_v2.status_code == 200 and response_v3.status_code == 200:
            return response_v2.json() + response_v3.json()
        else:
            return []
    except Exception:
        return []

async def start(update: Update, context: CallbackContext):
    user_id = str(update.effective_chat.id)
    user_states[user_id] = 'awaiting_eoas'
    user_data[user_id] = {'eoas': [], 'vaults': []}
    await update.message.reply_text("👋 Welcome! Please send your EOAs (wallet addresses) separated by spaces.")

    context.job_queue.run_daily(daily_report, time=time(hour=0, minute=0, second=0), data=user_id)

async def daily_report(context: CallbackContext):
    user_id = context.job.data
    await report_command(None, context)

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
        for eoa in eoas:
            try:
                checksummed_eoa = Web3.to_checksum_address(eoa)
                checksummed_eoas.append(checksummed_eoa)
            except Exception:
                await update.message.reply_text(f"⚠️ Invalid address detected: {eoa}. Please resend valid EOAs.")
                return
        user_data[user_id]['eoas'] = checksummed_eoas
        user_states[user_id] = 'tracking'
        save_data(user_data, history_data)
        await update.message.reply_text("✅ EOAs saved.")
        await report_command(update, context)
    else:
        await update.message.reply_text("⚙️ I'm currently tracking your data. To reset, send /start again.")

async def generate_report(user_id):
    user_specific_data = user_data.get(user_id)

    if not user_specific_data:
        return "⚠️ No data found. Please start by sending /start."

    eoas = user_specific_data['eoas']
    report_lines = []
    total_usd_value = 0
    total_apr = 0
    total_balance_usd = 0

    today_key = datetime.utcnow().strftime('%Y-%m-%d')
    one_day_ago = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    seven_days_ago = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

    history_1d = history_data.get(user_id, {}).get(one_day_ago, {})
    history_7d = history_data.get(user_id, {}).get(seven_days_ago, {})

    vaults_data = fetch_yearn_vault_data()
    if not vaults_data:
        return "🚨 Unable to retrieve data at the moment. Please try again later."

    token_balances = {}
    for eoa in eoas:
        for chain in ['ethereum', 'base', 'optimism', 'polygon', 'arbitrum']:
            balances = get_token_balances(eoa, chain)
            if not balances:
                return "🚨 Unable to retrieve data at the moment. Please try again later."
            token_balances.update(balances)

    vault_history_data = {}

    for vault_data in vaults_data:
        vault_address_lower = vault_data['address'].lower()

        if vault_address_lower in token_balances:
            balance_hex = token_balances[vault_address_lower]
            vault_balance = int(balance_hex, 16)

            if vault_balance == 0:
                continue

            name = vault_data.get('name', 'Unknown')
            symbol = vault_data.get('symbol', 'Unknown')
            price_per_share = float(vault_data.get('pricePerShare', 0)) / (10 ** vault_data.get('decimals', 18))
            underlying_token_price = vault_data.get('tvl', {}).get('price', 0)

            apr = vault_data.get('apr', {}).get('forwardAPR', {}).get('netAPR', 0)
            if apr is None or apr == 0:
                apr = vault_data.get('apr', {}).get('netAPR', 0)
            if apr == 0:
                apr = vault_data.get('apr', {}).get('points', {}).get('weekAgo', 0)
            if apr == 0:
                apr = vault_data.get('apr', {}).get('points', {}).get('monthAgo', 0)
            apr = (apr or 0) * 100

            vault_balance_in_tokens = vault_balance * price_per_share / (10 ** vault_data.get('decimals', 18))
            vault_usd_value = vault_balance_in_tokens * underlying_token_price

            if vault_usd_value < 0.0001:
                continue

            total_usd_value += vault_usd_value
            total_apr += apr * vault_usd_value
            total_balance_usd += vault_usd_value

            prev_apr_1d = history_1d.get(vault_address_lower, {}).get('apr', apr)
            prev_usd_value_1d = history_1d.get(vault_address_lower, {}).get('usd_value', vault_usd_value)
            prev_apr_7d = history_7d.get(vault_address_lower, {}).get('apr', apr)
            prev_usd_value_7d = history_7d.get(vault_address_lower, {}).get('usd_value', vault_usd_value)

            apr_change_1d = apr - prev_apr_1d
            apr_change_7d = apr - prev_apr_7d
            usd_change_1d = vault_usd_value - prev_usd_value_1d
            usd_change_7d = vault_usd_value - prev_usd_value_7d

            report_lines.append(
                f"*{name}* ({symbol})\n"  
                f"💵 ${vault_usd_value:,.2f} ({usd_change_1d:+,.2f} 1d, {usd_change_7d:+,.2f} 7d)\n"
                f"📈 {apr:.2f}% ({apr_change_1d:+.2f}% 1d, {apr_change_7d:+.2f}% 7d)\n"
            )

            vault_history_data[vault_address_lower] = {'usd_value': vault_usd_value, 'apr': apr}

    average_apr = total_apr / total_balance_usd if total_balance_usd > 0 else 0

    usd_change_1d = total_usd_value - history_1d.get('total_usd_value', total_usd_value)
    usd_change_7d = total_usd_value - history_7d.get('total_usd_value', total_usd_value)
    apr_change_1d = average_apr - history_1d.get('average_apr', average_apr)
    apr_change_7d = average_apr - history_7d.get('average_apr', average_apr)

    report_lines.append(f"💼 ${total_usd_value:,.2f} ({usd_change_1d:+,.2f} 1d, {usd_change_7d:+,.2f} 7d)")
    report_lines.append(f"📊 {average_apr:.2f}% ({apr_change_1d:+,.2f}% 1d, {apr_change_7d:+.2f}% 7d)")

    report = "\n".join(report_lines)

    if user_id not in history_data:
        history_data[user_id] = {}

    history_data[user_id][today_key] = {
        'total_usd_value': total_usd_value,
        'average_apr': average_apr,
        **vault_history_data
    }

    save_data(user_data, history_data, key=today_key)
    return report

async def report_command(update: Update, context: CallbackContext):
    user_id = str(update.effective_chat.id) if update else context.job.data
    await update.message.reply_text("🔄 Generating your report, please wait...")
    report = await generate_report(user_id)
    await update.message.reply_text(report, parse_mode='Markdown')

def main():
    global user_data, history_data
    user_data, history_data = load_data()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('report', report_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.run_polling()

if __name__ == '__main__':
    main()
