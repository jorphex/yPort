import os
import json
import requests
import sys
from datetime import datetime, time, timedelta
from web3 import Web3
from web3.middleware import geth_poa_middleware
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    CallbackContext,
    JobQueue,
)
import logging

sys.stdout = sys.stderr

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)
logger = logging.getLogger(__name__)

BOT_TOKEN = 'BOT_TOKEN_HERE'

ETH_RPC = "RPC_URL_HERE"
BASE_RPC = "RPC_URL_HERE"

eth_web3 = Web3(Web3.WebsocketProvider(ETH_RPC))
base_web3 = Web3(Web3.WebsocketProvider(BASE_RPC))

eth_web3.middleware_onion.inject(geth_poa_middleware, layer=0)
base_web3.middleware_onion.inject(geth_poa_middleware, layer=0)

DATA_FILE = "data.json"

user_states = {}
user_data = {}
history_data = {}

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as file:
            data = json.load(file)
            return data.get('user_data', {})
    return {}

def load_history():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r') as file:
            data = json.load(file)
            return data.get('history', {})
    return {}

def save_data(data, key=None):
    if key:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'r') as file:
                all_data = json.load(file)
        else:
            all_data = {}

        if 'user_data' not in all_data:
            all_data['user_data'] = {}
        if 'history' not in all_data:
            all_data['history'] = {}

        today = datetime.utcnow().date()
        seven_days_ago = today - timedelta(days=7)
        history = all_data['history']

        keys_to_remove = [date for date in history if datetime.strptime(date, '%Y-%m-%d').date() < seven_days_ago]
        for date in keys_to_remove:
            del history[date]

        history[key] = data
        all_data['history'] = history
        data = all_data

    with open(DATA_FILE, 'w') as file:
        json.dump(data, file, indent=4)

def fetch_yearn_vault_data(vault_address):
    try:
        response_v2 = requests.get(f"https://ydaemon.yearn.fi/vaults/v2?limit=520&skip=0")
        response_v3 = requests.get(f"https://ydaemon.yearn.fi/vaults/v3?limit=200&skip=0")

        if response_v2.status_code == 200:
            v2_data = response_v2.json()
        else:
            logger.error(f"Failed to fetch Yearn v2 vault data. Status code: {response_v2.status_code}")
            v2_data = []

        if response_v3.status_code == 200:
            v3_data = response_v3.json()
        else:
            logger.error(f"Failed to fetch Yearn v3 vault data. Status code: {response_v3.status_code}")
            v3_data = []

        combined_data = v2_data + v3_data

        for vault in combined_data:
            if Web3.to_checksum_address(vault['address']) == vault_address:
                return vault

        logger.error(f"Vault {vault_address} not found in Yearn API.")
        return None

    except Exception as e:
        logger.error(f"Exception fetching vault data: {e}")
        return None

def get_token_balance(web3_instance, token_address, wallet_address, chain_id=None):
    try:
        abi = [
            {
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function",
            }
        ]

        if chain_id == 8453:
            web3_instance = base_web3
        else:
            web3_instance = eth_web3
        
        contract = web3_instance.eth.contract(address=token_address, abi=abi)
        balance = contract.functions.balanceOf(wallet_address).call()
        return balance
    except Exception as e:
        logger.error(f"Error fetching balance: {e}")
        return 0

async def start(update: Update, context: CallbackContext):
    user_id = update.effective_chat.id
    user_states[user_id] = 'awaiting_eoas'
    user_data[user_id] = {'eoas': [], 'vaults': []}
    print("Received /start command.")
    await update.message.reply_text("👋 Welcome! Please send your EOAs (wallet addresses) separated by spaces.")

async def handle_message(update: Update, context: CallbackContext):
    user_id = update.effective_chat.id
    text = update.message.text.strip()

    if user_id not in user_states:
        await update.message.reply_text("Please start by sending /start")
        return

    state = user_states[user_id]

    if state == 'awaiting_eoas':
        eoas = text.split()
        checksummed_eoas = []
        for eoa in eoas:
            try:
                checksummed_eoa = Web3.to_checksum_address(eoa)
                checksummed_eoas.append(checksummed_eoa)
            except Exception as e:
                logger.error(f"Invalid EOA address: {eoa}")
                await update.message.reply_text(f"Invalid address detected: {eoa}. Please resend valid EOAs.")
                return
        user_data[user_id]['eoas'] = checksummed_eoas
        user_states[user_id] = 'awaiting_vaults'
        print(f"Received EOAs: {', '.join(checksummed_eoas)}")
        await update.message.reply_text("✅ EOAs saved. Now please send the Yearn vault addresses you want to track, separated by spaces.")

    elif state == 'awaiting_vaults':
        vaults = text.split()
        checksummed_vaults = []
        for vault in vaults:
            try:
                checksummed_vault = Web3.to_checksum_address(vault)
                checksummed_vaults.append(checksummed_vault)
            except Exception as e:
                logger.error(f"Invalid vault address: {vault}")
                await update.message.reply_text(f"Invalid address detected: {vault}. Please resend valid vault addresses.")
                return
        user_data[user_id]['vaults'] = checksummed_vaults
        user_states[user_id] = 'tracking'
        print(f"Received Vaults: {', '.join(checksummed_vaults)}")
        await update.message.reply_text("🏦 Vaults saved and tracking started.")
        save_data(user_data)
        await report_command(update, context)
    else:
        await update.message.reply_text("I'm currently tracking your data. To reset, send /start again.")

async def generate_report(user_id):
    user_specific_data = user_data.get(str(user_id))

    if not user_specific_data:
        return "No data found. Please start by sending /start."

    eoas = user_specific_data['eoas']
    vaults = user_specific_data['vaults']
    report_lines = []
    total_usd_value = 0
    total_apr = 0
    total_balance_usd = 0

    today_key = datetime.utcnow().strftime('%Y-%m-%d')
    one_day_ago = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    seven_days_ago = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')

    history_1d = history_data.get(one_day_ago, {})
    history_7d = history_data.get(seven_days_ago, {})

    for vault_address in vaults:
        vault_data = fetch_yearn_vault_data(vault_address)
        if not vault_data:
            report_lines.append(f"⚠️ Could not fetch data for vault {vault_address}")
            continue

        name = vault_data.get('name', 'Unknown')
        symbol = vault_data.get('symbol', 'Unknown')
        price_per_share = float(vault_data.get('pricePerShare', 0)) / (10 ** vault_data.get('decimals', 18))
        underlying_token_price = vault_data.get('tvl', {}).get('price', 0)

        apr = vault_data.get('apr', {}).get('forwardAPR', {}).get('netAPR', 0)
        if apr == 0:
            apr = vault_data.get('apr', {}).get('netAPR', 0)
        if apr == 0:
            apr = vault_data.get('apr', {}).get('points', {}).get('weekAgo', 0)
        if apr == 0:
            apr = vault_data.get('apr', {}).get('points', {}).get('monthAgo', 0)
        apr *= 100

        chain_id = vault_data.get('chainID', None)

        vault_balance = 0
        for eoa in eoas:
            balance = get_token_balance(eth_web3, vault_address, eoa, chain_id)
            vault_balance += balance

        vault_balance_in_tokens = vault_balance * price_per_share / (10 ** vault_data.get('decimals', 18))
        vault_usd_value = vault_balance_in_tokens * underlying_token_price
        total_usd_value += vault_usd_value

        total_apr += apr * vault_usd_value
        total_balance_usd += vault_usd_value

        prev_apr_1d = history_1d.get(vault_address, {}).get('apr', apr) if isinstance(history_1d.get(vault_address), dict) else apr
        prev_usd_value_1d = history_1d.get(vault_address, {}).get('usd_value', vault_usd_value) if isinstance(history_1d.get(vault_address), dict) else vault_usd_value
        prev_apr_7d = history_7d.get(vault_address, {}).get('apr', apr) if isinstance(history_7d.get(vault_address), dict) else apr
        prev_usd_value_7d = history_7d.get(vault_address, {}).get('usd_value', vault_usd_value) if isinstance(history_7d.get(vault_address), dict) else vault_usd_value

        apr_change_1d = apr - prev_apr_1d
        apr_change_7d = apr - prev_apr_7d
        usd_change_1d = vault_usd_value - prev_usd_value_1d
        usd_change_7d = vault_usd_value - prev_usd_value_7d

        report_lines.append(
            f"*{name}* ({symbol})\n"  
            f"💵 ${vault_usd_value:,.2f} ({usd_change_1d:+,.2f} 1d, {usd_change_7d:+,.2f} 7d)\n"
            f"📈 {apr:.2f}% ({apr_change_1d:+.2f}% 1d, {apr_change_7d:+.2f}% 7d)\n"
        )

    average_apr = total_apr / total_balance_usd if total_balance_usd > 0 else 0

    usd_change_1d = total_usd_value - history_1d.get('total_usd_value', total_usd_value)
    usd_change_7d = total_usd_value - history_7d.get('total_usd_value', total_usd_value)
    apr_change_1d = average_apr - history_1d.get('average_apr', average_apr)
    apr_change_7d = average_apr - history_7d.get('average_apr', average_apr)

    report_lines.append(f"💼 ${total_usd_value:,.2f} (PnL: {usd_change_1d:+,.2f} 1d, {usd_change_7d:+,.2f} 7d)")
    report_lines.append(f"📊 {average_apr:.2f}% (APR Change: {apr_change_1d:+,.2f}% 1d, {apr_change_7d:+,.2f}% 7d)")

    report = "\n".join(report_lines)

    history_data[today_key] = {
        'total_usd_value': total_usd_value,
        'average_apr': average_apr,
        **{vault_address: {'usd_value': vault_usd_value, 'apr': apr} for vault_address in vaults}
    }
    save_data(history_data, key=today_key)

    return report

async def daily_report(context: CallbackContext):
    user_id = context.job.data['user_id']
    report = await generate_report(user_id)
    await context.bot.send_message(chat_id=user_id, text=report, parse_mode='Markdown')

async def report_command(update: Update, context: CallbackContext):
    user_id = update.effective_chat.id
    await update.message.reply_text("🔄 Generating your report, please wait...")
    report = await generate_report(user_id)
    await update.message.reply_text(report, parse_mode='Markdown')

def main():
    application = ApplicationBuilder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('report', report_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    global user_data
    user_data = load_data()

    global history_data
    history_data = load_history()

    for user_id in user_data:
        application.job_queue.run_daily(
            daily_report,
            time=time(hour=0, minute=0),
            data={'user_id': user_id}
        )

    print("Starting bot...")
    application.run_polling()

if __name__ == "__main__":
    main()
