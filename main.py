import asyncio
import logging
import signal
from datetime import datetime, time, timedelta, timezone

from app.config import load_config
from app.http import SharedHttpClient
from app.storage import SQLiteStore
from app.web3_utils import Web3Manager
from app.yearn_api import YearnApi
from app.report import ReportService
from app.bots.telegram_bot import TelegramBot
from app.bots.discord_bot import DiscordBot

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("discord").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

async def _cache_loop(yearn_api: YearnApi, interval: int, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await yearn_api.update_all_caches()
        except Exception as exc:
            logger.error("Cache update failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            continue

async def _daily_loop(target_time: time, stop_event: asyncio.Event, callback) -> None:
    while not stop_event.is_set():
        now = datetime.now(timezone.utc)
        scheduled = datetime.combine(now.date(), target_time, tzinfo=timezone.utc)
        if now >= scheduled:
            scheduled += timedelta(days=1)
        wait_seconds = (scheduled - now).total_seconds()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            await callback()

async def main() -> None:
    config = load_config()

    if not config.alchemy_api_key:
        logger.warning("ALCHEMY_API_KEY not set")

    enable_telegram = config.enable_telegram and bool(config.telegram_bot_token)
    enable_discord = config.enable_discord and bool(config.discord_bot_token)

    if config.enable_telegram and not config.telegram_bot_token:
        logger.warning("Telegram enabled but TELEGRAM_BOT_TOKEN is missing")
    if config.enable_discord and not config.discord_bot_token:
        logger.warning("Discord enabled but DISCORD_BOT_TOKEN is missing")

    http_client = SharedHttpClient()
    await http_client.start()

    store = SQLiteStore(config.db_path)
    await store.init()

    web3_manager = Web3Manager(config.alchemy_api_key)
    web3_manager.init_ens()
    yearn_api = YearnApi(http_client, web3_manager, config.cache_expiry_seconds)
    await yearn_api.update_all_caches()

    report_service = ReportService(config, yearn_api, web3_manager, http_client)

    telegram_bot = None
    discord_bot = None

    if enable_telegram:
        telegram_bot = TelegramBot(config, store, report_service, web3_manager)
    if enable_discord:
        discord_bot = DiscordBot(config, store, report_service, web3_manager, http_client, yearn_api)

    stop_event = asyncio.Event()

    def _handle_signal(*_args) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    background_tasks = [
        asyncio.create_task(_cache_loop(yearn_api, config.cache_expiry_seconds, stop_event))
    ]

    if telegram_bot:
        background_tasks.append(
            asyncio.create_task(_daily_loop(config.daily_report_time_utc, stop_event, telegram_bot.send_daily_reports))
        )

    async def usage_report_callback() -> None:
        date_str = datetime.utcnow().date().isoformat()
        usage = await store.get_usage(date_str)
        if usage.get("on_demand_reports") or usage.get("daily_reports"):
            if telegram_bot and config.telegram_admin_chat_id:
                await telegram_bot.application.bot.send_message(
                    chat_id=config.telegram_admin_chat_id,
                    text=(
                        "Daily usage report:\n"
                        f"On-demand reports: {usage.get('on_demand_reports')}\n"
                        f"Daily reports: {usage.get('daily_reports')}"
                    ),
                )
            if discord_bot:
                await discord_bot.send_usage_report(usage)
        await store.reset_usage(date_str)

    background_tasks.append(
        asyncio.create_task(_daily_loop(time(hour=0, minute=1, tzinfo=timezone.utc), stop_event, usage_report_callback))
    )

    run_tasks = []
    if telegram_bot:
        await telegram_bot.start()
    if discord_bot:
        run_tasks.append(asyncio.create_task(discord_bot.start()))

    if run_tasks:
        wait_tasks = [asyncio.create_task(stop_event.wait())] + run_tasks
        done, pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        if not stop_event.is_set():
            stop_event.set()
        for task in pending:
            task.cancel()
    else:
        await stop_event.wait()

    for task in background_tasks:
        task.cancel()

    if discord_bot:
        await discord_bot.close()
    if telegram_bot:
        await telegram_bot.stop()

    await http_client.close()
    await store.close()

if __name__ == "__main__":
    asyncio.run(main())
