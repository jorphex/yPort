import asyncio
import logging
from typing import List

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, MessageEntity, Update
from telegram.ext import Application, ApplicationBuilder, CallbackContext, CallbackQueryHandler, CommandHandler, MessageHandler, filters
from telegramify_markdown import convert, split_entities

from ..addressing import parse_addresses_input
from ..config import Config
from ..report import ReportService
from ..web3_utils import Web3Manager
from ..storage import SQLiteStore
from ..format.telegram import (
    escape_markdown,
    render_chain_sections,
    render_overall_section,
    render_report,
    render_suggestions,
)

logger = logging.getLogger(__name__)

CALLBACK_REPORT = "action:yport"
CALLBACK_ADDRESSES = "action:addresses"
CALLBACK_DAILY_TOGGLE = "action:daily_toggle"
CALLBACK_HELP = "action:help"

TELEGRAM_MAX_LEN = 4096

class TelegramBot:
    def __init__(self, config: Config, store: SQLiteStore, report_service: ReportService, web3_manager: Web3Manager) -> None:
        self._config = config
        self._store = store
        self._report_service = report_service
        self._web3 = web3_manager
        self._application: Application = ApplicationBuilder().token(config.telegram_bot_token).build()
        self._locks: dict[str, asyncio.Lock] = {}

        self._application.add_handler(CommandHandler("start", self._start))
        self._application.add_handler(CommandHandler("yport", self._yport_command))
        self._application.add_handler(CommandHandler("addresses", self._addresses_command))
        self._application.add_handler(CommandHandler("dailytoggle", self._daily_toggle_command))
        self._application.add_handler(CommandHandler("help", self._help_command))
        self._application.add_handler(CallbackQueryHandler(self._button_handler))
        self._application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_message))

    @property
    def application(self) -> Application:
        return self._application

    async def start(self) -> None:
        await self._application.initialize()
        await self._application.start()
        await self._application.updater.start_polling()
        await self._register_commands()
        logger.info("Telegram bot started")

    async def stop(self) -> None:
        await self._application.updater.stop()
        await self._application.stop()
        await self._application.shutdown()
        logger.info("Telegram bot stopped")

    async def _register_commands(self) -> None:
        commands = [
            BotCommand("start", "Start and add addresses"),
            BotCommand("yport", "Generate your report"),
            BotCommand("addresses", "Manage addresses"),
            BotCommand("dailytoggle", "Toggle daily reports"),
            BotCommand("help", "Help"),
        ]
        await self._application.bot.set_my_commands(commands)

    async def _main_keyboard_for(self, user_id: str) -> InlineKeyboardMarkup:
        daily_enabled = await self._store.get_daily_reports_enabled("telegram", user_id)
        daily_label = "🔔 Daily Reports: ON" if daily_enabled else "🔕 Daily Reports: OFF"
        keyboard = [
            [InlineKeyboardButton("📊 Generate Report (yPort)", callback_data=CALLBACK_REPORT)],
            [InlineKeyboardButton("🧾 Manage Addresses", callback_data=CALLBACK_ADDRESSES)],
            [
                InlineKeyboardButton(daily_label, callback_data=CALLBACK_DAILY_TOGGLE),
            ],
            [InlineKeyboardButton("❓ Help", callback_data=CALLBACK_HELP)],
        ]
        return InlineKeyboardMarkup(keyboard)

    def _format_address_line(self, address: str, ens_name: str | None) -> str:
        if ens_name:
            return f"- {address} ({ens_name})"
        return f"- {address}"

    async def _addresses_prompt(self, user_id: str) -> str:
        addresses = await self._store.get_addresses("telegram", user_id)
        if addresses:
            lines = ["🧾 Current addresses:"]
            lines.extend([self._format_address_line(row["address"], row.get("ens_name")) for row in addresses])
            lines.append("Send new addresses to replace them.")
            return "\n".join(lines)
        return "⚠️ No addresses saved yet. Send addresses or ENS names."

    async def _start(self, update: Update, context: CallbackContext) -> None:
        user_id = str(update.effective_chat.id)
        logger.info("Telegram /start from %s", user_id)
        message = "👋 Welcome!\n\n" + await self._addresses_prompt(user_id)
        await self._reply(update, context, message, reply_markup=await self._main_keyboard_for(user_id))

    async def _help_command(self, update: Update, context: CallbackContext) -> None:
        user_id = str(update.effective_chat.id)
        message = (
            "📌 Commands:\n"
            "/start - add addresses\n"
            "/yport - generate a report\n"
            "/addresses - view or replace addresses\n"
            "/dailytoggle - toggle daily reports\n"
        )
        await self._reply(update, context, message, reply_markup=await self._main_keyboard_for(user_id))

    async def _addresses_command(self, update: Update, context: CallbackContext) -> None:
        user_id = str(update.effective_chat.id)
        message = await self._addresses_prompt(user_id)
        await self._reply(update, context, message, reply_markup=await self._main_keyboard_for(user_id))

    async def _daily_toggle_command(self, update: Update, context: CallbackContext) -> None:
        user_id = str(update.effective_chat.id)
        enabled = await self._store.get_daily_reports_enabled("telegram", user_id)
        new_state = not enabled
        await self._store.set_daily_reports("telegram", user_id, new_state)
        if new_state:
            time_str = self._config.daily_report_time_utc.strftime("%H:%M")
            message = f"🔔 Daily reports enabled. Expect them around {time_str} UTC."
        else:
            message = "🔕 Daily reports disabled."
        await self._reply(update, context, message, reply_markup=await self._main_keyboard_for(user_id))

    async def _yport_command(self, update: Update, context: CallbackContext) -> None:
        await self._send_report(update, context)

    async def _button_handler(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        await query.answer()
        user_id = str(query.message.chat_id)
        action = query.data

        if action == CALLBACK_REPORT:
            await self._send_report(update, context)
        elif action == CALLBACK_ADDRESSES:
            await self._addresses_command(update, context)
        elif action == CALLBACK_DAILY_TOGGLE:
            await self._daily_toggle_command(update, context)
        elif action == CALLBACK_HELP:
            await self._help_command(update, context)
        else:
            await context.bot.send_message(
                chat_id=user_id,
                text="Unknown action.",
                reply_markup=await self._main_keyboard_for(user_id),
            )

    async def _handle_message(self, update: Update, context: CallbackContext) -> None:
        user_id = str(update.effective_chat.id)
        text = update.message.text.strip()

        addresses, errors, ens_map, had_candidates = await parse_addresses_input(text, self._web3)

        if addresses:
            unique_addresses = sorted(set(addresses))
            await self._store.set_addresses("telegram", user_id, unique_addresses, ens_map)
            lines = [f"✅ Saved {len(unique_addresses)} address(es):"]
            lines.extend([self._format_address_line(addr, ens_map.get(addr)) for addr in unique_addresses])
            if errors:
                lines.append("⚠️ Some inputs could not be processed:")
                lines.extend([f"- {err}" for err in errors])
            await self._reply(update, context, "\n".join(lines), reply_markup=await self._main_keyboard_for(user_id))
            return

        if had_candidates and errors:
            await self._reply(
                update,
                context,
                "⚠️ No valid addresses found. Errors:\n" + "\n".join(f"- {err}" for err in errors),
                reply_markup=await self._main_keyboard_for(user_id),
            )
            return

        if not had_candidates:
            existing = await self._store.get_addresses("telegram", user_id)
            if existing:
                await self._reply(
                    update,
                    context,
                    "Send new addresses to replace the current list.",
                    reply_markup=await self._main_keyboard_for(user_id),
                )
            else:
                await self._reply(
                    update,
                    context,
                    "➡️ Send wallet addresses or ENS names separated by spaces.\nExample: 0xabc... 0xdef... vitalik.eth",
                    reply_markup=await self._main_keyboard_for(user_id),
                )

    async def _reply(self, update: Update, context: CallbackContext, text: str, reply_markup=None) -> None:
        if update.message:
            await update.message.reply_text(text, reply_markup=reply_markup)
            return
        chat_id = update.effective_chat.id if update.effective_chat else None
        if chat_id is not None:
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)

    def _markdown_chunks(self, lines: List[str]) -> List[tuple[str, List[MessageEntity]]]:
        text = "\n".join(lines)
        converted_text, entities = convert(text)
        chunks = split_entities(converted_text, entities, TELEGRAM_MAX_LEN)
        normalized: List[tuple[str, List[MessageEntity]]] = []
        for chunk_text, chunk_entities in chunks:
            normalized_entities: List[MessageEntity] = []
            for entity in chunk_entities:
                if isinstance(entity, MessageEntity):
                    normalized_entities.append(entity)
                elif hasattr(entity, "to_dict"):
                    normalized_entities.append(MessageEntity(**entity.to_dict()))
                else:
                    normalized_entities.append(MessageEntity(**entity))
            normalized.append((chunk_text, normalized_entities))
        return normalized

    async def _send_report(self, update: Update, context: CallbackContext) -> None:
        if update.callback_query and update.callback_query.message:
            user_id = str(update.callback_query.message.chat_id)
        else:
            user_id = str(update.effective_chat.id)

        lock = self._locks.setdefault(user_id, asyncio.Lock())
        if lock.locked():
            await context.bot.send_message(
                chat_id=user_id,
                text="⏳ A report is already being generated. Please wait.",
                reply_markup=await self._main_keyboard_for(user_id),
            )
            return

        addresses_rows = await self._store.get_addresses("telegram", user_id)
        addresses = [row["address"] for row in addresses_rows]
        if not addresses:
            await context.bot.send_message(
                chat_id=user_id,
                text="⚠️ Please send your address(es) or ENS name(s) first.",
                reply_markup=await self._main_keyboard_for(user_id),
            )
            return

        async with lock:
            await context.bot.send_message(
                chat_id=user_id,
                text="🔄 Generating your Yearn portfolio report...\n\nThis might take a minute...",
            )
            try:
                report = await self._report_service.generate(addresses)
            except Exception as exc:
                logger.error("Report generation failed: %s", exc)
                await context.bot.send_message(
                    chat_id=user_id,
                    text="❌ An error occurred while generating your report. Please try again later.",
                )
                return

            await self._store.increment_usage(on_demand=1)

            if report.empty:
                sections = [render_report(report, self._config)]
            else:
                sections = render_chain_sections(report, self._config)
                header_lines = ["✏️ **Your Yearn Portfolio Report**"]
                if report.has_yearn_gauge_deposit:
                    header_lines.append(f"⚠️ *{escape_markdown(self._config.veyfi_deprecation_message)}*")
                if sections:
                    sections[0] = header_lines + sections[0]
                else:
                    sections.append(header_lines)
                sections.append(render_overall_section(report))

            suggestions_lines = render_suggestions(report.suggestions)
            if suggestions_lines:
                sections.append(suggestions_lines)

            for idx, section in enumerate(sections):
                chunks = self._markdown_chunks(section)
                for chunk_index, (chunk_text, chunk_entities) in enumerate(chunks):
                    is_last_section = idx == len(sections) - 1
                    is_last_chunk = chunk_index == len(chunks) - 1
                    markup = await self._main_keyboard_for(user_id) if (is_last_section and is_last_chunk) else None
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=chunk_text,
                        entities=chunk_entities,
                        disable_web_page_preview=True,
                        reply_markup=markup,
                    )

    async def send_daily_reports(self) -> None:
        users = await self._store.get_daily_users("telegram")
        for row in users:
            user_id = row["user_id"]
            addresses_rows = await self._store.get_addresses("telegram", user_id)
            addresses = [r["address"] for r in addresses_rows]
            if not addresses:
                continue
            try:
                report = await self._report_service.generate(addresses)
            except Exception as exc:
                logger.error("Daily report failed for %s: %s", user_id, exc)
                continue

            report_lines = render_report(report, self._config)
            if report.empty:
                sections = [report_lines]
            else:
                sections = render_chain_sections(report, self._config)
                header_lines = ["✏️ **Your Yearn Portfolio Report**"]
                if report.has_yearn_gauge_deposit:
                    header_lines.append(f"⚠️ *{escape_markdown(self._config.veyfi_deprecation_message)}*")
                if sections:
                    sections[0] = header_lines + sections[0]
                else:
                    sections.append(header_lines)
                sections.append(render_overall_section(report))

            suggestions_lines = render_suggestions(report.suggestions)
            if suggestions_lines:
                sections.append(suggestions_lines)

            for idx, section in enumerate(sections):
                chunks = self._markdown_chunks(section)
                for chunk_index, (chunk_text, chunk_entities) in enumerate(chunks):
                    is_last_section = idx == len(sections) - 1
                    is_last_chunk = chunk_index == len(chunks) - 1
                    markup = await self._main_keyboard_for(user_id) if (is_last_section and is_last_chunk) else None
                    await self._application.bot.send_message(
                        chat_id=user_id,
                        text=chunk_text,
                        entities=chunk_entities,
                        disable_web_page_preview=True,
                        reply_markup=markup,
                    )

            await self._store.increment_usage(daily=1)
