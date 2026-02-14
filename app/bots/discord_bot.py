import asyncio
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, Optional

import discord
from discord.ext import commands, tasks
from ..addressing import parse_addresses_input
from ..config import Config
from ..format.discord import render_report, render_suggestions
from ..messages import split_lines
from ..report import ReportService, format_tvl
from ..storage import SQLiteStore
from ..web3_utils import Web3Manager
from ..chains import CHAIN_NAMES
from ..yearn_api import YearnApi

logger = logging.getLogger(__name__)

DISCORD_MAX_LEN = 2000

def _format_address_line(address: str, ens_name: Optional[str]) -> str:
    if ens_name:
        return f"- {address} ({ens_name})"
    return f"- {address}"

SINGLE_ASSET_TOKENS = {
    "ethereum": {
        "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
        "usdc": "0xA0b86991c6218b36c1d19D4a2e9eb0cE3606eb48",
        "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
    },
    "arbitrum": {
        "weth": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "dai": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
        "usdc": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "usdc_e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        "usdt": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
        "wbtc": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
    },
    "polygon": {
        "weth": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
        "dai": "0x8f3Cf7ad23Cd3CaDbD9735AFf958023239c6A063",
        "usdc": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
        "usdc_e": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174",
        "usdt": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
        "wbtc": "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6",
    },
    "base": {
        "weth": "0x4200000000000000000000000000000000000006",
        "dai": "0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb",
        "usdc": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    },
    "optimism": {
        "weth": "0x4200000000000000000000000000000000000006",
        "dai": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
        "usdc": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
        "usdc_e": "0x7F5c764cBc14f9669B88837ca1490cCa17c31607",
        "usdt": "0x94b008aA00579c1307B0EF2c499aD98a8ce58e58",
        "wbtc": "0x68f180fcce6836688e9084f035309e29bf0a2095",
    },
}

class AddressModal(discord.ui.Modal):
    def __init__(self, store: SQLiteStore, web3_manager: Web3Manager, user_id: str) -> None:
        super().__init__(title="Manage addresses")
        self._store = store
        self._web3 = web3_manager
        self._user_id = user_id
        self.addresses = discord.ui.TextInput(
            label="Addresses or ENS names",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=2000,
        )
        self.add_item(self.addresses)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        text = self.addresses.value.strip()
        addresses, errors, ens_map, had_candidates = await parse_addresses_input(text, self._web3)

        if addresses:
            unique_addresses = sorted(set(addresses))
            await self._store.set_addresses("discord", self._user_id, unique_addresses, ens_map)
            lines = [f"✅ Saved {len(unique_addresses)} address(es):"]
            lines.extend([_format_address_line(addr, ens_map.get(addr)) for addr in unique_addresses])
            if errors:
                lines.append("⚠️ Some inputs could not be processed:")
                lines.extend([f"- {err}" for err in errors])
            await interaction.followup.send("\n".join(lines), ephemeral=True)
            return

        if had_candidates and errors:
            await interaction.followup.send("No valid addresses found.\n" + "\n".join(errors), ephemeral=True)
            return

        await interaction.followup.send("No valid addresses found in your input.", ephemeral=True)

class ManageAddressesView(discord.ui.View):
    def __init__(self, store: SQLiteStore, web3_manager: Web3Manager, user_id: Optional[str] = None, timeout: int = 300) -> None:
        super().__init__(timeout=timeout)
        self._store = store
        self._web3 = web3_manager
        self._user_id = user_id

    @discord.ui.button(label="🧾 Manage addresses", style=discord.ButtonStyle.primary)
    async def manage_addresses(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        user_id = self._user_id or str(interaction.user.id)
        await interaction.response.send_modal(AddressModal(self._store, self._web3, user_id))

class TopVaultsReportView(discord.ui.View):
    def __init__(self, bot_ref: "DiscordBot", timeout: int = 3 * 60 * 60) -> None:
        super().__init__(timeout=timeout)
        self._bot_ref = bot_ref

    @discord.ui.button(label="📊 Generate Report", style=discord.ButtonStyle.success)
    async def generate_report(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self._bot_ref._handle_yport(interaction)

    @discord.ui.button(label="🧾 Manage Addresses", style=discord.ButtonStyle.primary)
    async def manage_addresses(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        view = ManageAddressesView(self._bot_ref._store, self._bot_ref._web3)
        await interaction.response.send_message(
            "Use the button below to add or replace addresses.",
            ephemeral=True,
            view=view,
        )

    @discord.ui.button(label="❓ Help", style=discord.ButtonStyle.secondary)
    async def help(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message(
            "📌 Commands:\n- /yport: your Yearn report\n- /addresses: manage wallets\n- /help: this help",
            ephemeral=True,
        )

class DiscordBot:
    def __init__(
        self,
        config: Config,
        store: SQLiteStore,
        report_service: ReportService,
        web3_manager: Web3Manager,
        http_client,
        yearn_api: YearnApi,
    ) -> None:
        self._config = config
        self._store = store
        self._report_service = report_service
        self._web3 = web3_manager
        self._http = http_client
        self._yearn = yearn_api

        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        intents.guilds = True

        self.bot = commands.Bot(command_prefix="!", intents=intents)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._last_report_times: Dict[str, datetime] = {}
        self._last_scheduled_report_id: Optional[int] = None

        self.bot.event(self.on_ready)
        self.bot.event(self.on_message)

        self._register_commands()

    def _register_commands(self) -> None:
        @self.bot.tree.command(name="yport", description="Generate your Yearn report")
        async def yport_command(interaction: discord.Interaction) -> None:
            await self._handle_yport(interaction)

        @self.bot.tree.command(name="addresses", description="Show or update your saved addresses")
        async def addresses_command(interaction: discord.Interaction) -> None:
            await self._handle_addresses(interaction)

        @self.bot.tree.command(name="help", description="Show help")
        async def help_command(interaction: discord.Interaction) -> None:
            await interaction.response.send_message(
                "📌 Commands:\n- /yport: your Yearn report\n- /addresses: manage wallets\n- /help: this help",
                ephemeral=True,
            )

    async def on_ready(self) -> None:
        logger.info("Discord bot connected as %s", self.bot.user)
        try:
            synced = await self.bot.tree.sync()
            logger.info("Synced %s commands", len(synced))
        except Exception as exc:
            logger.error("Failed to sync commands: %s", exc)

        log_channel = self.bot.get_channel(self._config.discord_log_channel_id)
        if log_channel:
            await log_channel.send("Discord bot is online and ready.")

        if not self.top_vaults_task.is_running():
            self.top_vaults_task.start()

    async def on_message(self, message: discord.Message) -> None:
        if message.author == self.bot.user:
            return

        if isinstance(message.channel, discord.DMChannel):
            await self._handle_dm(message)
        elif message.guild:
            await self._moderate_public_channel(message)

        await self.bot.process_commands(message)

    async def _handle_dm(self, message: discord.Message) -> None:
        text = message.content.strip()
        addresses, errors, ens_map, had_candidates = await parse_addresses_input(text, self._web3)
        user_id = str(message.author.id)

        if addresses:
            unique_addresses = sorted(set(addresses))
            await self._store.set_addresses("discord", user_id, unique_addresses, ens_map)
            lines = [f"✅ Saved {len(unique_addresses)} address(es):"]
            lines.extend([_format_address_line(addr, ens_map.get(addr)) for addr in unique_addresses])
            if errors:
                lines.append("⚠️ Some inputs could not be processed:")
                lines.extend([f"- {err}" for err in errors])
            lines.append("You can now use /yport in the server.")
            await message.channel.send("\n".join(lines))
            return

        if had_candidates and errors:
            await message.channel.send("No valid addresses found.\n" + "\n".join(errors))
            return

        await message.channel.send("➡️ Send wallet addresses or ENS names separated by spaces. Example: 0xabc... vitalik.eth")

    async def _moderate_public_channel(self, message: discord.Message) -> None:
        if message.channel.id != self._config.discord_public_channel_id:
            return
        if message.author.id in {self._config.discord_admin_user_id, self.bot.user.id}:
            return
        try:
            await message.delete()
        except discord.Forbidden:
            logger.error("Missing permission to delete messages in public channel")
        except Exception as exc:
            logger.error("Failed to delete message: %s", exc)

    async def _handle_yport(self, interaction: discord.Interaction) -> None:
        user_id = str(interaction.user.id)
        now = datetime.utcnow()
        last_time = self._last_report_times.get(user_id)
        if last_time and now - last_time < timedelta(seconds=self._config.rate_limit_seconds):
            await interaction.response.send_message(
                f"⏳ Please wait {self._config.rate_limit_seconds} seconds before requesting another report.",
                ephemeral=True,
            )
            return
        self._last_report_times[user_id] = now

        lock = self._locks.setdefault(user_id, asyncio.Lock())
        if lock.locked():
            await interaction.response.send_message(
                "⏳ Your previous report request is still processing. Please wait.", ephemeral=True
            )
            return

        addresses_rows = await self._store.get_addresses("discord", user_id)
        addresses = [row["address"] for row in addresses_rows]
        if not addresses:
            view = ManageAddressesView(self._store, self._web3, user_id)
            await interaction.response.send_message(
                "⚠️ No addresses found. Use the button below to add them.", ephemeral=True, view=view
            )
            return

        async with lock:
            await interaction.response.defer(ephemeral=True, thinking=True)
            await interaction.followup.send(
                "🔄 Generating your Yearn portfolio report...\n\nThis might take a minute or two, especially if checking multiple chains...",
                ephemeral=True,
                suppress_embeds=True,
            )
            try:
                report = await self._report_service.generate(addresses)
            except Exception as exc:
                logger.error("Discord report generation failed: %s", exc)
                await interaction.followup.send(
                    "❌ An error occurred while generating your report. Please try again later.",
                    ephemeral=True,
                )
                return

            await self._store.increment_usage(on_demand=1)

            report_lines = render_report(report, self._config)
            suggestions_lines = render_suggestions(report.suggestions)
            sections = [report_lines]
            if suggestions_lines:
                sections.append(suggestions_lines)

            view = ManageAddressesView(self._store, self._web3, user_id)

            for idx, section in enumerate(sections):
                chunks = split_lines(section, DISCORD_MAX_LEN)
                for chunk_index, chunk in enumerate(chunks):
                    is_last_section = idx == len(sections) - 1
                    is_last_chunk = chunk_index == len(chunks) - 1
                    payload = {"ephemeral": True, "suppress_embeds": True}
                    if is_last_section and is_last_chunk:
                        payload["view"] = view
                    await interaction.followup.send(chunk, **payload)

    async def _handle_addresses(self, interaction: discord.Interaction) -> None:
        user_id = str(interaction.user.id)
        addresses_rows = await self._store.get_addresses("discord", user_id)
        if addresses_rows:
            lines = ["🧾 Current addresses:"]
            lines.extend([_format_address_line(row["address"], row.get("ens_name")) for row in addresses_rows])
            lines.append("Use the button below to replace them.")
            view = ManageAddressesView(self._store, self._web3, user_id)
            await interaction.response.send_message("\n".join(lines), ephemeral=True, view=view)
            return

        view = ManageAddressesView(self._store, self._web3, user_id)
        await interaction.response.send_message("⚠️ No addresses saved yet.", ephemeral=True, view=view)

    async def start(self) -> None:
        await self.bot.start(self._config.discord_bot_token)

    async def close(self) -> None:
        await self.bot.close()

    @tasks.loop(hours=3)
    async def top_vaults_task(self) -> None:
        await self._send_top_vaults_report()

    @top_vaults_task.before_loop
    async def before_top_vaults_task(self) -> None:
        await self.bot.wait_until_ready()

    async def _send_top_vaults_report(self) -> None:
        try:
            await self._yearn.ensure_ydaemon_cache()
            vaults_data = self._yearn.get_ydaemon_data()
            if not vaults_data:
                return

            single_asset_addresses = {
                addr.lower() for chain_tokens in SINGLE_ASSET_TOKENS.values() for addr in chain_tokens.values()
            }

            filtered = []
            for vault in vaults_data:
                if not vault.get("address") or not vault.get("chainID") or not vault.get("token", {}).get("address"):
                    continue
                if vault.get("info", {}).get("retired", False):
                    continue
                if vault.get("kind") != "Multi Strategy":
                    continue
                if vault["token"]["address"].lower() not in single_asset_addresses:
                    continue
                if Decimal(vault.get("tvl", {}).get("tvl", 0)) < Decimal("50000"):
                    continue

                apr_data = vault.get("apr", {})
                points_data = apr_data.get("points", {})
                net_apr = apr_data.get("netAPR")
                week_ago = points_data.get("weekAgo")
                month_ago = points_data.get("monthAgo")

                if net_apr is not None:
                    primary_apr = float(net_apr)
                elif week_ago is not None:
                    primary_apr = float(week_ago)
                elif month_ago is not None:
                    primary_apr = float(month_ago)
                else:
                    primary_apr = 0.0

                if abs(primary_apr) < 0.000001:
                    continue
                vault["_sort_apr"] = primary_apr
                filtered.append(vault)

            filtered.sort(key=lambda v: float(v.get("_sort_apr", 0.0)), reverse=True)
            top_vaults = filtered[:5]
            if not top_vaults:
                return

            yearn_tvl = await self._fetch_yearn_tvl()
            formatted_tvl = format_tvl(Decimal(yearn_tvl)) if yearn_tvl else "Unavailable"

            embed = discord.Embed(
                title="Top 5 Single-Asset Vaults by APY",
                description=f"Based on available APY data. Total Yearn TVL: {formatted_tvl}",
                color=discord.Color.blue(),
            )

            for idx, vault in enumerate(top_vaults, start=1):
                name = vault.get("display_name") or vault.get("name", "Unknown")
                token = vault.get("token", {}).get("symbol", "?")
                chain_id = vault.get("chainID")
                chain = CHAIN_NAMES.get(chain_id, str(chain_id))
                vault_address = vault.get("address")
                apr_value = vault.get("_sort_apr", 0)
                apr_percent = Decimal(apr_value) * Decimal("100")
                tvl_vault = format_tvl(Decimal(vault.get("tvl", {}).get("tvl", 0)))
                vault_url = f"https://yearn.fi/v3/{chain_id}/{vault_address}"

                embed.add_field(
                    name=f"{idx}. {name}",
                    value=f"[{token} / {chain}]({vault_url})\nAPY: **{apr_percent:.2f}%** | TVL: {tvl_vault}",
                    inline=False,
                )

            embed.set_footer(text="Updated every 3 hours.")
            embed.timestamp = datetime.utcnow()

            channel = self.bot.get_channel(self._config.discord_public_channel_id)
            if not channel:
                return

            if self._last_scheduled_report_id:
                try:
                    old = await channel.fetch_message(self._last_scheduled_report_id)
                    await old.delete()
                except Exception:
                    pass

            report_view = TopVaultsReportView(self)
            new_message = await channel.send(embed=embed, view=report_view)
            self._last_scheduled_report_id = new_message.id
        except Exception as exc:
            logger.error("Top vaults report failed: %s", exc)

    async def _fetch_yearn_tvl(self) -> float:
        url = "https://api.llama.fi/tvl/yearn"
        try:
            async with self._http.session.get(url, timeout=10) as response:
                if response.status == 200:
                    return float(await response.json())
        except Exception as exc:
            logger.error("Yearn TVL fetch failed: %s", exc)
        return 0

    async def send_usage_report(self, usage: dict) -> None:
        log_channel = self.bot.get_channel(self._config.discord_log_channel_id)
        if log_channel:
            await log_channel.send(
                f"Daily usage report:\nOn-demand reports: {usage.get('on_demand_reports')}\nDaily reports: {usage.get('daily_reports')}"
            )
