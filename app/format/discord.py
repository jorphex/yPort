from decimal import Decimal
from typing import List

from ..report import ReportData, SuggestionEntry, format_tvl
from ..config import Config


def escape_markdown(text: str) -> str:
    escape_chars = "*_~`|\\"
    result = ""
    for ch in str(text):
        if ch in escape_chars:
            result += "\\" + ch
        else:
            result += ch
    return result


def _format_money(value: Decimal) -> str:
    return f"${value:,.2f}"


def _format_signed_money(value: Decimal) -> str:
    if value >= 0:
        return f"+${value:,.2f}"
    return f"${value:,.2f}"


def render_report(report: ReportData, config: Config) -> List[str]:
    lines: List[str] = ["✏️ **Your Yearn Portfolio Report**"]

    if report.has_yearn_gauge_deposit:
        lines.append(f"⚠️ *{escape_markdown(config.veyfi_deprecation_message)}*")

    if report.empty:
        lines.append("*No Yearn vault holdings found for the provided addresses.*")
        lines.append(f"*{escape_markdown(report.cache_note)}*")
        return lines

    for chain in report.chains:
        lines.append(f"--- **{escape_markdown(chain.chain_name)}** ---")
        for entry in chain.vaults:
            name = escape_markdown(entry.display_name)
            token = escape_markdown(entry.token_symbol)
            staked_indicator = ""
            if entry.staked_status == "yearn":
                staked_indicator = " *(Staked: Yearn)*"
            elif entry.staked_status == "1up":
                if entry.staked_indicator_url:
                    staked_indicator = f" *([Staked: 1UP]({entry.staked_indicator_url}))*"
                else:
                    staked_indicator = " *(Staked: 1UP)*"

            staking_line = ""
            if entry.staked_status != "none" and entry.current_staking_apr_percent > 0:
                staking_line = f"\n  Staking APR: {entry.current_staking_apr_percent:.2f}% *({escape_markdown(entry.current_staking_apr_source)})*"

            lines.append(
                f"**[{name} ({token})]({entry.vault_url})**{staked_indicator}\n"
                f"Value: {_format_money(entry.vault_usd_value)}\n"
                f"Vault APY: {entry.vault_apr_percent:.2f}%"
                f"{staking_line}"
                f"\nYield: {entry.yield_7d:.2f}% [7d] ({_format_signed_money(entry.usd_change_7d)}), {entry.yield_30d:.2f}% [30d] ({_format_signed_money(entry.usd_change_30d)})"
            )

        if chain.total_usd > 0:
            lines.append(
                "---\n"
                f"💰 **Chain Total: {_format_money(chain.total_usd)}**\n"
                f"📊 Avg Vault APY: {chain.avg_apr:.2f}%\n"
                f"📈 Avg Yield: {chain.avg_yield_7d:.2f}% [7d] ({_format_signed_money(chain.total_usd_change_7d)}), {chain.avg_yield_30d:.2f}% [30d] ({_format_signed_money(chain.total_usd_change_30d)})"
            )
        else:
            lines.append("*No holdings found on this chain.*")

    lines.append("--- **Overall Portfolio** ---")
    if report.overall.total_usd > 0:
        lines.append(f"💰 Total Value: {_format_money(report.overall.total_usd)}")
        lines.append(f"📊 Avg Vault APY: {report.overall.avg_apr:.2f}%")
        lines.append(
            f"📈 Avg Yield: {report.overall.avg_yield_7d:.2f}% [7d] ({_format_signed_money(report.overall.total_usd_change_7d)}), {report.overall.avg_yield_30d:.2f}% [30d] ({_format_signed_money(report.overall.total_usd_change_30d)})"
        )
    else:
        lines.append("*No Yearn vault holdings found for the provided addresses.*")

    lines.append(f"*{escape_markdown(report.cache_note)}*")
    return lines


def render_suggestions(suggestions: List[SuggestionEntry]) -> List[str]:
    if not suggestions:
        return []

    lines: List[str] = ["💡 **Vault Suggestions**"]
    current_chain = None

    for suggestion in suggestions:
        if suggestion.chain_name != current_chain:
            current_chain = suggestion.chain_name
            lines.append(f"--- *{escape_markdown(current_chain)}* ---")

        name = escape_markdown(suggestion.display_name)
        token = escape_markdown(suggestion.token_symbol)
        entry_lines = [
            f"**[{name} ({token})]({suggestion.vault_url})**",
            f"  Vault APY: {suggestion.base_apr:.2f}% (+{suggestion.apr_difference:.2f}%)",
            f"  TVL: {format_tvl(suggestion.tvl)}",
        ]
        lines.append("\n".join(entry_lines))

    return lines
