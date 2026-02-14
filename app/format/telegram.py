from decimal import Decimal
from typing import List

from ..report import ReportData, SuggestionEntry, format_tvl
from ..config import Config


def _escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _format_money(value: Decimal) -> str:
    return f"${value:,.2f}"


def _format_signed_money(value: Decimal) -> str:
    if value >= 0:
        return f"+${value:,.2f}"
    return f"${value:,.2f}"


def render_report(report: ReportData, config: Config) -> List[str]:
    lines: List[str] = ["<b>Your Yearn Portfolio Report</b>"]

    if report.has_yearn_gauge_deposit:
        notice = _escape_html(config.veyfi_deprecation_message)
        lines.append(f"<i>Notice: {notice}</i>")

    if report.empty:
        lines.append("<i>No Yearn vault holdings found for the provided addresses.</i>")
        lines.append(f"<i>{_escape_html(report.cache_note)}</i>")
        return lines

    for chain in report.chains:
        lines.append(f"--- <b>{_escape_html(chain.chain_name)}</b> ---")
        for entry in chain.vaults:
            name = _escape_html(entry.display_name)
            token = _escape_html(entry.token_symbol)
            staked_indicator = ""
            if entry.staked_status == "yearn":
                staked_indicator = " <i>(Staked: Yearn)</i>"
            elif entry.staked_status == "1up":
                if entry.staked_indicator_url:
                    staked_indicator = f" <i>(<a href='{entry.staked_indicator_url}'>Staked: 1UP</a>)</i>"
                else:
                    staked_indicator = " <i>(Staked: 1UP)</i>"

            staking_line = ""
            if entry.staked_status != "none" and entry.current_staking_apr_percent > 0:
                staking_line = f"\n  Staking APR: {entry.current_staking_apr_percent:.2f}% ({_escape_html(entry.current_staking_apr_source)})"

            lines.append(
                f"<b><a href='{entry.vault_url}'>{name} ({token})</a></b>{staked_indicator}\n"
                f"  Value: {_format_money(entry.vault_usd_value)}\n"
                f"  Vault APR: {entry.vault_apr_percent:.2f}%"
                f"{staking_line}"
                f"\n  Yield: {entry.yield_7d:.2f}% [7d] ({_format_signed_money(entry.usd_change_7d)}), {entry.yield_30d:.2f}% [30d] ({_format_signed_money(entry.usd_change_30d)})"
            )

        if chain.total_usd > 0:
            lines.append(
                "  ---\n"
                f"  <b>Chain Total: {_format_money(chain.total_usd)}</b>\n"
                f"  Avg Vault APR: {chain.avg_apr:.2f}%\n"
                f"  Avg Yield: {chain.avg_yield_7d:.2f}% [7d] ({_format_signed_money(chain.total_usd_change_7d)}), {chain.avg_yield_30d:.2f}% [30d] ({_format_signed_money(chain.total_usd_change_30d)})"
            )
        else:
            lines.append("  <i>No holdings found on this chain.</i>")

    lines.append("--- <b>Overall Portfolio</b> ---")
    if report.overall.total_usd > 0:
        lines.append(f"Total Value: {_format_money(report.overall.total_usd)}")
        lines.append(f"Avg Vault APR: {report.overall.avg_apr:.2f}%")
        lines.append(
            f"Avg Yield: {report.overall.avg_yield_7d:.2f}% [7d] ({_format_signed_money(report.overall.total_usd_change_7d)}), {report.overall.avg_yield_30d:.2f}% [30d] ({_format_signed_money(report.overall.total_usd_change_30d)})"
        )
    else:
        lines.append("<i>No Yearn vault holdings found for the provided addresses.</i>")

    lines.append(f"<i>{_escape_html(report.cache_note)}</i>")
    return lines


def render_suggestions(suggestions: List[SuggestionEntry]) -> List[str]:
    if not suggestions:
        return []

    lines: List[str] = ["<b>Vault Suggestions</b>"]
    current_chain = None

    for suggestion in suggestions:
        if suggestion.chain_name != current_chain:
            current_chain = suggestion.chain_name
            lines.append(f"--- <i>{_escape_html(current_chain)}</i> ---")

        name = _escape_html(suggestion.display_name)
        token = _escape_html(suggestion.token_symbol)
        entry_lines = [
            f"<b><a href='{suggestion.vault_url}'>{name} ({token})</a></b>",
            f"  Vault APY: {suggestion.base_apr:.2f}% (+{suggestion.apr_difference:.2f}%)",
            f"  TVL: {format_tvl(suggestion.tvl)}",
        ]
        lines.append("\n".join(entry_lines))

    return lines
