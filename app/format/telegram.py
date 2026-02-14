from decimal import Decimal
from typing import List

from ..report import ReportData, SuggestionEntry, format_tvl
from ..config import Config


def _format_money(value: Decimal) -> str:
    return f"${value:,.2f}"


def _format_signed_money(value: Decimal) -> str:
    if value >= 0:
        return f"+${value:,.2f}"
    return f"${value:,.2f}"


def render_report(report: ReportData, config: Config) -> List[str]:
    lines: List[str] = ["**Your Yearn Portfolio Report**"]

    if report.has_yearn_gauge_deposit:
        lines.append(f"*Notice: {config.veyfi_deprecation_message}*")

    if report.empty:
        lines.append("*No Yearn vault holdings found for the provided addresses.*")
        lines.append(f"*{report.cache_note}*")
        return lines

    for chain in report.chains:
        lines.append(f"--- **{chain.chain_name}** ---")
        for entry in chain.vaults:
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
                staking_line = (
                    f"\nStaking APR: {entry.current_staking_apr_percent:.2f}% "
                    f"({entry.current_staking_apr_source})"
                )

            lines.append(
                f"**[{entry.display_name} ({entry.token_symbol})]({entry.vault_url})**{staked_indicator}\n"
                f"Value: {_format_money(entry.vault_usd_value)}\n"
                f"Vault APR: {entry.vault_apr_percent:.2f}%"
                f"{staking_line}"
                f"\nYield: {entry.yield_7d:.2f}% [7d] ({_format_signed_money(entry.usd_change_7d)}), "
                f"{entry.yield_30d:.2f}% [30d] ({_format_signed_money(entry.usd_change_30d)})"
            )

        if chain.total_usd > 0:
            lines.append(
                "---\n"
                f"**Chain Total: {_format_money(chain.total_usd)}**\n"
                f"Avg Vault APR: {chain.avg_apr:.2f}%\n"
                f"Avg Yield: {chain.avg_yield_7d:.2f}% [7d] ({_format_signed_money(chain.total_usd_change_7d)}), "
                f"{chain.avg_yield_30d:.2f}% [30d] ({_format_signed_money(chain.total_usd_change_30d)})"
            )
        else:
            lines.append("*No holdings found on this chain.*")

    lines.append("--- **Overall Portfolio** ---")
    if report.overall.total_usd > 0:
        lines.append(f"Total Value: {_format_money(report.overall.total_usd)}")
        lines.append(f"Avg Vault APR: {report.overall.avg_apr:.2f}%")
        lines.append(
            f"Avg Yield: {report.overall.avg_yield_7d:.2f}% [7d] ({_format_signed_money(report.overall.total_usd_change_7d)}), "
            f"{report.overall.avg_yield_30d:.2f}% [30d] ({_format_signed_money(report.overall.total_usd_change_30d)})"
        )
    else:
        lines.append("*No Yearn vault holdings found for the provided addresses.*")

    lines.append(f"*{report.cache_note}*")
    return lines


def render_suggestions(suggestions: List[SuggestionEntry]) -> List[str]:
    if not suggestions:
        return []

    lines: List[str] = ["**Vault Suggestions**"]
    current_chain = None

    for suggestion in suggestions:
        if suggestion.chain_name != current_chain:
            current_chain = suggestion.chain_name
            lines.append(f"--- *{current_chain}* ---")

        entry_lines = [
            f"**[{suggestion.display_name} ({suggestion.token_symbol})]({suggestion.vault_url})**",
            f"Vault APY: {suggestion.base_apr:.2f}% (+{suggestion.apr_difference:.2f}%)",
            f"TVL: {format_tvl(suggestion.tvl)}",
        ]
        lines.append("\n".join(entry_lines))

    return lines
