from decimal import Decimal
from typing import List

from ..report import ReportData, SuggestionEntry, format_tvl, ChainReport, VaultEntry
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
        chain_header = f"— **{escape_markdown(chain.chain_name)}** —"
        chain_header_cont = f"— **{escape_markdown(chain.chain_name)}** — (cont.)"
        lines.append(chain_header)
        for idx, entry in enumerate(chain.vaults, start=1):
            if idx > 1 and (idx - 1) % 10 == 0:
                lines.append("")
                lines.append(chain_header_cont)
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

            lines.append(f"**[{name} ({token})]({entry.vault_url})**{staked_indicator}")
            details = f"• Value: {_format_money(entry.vault_usd_value)} | Vault APY: {entry.vault_apr_percent:.2f}%"
            if entry.staked_status != "none" and entry.current_staking_apr_percent > 0:
                details += (
                    f" | Staking APY: {entry.current_staking_apr_percent:.2f}% "
                    f"({escape_markdown(entry.current_staking_apr_source)})"
                )
            lines.append(details)
            lines.append(
                f"• Yield: {entry.yield_7d:.2f}% [7d] ({_format_signed_money(entry.usd_change_7d)}), "
                f"{entry.yield_30d:.2f}% [30d] ({_format_signed_money(entry.usd_change_30d)})"
            )

        if chain.total_usd > 0:
            lines.append(f"💰 **Chain Total: {_format_money(chain.total_usd)}**")
            lines.append(
                f"📊 Avg Vault APY: {chain.avg_apr:.2f}% | "
                f"📈 Avg Yield: {chain.avg_yield_7d:.2f}% [7d] ({_format_signed_money(chain.total_usd_change_7d)}), "
                f"{chain.avg_yield_30d:.2f}% [30d] ({_format_signed_money(chain.total_usd_change_30d)})"
            )
        else:
            lines.append("*No holdings found on this chain.*")
        lines.append("")

    lines.append("— **Overall Portfolio** —")
    if report.overall.total_usd > 0:
        lines.append(f"💰 Total Value: {_format_money(report.overall.total_usd)}")
        lines.append(
            f"📊 Avg Vault APY: {report.overall.avg_apr:.2f}% | "
            f"📈 Avg Yield: {report.overall.avg_yield_7d:.2f}% [7d] ({_format_signed_money(report.overall.total_usd_change_7d)}), "
            f"{report.overall.avg_yield_30d:.2f}% [30d] ({_format_signed_money(report.overall.total_usd_change_30d)})"
        )
    else:
        lines.append("*No Yearn vault holdings found for the provided addresses.*")

    lines.append(f"*{escape_markdown(report.cache_note)}*")
    return lines


def _format_vault_lines(entry: VaultEntry) -> List[str]:
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

    lines = [f"**[{name} ({token})]({entry.vault_url})**{staked_indicator}"]
    details = f"• Value: {_format_money(entry.vault_usd_value)} | Vault APY: {entry.vault_apr_percent:.2f}%"
    if entry.staked_status != "none" and entry.current_staking_apr_percent > 0:
        details += (
            f" | Staking APY: {entry.current_staking_apr_percent:.2f}% "
            f"({escape_markdown(entry.current_staking_apr_source)})"
        )
    lines.append(details)
    lines.append(
        f"• Yield: {entry.yield_7d:.2f}% [7d] ({_format_signed_money(entry.usd_change_7d)}), "
        f"{entry.yield_30d:.2f}% [30d] ({_format_signed_money(entry.usd_change_30d)})"
    )
    return lines


def _format_chain_total(chain: ChainReport) -> List[str]:
    if chain.total_usd <= 0:
        return ["*No holdings found on this chain.*"]
    return [
        f"💰 **Chain Total: {_format_money(chain.total_usd)}**",
        (
            f"📊 Avg Vault APY: {chain.avg_apr:.2f}% | "
            f"📈 Avg Yield: {chain.avg_yield_7d:.2f}% [7d] ({_format_signed_money(chain.total_usd_change_7d)}), "
            f"{chain.avg_yield_30d:.2f}% [30d] ({_format_signed_money(chain.total_usd_change_30d)})"
        ),
    ]


def render_chain_sections(report: ReportData, config: Config, vaults_per_chunk: int = 10) -> List[List[str]]:
    sections: List[List[str]] = []
    header_prefix = "— **{name}** —"
    for chain in report.chains:
        chain_name = escape_markdown(chain.chain_name)
        header = header_prefix.format(name=chain_name)
        header_cont = header_prefix.format(name=chain_name) + " (cont.)"
        vaults = chain.vaults
        if not vaults:
            chunk_lines = [header] + _format_chain_total(chain)
            sections.append(chunk_lines)
            continue

        for idx in range(0, len(vaults), vaults_per_chunk):
            chunk = vaults[idx : idx + vaults_per_chunk]
            chunk_lines = [header if idx == 0 else header_cont]
            for entry in chunk:
                chunk_lines.extend(_format_vault_lines(entry))
            if idx + vaults_per_chunk >= len(vaults):
                chunk_lines.extend(_format_chain_total(chain))
            sections.append(chunk_lines)

    return sections


def render_overall_section(report: ReportData) -> List[str]:
    lines = ["— **Overall Portfolio** —"]
    if report.overall.total_usd > 0:
        lines.append(f"💰 Total Value: {_format_money(report.overall.total_usd)}")
        lines.append(
            f"📊 Avg Vault APY: {report.overall.avg_apr:.2f}% | "
            f"📈 Avg Yield: {report.overall.avg_yield_7d:.2f}% [7d] ({_format_signed_money(report.overall.total_usd_change_7d)}), "
            f"{report.overall.avg_yield_30d:.2f}% [30d] ({_format_signed_money(report.overall.total_usd_change_30d)})"
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
            if len(lines) > 1:
                lines.append("")
            lines.append(f"— *{escape_markdown(current_chain)}* —")

        name = escape_markdown(suggestion.display_name)
        token = escape_markdown(suggestion.token_symbol)
        lines.append(f"**[{name} ({token})]({suggestion.vault_url})**")
        lines.append(
            f"• Vault APY: {suggestion.base_apr:.2f}% (+{suggestion.apr_difference:.2f}%) | "
            f"TVL: {format_tvl(suggestion.tvl)}"
        )

    return lines
