import logging
from typing import Dict, List, Tuple

from web3 import Web3

from .web3_utils import Web3Manager

logger = logging.getLogger(__name__)

ENS_SUFFIXES = (".eth", ".xyz")

async def parse_addresses_input(text: str, web3_manager: Web3Manager) -> Tuple[List[str], List[str], Dict[str, str], bool]:
    tokens = text.split()
    valid: List[str] = []
    errors: List[str] = []
    ens_map: Dict[str, str] = {}
    had_candidates = False

    for token in tokens:
        token = token.strip()
        if not token:
            continue
        is_address_like = token.startswith("0x") and len(token) == 42
        is_ens_like = token.lower().endswith(ENS_SUFFIXES)

        if not (is_address_like or is_ens_like):
            continue

        had_candidates = True

        if Web3.is_address(token):
            checksum = Web3.to_checksum_address(token)
            valid.append(checksum)
            continue

        if is_ens_like:
            resolved = await web3_manager.resolve_ens(token)
            if resolved and Web3.is_address(resolved):
                checksum = Web3.to_checksum_address(resolved)
                valid.append(checksum)
                ens_map[checksum] = token
            else:
                errors.append(f"{token} (ENS not resolved)")
            continue

        errors.append(f"{token} (invalid address)")

    return valid, errors, ens_map, had_candidates
