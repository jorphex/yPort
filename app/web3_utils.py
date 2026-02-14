import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from web3 import Web3
from ens import ENS

from .chains import CHAIN_TO_ALCHEMY_PREFIX, CHAIN_TO_RPC_URL

logger = logging.getLogger(__name__)

@dataclass
class Web3Manager:
    api_key: str

    def __post_init__(self) -> None:
        self._instances: dict[int, Web3] = {}
        self._ens: Optional[ENS] = None

    def get_instance(self, chain_id: int) -> Optional[Web3]:
        if chain_id in self._instances:
            return self._instances[chain_id]

        prefix = CHAIN_TO_ALCHEMY_PREFIX.get(chain_id)
        rpc_url = None
        if prefix and self.api_key:
            rpc_url = f"https://{prefix}.g.alchemy.com/v2/{self.api_key}"
        else:
            rpc_url = CHAIN_TO_RPC_URL.get(chain_id)

        if not rpc_url:
            logger.warning("No RPC URL configured for chain %s", chain_id)
            return None

        try:
            w3 = Web3(Web3.HTTPProvider(rpc_url))
            if w3.is_connected():
                self._instances[chain_id] = w3
                logger.info("Initialized Web3 for chain %s", chain_id)
                return w3
            logger.error("Failed to connect Web3 for chain %s", chain_id)
        except Exception as exc:
            logger.error("Error initializing Web3 for chain %s: %s", chain_id, exc)
        return None

    def init_ens(self) -> None:
        if self._ens is not None:
            return
        w3 = self.get_instance(1)
        if not w3:
            logger.error("Ethereum Web3 not available; ENS disabled")
            self._ens = None
            return
        try:
            self._ens = ENS.from_web3(w3)
            logger.info("ENS resolver initialized")
        except Exception as exc:
            logger.error("Failed to initialize ENS: %s", exc)
            self._ens = None

    async def resolve_ens(self, name: str) -> Optional[str]:
        if self._ens is None:
            self.init_ens()
        if self._ens is None:
            return None
        try:
            loop = asyncio.get_running_loop()
            address = await loop.run_in_executor(None, self._ens.address, name)
            return address
        except Exception as exc:
            logger.error("ENS resolution failed for %s: %s", name, exc)
            return None
