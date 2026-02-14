CHAIN_NAMES = {
    1: "Ethereum",
    10: "Optimism",
    137: "Polygon",
    42161: "Arbitrum",
    8453: "Base",
    747474: "Katana",
}

CHAIN_NAME_TO_ID = {name.lower(): chain_id for chain_id, name in CHAIN_NAMES.items()}

CHAIN_TO_ALCHEMY_PREFIX = {
    1: "eth-mainnet",
    10: "opt-mainnet",
    137: "polygon-mainnet",
    42161: "arb-mainnet",
    8453: "base-mainnet",
}

CHAIN_TO_RPC_URL = {
    747474: "https://rpc.katana.network",
}

SUPPORTED_CHAINS = list(CHAIN_TO_ALCHEMY_PREFIX.keys()) + list(CHAIN_TO_RPC_URL.keys())
