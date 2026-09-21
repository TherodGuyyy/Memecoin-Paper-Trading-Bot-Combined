"""
Fetches live prices from DEXScreener for tokens you've paper-entered.
Same public API the memecoin bot already uses — no key needed.
"""

import logging
import requests

log = logging.getLogger("price_source")

DEXSCREENER_BASE = "https://api.dexscreener.com"


def get_current_price(token_address: str) -> float | None:
    """Returns the current USD price for a Solana token, or None if
    unavailable (e.g. no active pair, or a network hiccup)."""
    info = get_token_info(token_address)
    return info["price"] if info else None


def get_token_info(token_address: str) -> dict | None:
    """
    Returns {"price": float, "symbol": str} for a Solana token, or None
    if unavailable. Same underlying DEXScreener call as get_current_price
    — this just also pulls the symbol out of the same response, so
    callers that want both don't need two separate API calls.
    """
    url = f"{DEXSCREENER_BASE}/latest/dex/tokens/{token_address}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        pairs = [p for p in data.get("pairs", []) if p.get("chainId") == "solana"]
        if not pairs:
            return None
        best_pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0) or 0)
        price = best_pair.get("priceUsd")
        if price is None:
            return None
        symbol = best_pair.get("baseToken", {}).get("symbol") or "UNKNOWN"
        return {"price": float(price), "symbol": symbol}
    except (requests.RequestException, ValueError, TypeError) as e:
        log.warning("Failed to fetch token info for %s: %s", token_address, e)
        return None
