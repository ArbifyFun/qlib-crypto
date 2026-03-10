from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolRegistry:
    """Utilities for exchange symbol normalization and qlib symbol namespace."""

    @staticmethod
    def to_qlib_symbol(exchange: str, symbol: str) -> str:
        return f"{exchange.upper()}_{symbol.replace('-', '_')}"

    @staticmethod
    def from_qlib_symbol(qlib_symbol: str) -> tuple[str, str]:
        ex, raw = qlib_symbol.split("_", 1)
        return ex.upper(), raw.replace("_", "-") if ex.upper() == "OKX" else raw
