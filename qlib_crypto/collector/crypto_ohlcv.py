"""Collector entrypoint aliases for qlib-crypto.

This module keeps names stable for future pluginization.
"""

from scripts.data_collector.crypto_ohlcv.collector import Run

__all__ = ["Run"]
