"""Data contrib modules for qlib-crypto.

Avoid eager imports to keep this package usable without compiled qlib runtime.
"""

from importlib import import_module

__all__ = ["Alpha158Crypto", "StablecoinNeutralize"]


def __getattr__(name):
    if name == "Alpha158Crypto":
        return getattr(import_module("qlib_crypto.contrib.data.handler"), name)
    if name == "StablecoinNeutralize":
        return getattr(import_module("qlib_crypto.contrib.data.processor"), name)
    raise AttributeError(name)
