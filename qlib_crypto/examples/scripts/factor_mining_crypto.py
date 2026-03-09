"""Minimal factor mining entry for qlib-crypto.

Usage:
python qlib_crypto/examples/scripts/factor_mining_crypto.py \
  --provider_uri ~/.qlib/crypto_data --start 2020-01-01 --end 2024-12-31
"""

import fire
import qlib
import pandas as pd

from qlib.data import D


def run(provider_uri: str, start: str = "2020-01-01", end: str = "2024-12-31", instruments: str = "all"):
    qlib.init(provider_uri=provider_uri, region="crypto")
    feats = D.features(
        D.instruments(instruments),
        [
            "$close",
            "$volume",
            "Ref($close, 1)/$close - 1",
            "Std($close, 20)/$close",
            "$funding_rate",
            "$open_interest",
        ],
        start_time=start,
        end_time=end,
        freq="day",
    )
    if feats.empty:
        print("No features loaded. Check provider_uri and dumped fields.")
        return
    ic_proxy = feats.groupby(level=0)["Ref($close, 1)/$close - 1"].mean().describe()
    print("Feature snapshot:")
    print(feats.head())
    print("\nSimple return distribution:")
    print(ic_proxy)


if __name__ == "__main__":
    fire.Fire(run)
