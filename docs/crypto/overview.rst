Crypto Support Overview
=======================

Architecture
------------
* Data is fetched from public cryptocurrency APIs via libraries such as `ccxt` or `pycoingecko`.
* Raw responses are converted to Qlib format with the collector scripts in ``scripts/data_collector/crypto``.
* Once prepared, datasets can be loaded through the standard ``qlib.data`` interface for research.

Installation
------------
1. Install Qlib (``pip install pyqlib``).
2. Install extra dependencies:

   .. code-block:: bash

      pip install ccxt loguru fire requests numpy pandas tqdm lxml pycoingecko

Known Issues
------------
* Public APIs may enforce rate limits and sometimes return incomplete data.
* The example dataset from Coingecko lacks OHLC information, so backtesting is not supported.
* Qlib does **not** guarantee the accuracy or availability of third-party data. Prepare and verify your own dataset before trading.

For a usage example, see ``scripts/data_collector/crypto/README.md``.
