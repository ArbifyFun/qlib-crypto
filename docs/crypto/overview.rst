Crypto Support Overview
=======================

Architecture
------------
* Data is fetched from exchanges through ``ccxt`` (currently using the OKX API).
* Raw responses are converted to Qlib format with the collector scripts in ``scripts/data_collector/crypto``.
* Once prepared, datasets (including OHLCV fields) can be loaded through the standard ``qlib.data`` interface for research.

Backtesting
-----------
* OHLC data is now supported, enabling backtests through Qlib's workflow.
* Use ``CryptoExchange`` with ``SimulatorExecutor`` or the template in ``examples/crypto/backtest_config.yaml`` to evaluate strategies on 24/7 markets.

Instrument List
---------------
``CryptoInstrumentProvider`` requires a list of tradable symbols.  By default,
this list is loaded from
``~/.qlib/qlib_data/crypto_data/instruments.json``.  You can override the
location by passing ``instrument_provider={'kwargs': {'source': 'PATH'}}`` to
``qlib.init``.

Installation
------------
1. Install Qlib (``pip install pyqlib``).
2. Install extra dependencies:

   .. code-block:: bash

      pip install ccxt loguru fire requests numpy pandas tqdm lxml

Known Issues
------------
* Public APIs may enforce rate limits and sometimes return incomplete data.
* Example datasets generated from the OKX API may have limited history and require additional cleaning before production use.
* Qlib does **not** guarantee the accuracy or availability of third-party data. Prepare and verify your own dataset before trading.

For a usage example, see ``scripts/data_collector/crypto/README.md``.
