import qlib
from qlib.utils import init_instance_by_config, flatten_dict
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, SigAnaRecord, PortAnaRecord


if __name__ == "__main__":
    # initialize qlib with crypto region
    qlib.init(region="crypto")

    # model config
    model_config = {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
    }

    # dataset config using CryptoDatasetHandler
    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "CryptoDatasetHandler",
                "module_path": "qlib.contrib.data",
                "kwargs": {
                    "instruments": "BTC",
                    "start_time": "2020-01-01",
                    "end_time": "2022-12-31",
                    "freq": "day",
                },
            },
            "segments": {
                "train": ("2020-01-01", "2020-12-31"),
                "valid": ("2021-01-01", "2021-06-30"),
                "test": ("2021-07-01", "2021-12-31"),
            },
        },
    }

    model = init_instance_by_config(model_config)
    dataset = init_instance_by_config(dataset_config)

    # optional: preview prepared data
    example_df = dataset.prepare("train")
    print(example_df.head())

    port_analysis_config = {
        "executor": {
            "class": "SimulatorExecutor",
            "module_path": "qlib.backtest.executor",
            "kwargs": {
                "time_per_step": "day",
                "generate_portfolio_metrics": True,
            },
        },
        "strategy": {
            "class": "TopkDropoutStrategy",
            "module_path": "qlib.contrib.strategy.signal_strategy",
            "kwargs": {
                "signal": (model, dataset),
                "topk": 10,
                "n_drop": 1,
            },
        },
        "backtest": {
            "start_time": "2021-01-01",
            "end_time": "2021-12-31",
            "account": 100000,
            "benchmark": "BTC",
            "exchange_kwargs": {
                "freq": "day",
                "limit_threshold": 0.095,
                "deal_price": "close",
                "open_cost": 0.0005,
                "close_cost": 0.0015,
                "min_cost": 5,
            },
        },
    }

    with R.start(experiment_name="crypto_workflow"):
        R.log_params(**flatten_dict({"model": model_config, "dataset": dataset_config}))
        model.fit(dataset)
        recorder = R.get_recorder()

        sr = SignalRecord(model, dataset, recorder)
        sr.generate()

        sar = SigAnaRecord(recorder)
        sar.generate()

        par = PortAnaRecord(recorder, port_analysis_config, "day")
        par.generate()
