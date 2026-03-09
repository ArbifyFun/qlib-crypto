from qlib.contrib.data.handler import _DEFAULT_LEARN_PROCESSORS, check_transform_proc
from qlib.data.dataset.handler import DataHandlerLP


class Alpha158Crypto(DataHandlerLP):
    """Crypto-oriented handler for spot/perp factor mining.

    Keep the same DataHandlerLP interface as Alpha158 while exposing crypto fields.
    """

    def __init__(
        self,
        instruments="all",
        start_time=None,
        end_time=None,
        freq="day",
        infer_processors=None,
        learn_processors=_DEFAULT_LEARN_PROCESSORS,
        fit_start_time=None,
        fit_end_time=None,
        process_type=DataHandlerLP.PTYPE_A,
        filter_pipe=None,
        inst_processors=None,
        label_mode="spot",
        **kwargs,
    ):
        infer_processors = [] if infer_processors is None else infer_processors
        infer_processors = check_transform_proc(infer_processors, fit_start_time, fit_end_time)
        learn_processors = check_transform_proc(learn_processors, fit_start_time, fit_end_time)

        data_loader = {
            "class": "QlibDataLoader",
            "kwargs": {
                "config": {
                    "feature": self.get_feature_config(),
                    "label": kwargs.pop("label", self.get_label_config(label_mode=label_mode)),
                },
                "filter_pipe": filter_pipe,
                "freq": freq,
                "inst_processors": inst_processors,
            },
        }

        super().__init__(
            instruments=instruments,
            start_time=start_time,
            end_time=end_time,
            data_loader=data_loader,
            infer_processors=infer_processors,
            learn_processors=learn_processors,
            process_type=process_type,
            **kwargs,
        )

    def get_feature_config(self):
        fields = [
            "$open",
            "$high",
            "$low",
            "$close",
            "$volume",
            "$vwap",
            "Ref($close, 1)/$close - 1",
            "Ref($close, 5)/$close - 1",
            "Ref($close, 20)/$close - 1",
            "Std($close, 5)/$close",
            "Std($close, 20)/$close",
            "Mean($money, 5)",
            "Mean($volume, 5)",
            "$funding_rate",
            "$open_interest",
            "$basis",
        ]
        names = [
            "OPEN",
            "HIGH",
            "LOW",
            "CLOSE",
            "VOLUME",
            "VWAP",
            "RET_1",
            "RET_5",
            "RET_20",
            "RV_5",
            "RV_20",
            "TURNOVER_USD_5",
            "VOLUME_MA_5",
            "FUNDING",
            "OPEN_INTEREST",
            "BASIS",
        ]
        return fields, names

    def get_label_config(self, label_mode: str = "spot"):
        if label_mode == "spot":
            return ["Ref($close, -2)/Ref($close, -1) - 1"], ["LABEL0"]
        if label_mode == "perp_funding_adj":
            return ["Ref($close, -2)/Ref($close, -1) - 1 - Ref($funding_rate, -1)"], ["LABEL0"]
        raise ValueError(f"unsupported label_mode: {label_mode}")
