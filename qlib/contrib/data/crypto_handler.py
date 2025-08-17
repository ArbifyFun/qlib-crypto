# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""Data handler for cryptocurrency datasets.

This handler provides a simple default feature set for cryptocurrency
market data. It supports both daily and intraday frequencies and assumes
24/7 trading calendars.
"""

from qlib.contrib.data.handler import (
    DataHandlerLP as BaseHandler,
    _DEFAULT_INFER_PROCESSORS,
    _DEFAULT_LEARN_PROCESSORS,
    check_transform_proc,
)


class CryptoHandler(BaseHandler):
    """Default crypto data handler.

    Parameters
    ----------
    instruments : str or list-like, optional
        Trading symbols to load.
    freq : str, optional
        Data frequency, e.g., ``"day"`` or ``"1min"``.
    infer_processors, learn_processors : list, optional
        Processor configs for inference and training.
    """

    def __init__(
        self,
        instruments="BTC",
        start_time=None,
        end_time=None,
        freq="day",
        infer_processors=_DEFAULT_INFER_PROCESSORS,
        learn_processors=_DEFAULT_LEARN_PROCESSORS,
        fit_start_time=None,
        fit_end_time=None,
        filter_pipe=None,
        inst_processors=None,
        **kwargs,
    ):
        infer_processors = check_transform_proc(infer_processors, fit_start_time, fit_end_time)
        learn_processors = check_transform_proc(learn_processors, fit_start_time, fit_end_time)

        data_loader = {
            "class": "QlibDataLoader",
            "kwargs": {
                "config": {
                    "feature": self.get_feature_config(),
                    "label": kwargs.pop("label", self.get_label_config()),
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
            **kwargs,
        )

    def get_feature_config(self):
        """Return default crypto feature expressions."""
        fields = [
            "$close/Ref($close, -1) - 1",  # simple return
            "$volume",  # raw volume as indicator
        ]
        names = ["RET", "VOLUME"]
        return fields, names

    def get_label_config(self):
        return ["Ref($close, -2)/Ref($close, -1) - 1"], ["LABEL0"]
