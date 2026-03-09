from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass
class CryptoInstrument:
    symbol: str
    exchange: str
    market_type: str
    listed_at: str
    delisted_at: str = "2099-12-31"

    @property
    def qlib_symbol(self) -> str:
        norm = self.symbol.replace("-", "_")
        return f"{self.exchange.upper()}_{norm}"


def dump_instruments_all(instruments: Iterable[CryptoInstrument], output_path: str) -> Path:
    """Write qlib instruments/all.txt as: <symbol>\t<start>\t<end>."""
    output = Path(output_path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    rows: List[str] = []
    for inst in instruments:
        rows.append(f"{inst.qlib_symbol}\t{inst.listed_at}\t{inst.delisted_at}")
    output.write_text("\n".join(sorted(rows)) + "\n")
    return output
