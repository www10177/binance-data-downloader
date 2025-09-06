from enum import Enum


class Binance(str, Enum):
    UM = "um"

    def get_base_url(self):
        match self:
            case Binance.UM:
                return "https://data.binance.vision/data/futures/um/daily"
            case _:
                raise ValueError(f"Unsupported Binance type: {self}")
