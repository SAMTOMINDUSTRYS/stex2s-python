from dataclasses import dataclass, field
from typing import List, Dict

@dataclass
class Client:
    csid: str
    name: str
    balance: float = 0
    holdings: Dict[str, int] = field(default_factory = dict)

    @property
    def stexid(self):
        return self.csid

    # TODO CRIT what about <0 balance
    def adjust_balance(self, balance_adjustment):
        self.balance += balance_adjustment

    # TODO CRIT what about <0 holdings
    def adjust_holding(self, symbol, adjust_qty):
        if symbol not in self.holdings:
            self.holdings[symbol] = 0
        self.holdings[symbol] += adjust_qty

