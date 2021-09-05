from .broker_exception import (
    InsufficientBalanceException,
    InsufficientHoldingException,
)
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

    def screen_order(self, side, symbol, price, volume):
        if side == "BUY":
            if price * volume > self.balance:
                raise InsufficientBalanceException("Insufficient balance")
        elif side == "SELL":
            if symbol not in self.holdings:
                raise InsufficientHoldingException("No holding")
            else:
                if self.holdings[symbol] < volume:
                    raise InsufficientHoldingException("Insufficient holding")
        return True
    
