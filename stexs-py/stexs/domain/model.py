from dataclasses import dataclass, field
from typing import List, Dict
import time
import copy

from stexs.services.logger import log

@dataclass
class Stock:
    symbol: str
    name: str

    @property
    def stexid(self):
        return self.symbol

@dataclass
class Client:
    csid: str
    name: str
    balance: float = 0
    holdings: Dict[str, int] = field(default_factory = dict)

    @property
    def stexid(self):
        return self.csid

@dataclass
class Trade:
    symbol: str
    buy_txid: str
    avg_price: float
    total_price: float
    volume: int
    closed: bool = False
    sell_txids: List[str] = field(default_factory = List)

@dataclass
class Order:
    txid: str
    csid: str
    ts: int
    side: str
    symbol: str
    price: float
    volume: int
    closed: bool = False

    @property
    def stexid(self):
        return self.txid

@dataclass
class MarketStall:
    stock: Stock
    last_price: float = None
    min_price: float = None
    max_price: float = None
    n_trades: int = 0
    v_trades: float = 0
    order_history = []

    def __rich__(self):
        return ' '.join([
            "[b]%s[/]" % self.stock.symbol,
            "[b]NOW[/] %.3f" % self.last_price,
            "[b]MIN[/] %.3f" % self.min_price,
            "[b]MAX[/] %.3f" % self.max_price,
            "[b]NUM[/] %04d" % self.n_trades,
            "[b]VOL[/] %04d" % self.v_trades,
        ])

    def log_trade(self, trade):
        self.order_history.append(trade)

        # Update summary
        if not self.last_price:
            self.last_price = trade.avg_price

        if not self.min_price or not self.max_price:
            self.min_price = self.max_price = trade.avg_price

        self.last_price = trade.avg_price
        if self.last_price > self.max_price:
            self.max_price = self.last_price
        if self.last_price < self.min_price:
            self.min_price = self.last_price

        self.n_trades += 1
        self.v_trades += trade.volume
        log.info("[bold cyan]TRDE[/] " + self.__rich__())


