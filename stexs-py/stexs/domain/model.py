from dataclasses import dataclass, field
from dataclasses import replace as dataclass_replace
from typing import List, Dict
import time
import copy
import uuid

from stexs.services.logger import log # TODO Remove service dependency

@dataclass
class Stock:
    symbol: str
    name: str

    @property
    def stexid(self):
        return self.symbol


@dataclass
class Trade:
    tid: str
    ts: int
    symbol: str
    buy_txid: str
    avg_price: float
    total_price: float
    volume: int
    closed: bool = False
    excess: int = 0 # TODO CRIT Cheeky way of keeping size of excess last sell
    sell_txids: List[str] = field(default_factory = list)

    @staticmethod
    def propose_trade(filled_buy: "Order", filled_sells: "Order", excess: int):
        # Calculate average price of fulfilled buy
        tot_price = 0
        sell_txids = []
        for i_sell, sell in enumerate(filled_sells):
            sell_txids.append(sell.txid)

            if i_sell == len(filled_sells)-1:
                tot_price += (sell.price * (sell.volume - excess))
            else:
                tot_price += (sell.price * sell.volume)

        return Trade(
            tid=str(uuid.uuid4())[:5],
            symbol=filled_buy.symbol,
            volume=filled_buy.volume,
            buy_txid=filled_buy.txid,
            sell_txids=sell_txids,
            avg_price=tot_price/filled_buy.volume, # TODO Think this needs to buy at max buy, not buy at min sell
            total_price=tot_price,
            excess=excess,
            closed=False,
            ts=0,
        )

    def clear_trade(self):
        self.closed = True
        self.ts = int(time.time())

@dataclass
class MarketStall:
    stock: Stock
    last_price: float = None
    min_price: float = None
    max_price: float = None
    n_trades: int = 0
    v_trades: float = 0
    order_history: List[object] = field(default_factory = list)

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


