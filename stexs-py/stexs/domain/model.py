from dataclasses import dataclass, field, replace
from dataclasses import replace as dataclass_replace
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
    excess: int = 0 # TODO CRIT Cheeky way of keeping size of excess last sell
    sell_txids: List[str] = field(default_factory = List)

    @staticmethod
    def propose_trade(filled_buy, filled_sells, excess):
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
            symbol=filled_buy.symbol,
            volume=filled_buy.volume,
            buy_txid=filled_buy.txid,
            sell_txids=sell_txids,
            avg_price=tot_price/filled_buy.volume,
            total_price=tot_price,
            excess=excess,
            closed=False,
        )

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

    @staticmethod
    def split_sell(filled_sell, excess_volume: int):

        if filled_sell.side != "SELL":
            raise Exception("Cannot split non-sell.")
        if excess_volume >= filled_sell.volume:
            raise Exception("Cannot split sell for same or greater volume.")
        if excess_volume <= 0:
            raise Exception("Cannot split sell without excess volume.")

        filled_sell.volume -= excess_volume

        # Fiddle the txid so we know it is a split
        if '/' in filled_sell.txid:
            parent, split = filled_sell.txid.split('/')
            split_num = int(split)+1
        else:
            parent = filled_sell.txid
            split_num = 1
        new_txid = '%s/%d' % (parent, split_num)

        remainder_sell = dataclass_replace(filled_sell, txid=new_txid, volume=excess_volume, closed=False)
        return filled_sell, remainder_sell

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


