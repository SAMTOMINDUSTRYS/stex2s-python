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
    def propose_trade(filled_buy: "Order", filled_sells: "Order", excess: int = 0, execution_price: float = None):
        # Calculate average price of fulfilled buy
        tot_price = 0
        sell_txids = []
        for i_sell, sell in enumerate(filled_sells):
            sell_txids.append(sell.txid)

            if i_sell == len(filled_sells)-1:
                tot_price += (sell.price * (sell.volume - excess))
            else:
                tot_price += (sell.price * sell.volume)

        if execution_price:
            avg_price = execution_price
            tot_price = execution_price * filled_buy.volume

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

    @staticmethod
    def get_execution_price(buy_ts, sell_ts, buy_price, sell_price, reference_price, highest_bid, lowest_ask):
        if buy_ts > sell_ts:
            is_buying = True
            is_selling = False
            incoming_order_price, book_order_price = buy_price, sell_price
        else:
            is_buying = False
            is_selling = True
            incoming_order_price, book_order_price = sell_price, buy_price

        price = None

        if not highest_bid and not lowest_ask:
            # EX1
            # If we can match without a highest_bid or lowest_ask then these are both
            # market orders and no other information is available to set a price
            price = reference_price

        elif book_order_price == float("inf") or book_order_price == float("-inf"):
            # Market or limit order meeting a market order

            if not highest_bid:
                highest_bid = reference_price
            if not lowest_ask:
                lowest_ask = reference_price

            if is_selling:
                # EX16, EX17, EX18 (Mixed market and limit)
                # EX9, EX10 (Limit meets only market orders so highest_bid unset)
                # EX4, EX5 (Market order ask order meets market or limit so lowest_ask unset)
                # Sell at highest price
                # Market or limit order meets market only, or mixed book
                price = max(reference_price, highest_bid, lowest_ask)

            elif is_buying:
                # EX19, EX20, EX21 (Mixed market and limit)
                # EX11, EX12 (Limit meets only market orders so lowest_ask unset)
                # EX6, EX7 (Market only bid order meets market or limit so highest_bid unset)
                # Buy at lowest price
                # Market or limit order meets market only, or mixed book
                price = min(reference_price, highest_bid, lowest_ask)

        else:
            # Market or limit order meeting only limit orders
            if is_selling:
                # EX2, EX13
                price = highest_bid
            elif is_buying:
                # EX3, EX14
                price = lowest_ask

        return price

    def clear_trade(self):
        self.closed = True
        self.ts = int(time.time())

@dataclass
class MarketStall:
    stock: Stock
    last_price: float = 1.0 # TODO CRIT Need to load in or otherwise set the last_price (its never None IRL)
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


