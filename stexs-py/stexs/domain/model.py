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
class OrderBook:
    stock: Stock # does this make sense?
    dbuys: int = 0
    nbuys: int = 0
    vbuys: int = 0
    buy: float = 0
    sell: float = 0
    dsells: int = 0
    nsells: int = 0
    vsells: int = 0

    buy_book = []
    sell_book = []

    def add_order(self, order):
        if order.side == "BUY":
            self._add_buy(order)
        elif order.side == "SELL":
            self._add_sell(order)
        self.summarise_books()
        log.info("[bold white]ORDR[/] [b]%s[/] %s" % (self.stock.symbol, order))

    def _add_buy(self, order):
        self.buy_book.append(order)
        self.buy_book = self._balance_book(self.buy_book, buy=True)

    def _add_sell(self, order):
        self.sell_book.append(order)
        self.sell_book = self._balance_book(self.sell_book, sell=True)

    def balance_books(self):
        self.buy_book = self._balance_book(self.buy_book, buy=True)
        self.sell_book = self._balance_book(self.sell_book, sell=True)

    def _balance_book(self, book, buy=False, sell=False):
        if not buy and not sell:
            raise Exception("Cannot sort book: must sort by `buy` or `sell`")
        if buy and sell:
            raise Exception("Cannot sort book: cannot sort by `buy` and `sell`")

        book = [order for order in book if not order.closed]
        if buy:
            book = sorted(book, key=lambda order: (-order.price, order.ts, order.txid))
        else:
            book = sorted(book, key=lambda order: (order.price, order.ts, order.txid))

        return book

    def match_one(self):
        done = False

        new_orders = []

        for buy in self.buy_book: 
            if buy.closed:
                # Abort on closed buy
                break

            buy_filled = False
            buy_sells = []
            curr_volume = 0

            buy_price = buy.price
            buy_volume = buy.volume

            for sell in self.sell_book:
                if sell.closed:
                    # Skip sold sell
                    continue

                sell_price = sell.price
                sell_volume = sell.volume

                # If the buy match or exceeds the sell price, we can trade
                if buy_price >= sell_price:
                    curr_volume += sell_volume
                    buy_sells.append(sell)

                    if curr_volume >= buy_volume:
                        # Either volume is just right or there is some excess to split into new Order
                        buy_filled = True
                    else:
                        # Not enough volume, keep iterating Orders
                        continue

                    if buy_filled:
                        #self.trade(buy=buy, sells=buy_sells, excess=curr_volume-buy_volume)
                        new_orders.append({
                            "buy": buy,
                            "sells": buy_sells,
                            "excess": curr_volume - buy_volume,
                        })
                        done = True # Force update before running match again
                        break # Don't keep trying to add sells to this buy!

                else:
                    # Sells are sorted, so if we have not filled this buy there
                    # are no more sells at the right price range
                    done = True
                    break

            if done:
                break
        return new_orders

    def current_buy(self):
        try:
            return self.buy_book[0].price
        except:
            return 0

    def current_sell(self):
        try:
            return self.sell_book[0].price
        except:
            return 0

    def update_summary(self, summary):
        self.dbuys = summary["dbuys"]
        self.nbuys = summary["nbuys"]
        self.vbuys = summary["vbuys"]
        self.buy = summary["buy"]
        self.sell = summary["sell"]
        self.dsells = summary["dsells"]
        self.nsells = summary["nsells"]
        self.vsells = summary["vsells"]

    def summarise_books(self):
        buy = self.current_buy()
        sell = self.current_sell()

        dbuys = dsells = 0
        nbuys = nsells = 0
        vbuys = vsells = 0

        for order in self.buy_book:
            if order.closed:
                continue
            dbuys +=1

            if order.price == buy:
                nbuys += 1
                vbuys += order.volume

        for order in self.sell_book:
            if order.closed:
                continue
            dsells +=1

            if order.price == sell:
                nsells += 1
                vsells += order.volume

        return {
            "ts": int(time.time()),

            "dbuys": dbuys,
            "dsells": dsells,

            "nbuys": nbuys,
            "nsells": nsells,

            "vbuys": vbuys,
            "vsells": vsells,

            "buy": buy,
            "sell": sell,
        }

@dataclass
class MarketStall:
    stock: Stock
    last_price: float = None
    min_price: float = None
    max_price: float = None
    n_trades: int = 0
    v_trades: float = 0
    order_history = []

    def __post_init__(self):
        self.orderbook = OrderBook(stock=self.stock)

    def __rich__(self):
        return ' '.join([
            "[b]%s[/]" % self.stock.symbol,
            "[b]NOW[/] %.3f" % self.last_price,
            "[b]MIN[/] %.3f" % self.min_price,
            "[b]MAX[/] %.3f" % self.max_price,
            "[b]NUM[/] %04d" % self.n_trades,
            "[b]VOL[/] %04d" % self.v_trades,
        ])

    def add_order(self, order):
        self.orderbook.add_order(order)

        # Update book summary
        summary = self.orderbook.summarise_books()
        self.orderbook.update_summary(summary)
        log.info("[bold green]BOOK[/] [b]%s[/] %s" % (self.stock.symbol, str(summary)))


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


    def execute_trade(self, buy, sells, excess=0):

        # Close buy and sells
        buy.closed = True
        for sell in sells:
            sell.closed = True

        # Price
        tot_price = 0
        sell_txids = []
        for i_sell, sell in enumerate(sells):
            sell_txids.append(sell.txid)

            if i_sell == len(sells)-1:
                tot_price += (sell.price * (sell.volume - excess))
            else:
                tot_price += (sell.price * sell.volume)

        # Record Trade
        trade = Trade(
            symbol=buy.symbol,
            volume=buy.volume,
            buy_txid=buy.txid,
            sell_txids=sell_txids,
            avg_price=tot_price/buy.volume,
            total_price=tot_price,
            closed=True,
        )

        # Finally, if Sell volume exceeded requirement, split the final sell into a new Order
        if excess > 0:
            last_sell = sells[-1]
            last_sell.volume -= excess

            # new_sell.ts does not get updated
            new_sell = copy.copy(last_sell)
            new_sell.closed = False
            new_sell.volume = excess

            # Fiddle the txid so we know it is a split
            if '/' in new_sell.txid:
                split_num = int(new_sell.txid.split('/')[1])+1
            else:
                split_num = 1
            new_sell.txid += '/%d' % split_num
            self.orderbook.add_order(new_sell)

        return trade


    def handle_order(self, msg):
        buys = []
        sells = []

        # Assume the order goes through with no trouble
        self.add_order(msg)
        # and hackily add the order to the actions to be bubbled up to STEX
        if msg.side == "BUY":
            buys.append(msg)
        elif msg.side == "SELL":
            sells.append(msg)

        return buys, sells


    def handle_market(self):
        # Trigger match process
        # TODO Needs to be done somewhere else
        #       Will need to think about how this can be thread-safe if we move
        #       matching/trading to a separate thread from adding orders
        fulfilled_orders = self.orderbook.match_one()


        # If we have to return here to execute orders, we can only match once
        buys = []
        sells = []
        trades = []
        for order in fulfilled_orders:
            trade = self.execute_trade(order["buy"], order["sells"], excess=order["excess"])
            trades.append(trade)
            self.log_trade(trade)
            log.info(trade)

            buys.append(order["buy"])
            sells.extend(order["sells"])

        # some gross logging for now
        self.orderbook.balance_books()
        buy_str = []
        for order in self.orderbook.buy_book:
            buy_str.append("%s#%d@%.3f" % (order.txid, order.volume, order.price))
        log.info(buy_str)
        sell_str = []
        for order in self.orderbook.sell_book:
            sell_str.append("%s#%d@%.3f" % (order.txid, order.volume, order.price))
        log.info(sell_str)

        # Sneaky way of getting actions back up to the Exchange, we'll obviously
        # want to do this async, and not in the order handler
        return buys, sells, trades

