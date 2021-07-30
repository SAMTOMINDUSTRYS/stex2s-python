from stexs.domain import model
from stexs.services.logger import log
from stexs.services import orderbook
import stexs.io.persistence as iop
from typing import List, Dict
import time

class Exchange:

    def __init__(self, *args, **kwargs):
        self.txid_set = set([]) # set = field(default_factory=set)
        self.stalls = {} # Dict[str, model.MarketStall] = field(default_factory = dict)

        # TODO Little hack for now
        self.stock_uow = iop.stock.StockSqliteUoW
        self.user_uow = iop.user.MemoryClientUoW

    def add_stocks(self, stocks: List[model.Stock]):
        with self.stock_uow() as uow:
            for stock in stocks:
                uow.stocks.add(stock)
                uow.commit()
                self.stalls[stock.symbol] = model.MarketStall(stock=stock)

    def add_users(self, clients: List[model.Client]):
        with self.user_uow() as uow:
            for client in clients:
                uow.users.add(client)
            uow.commit()

    def validate_preorder(self, user, order):
        if order.side == "BUY":
            if order.price * order.volume > user.balance:
                raise Exception("Insufficient balance")
        elif order.side == "SELL":
            if order.symbol not in user.holdings:
                raise Exception("Insufficient holding")
            else:
                if user.holdings[order.symbol] < order.volume:
                    raise Exception("Insufficient holding")

    def update_users(self, buy_orders, sell_orders, executed=False):
        if not executed:
            for order in buy_orders:
                self.adjust_balance(order.csid, order.price * order.volume * -1)
            for order in sell_orders:
                self.adjust_holding(order.csid, order.symbol, order.volume * -1)
        else:
            for order in buy_orders:
                self.adjust_holding(order.csid, order.symbol, order.volume)
            for order in sell_orders:
                # CRIT TODO Check this works with splits
                self.adjust_balance(order.csid, order.price * order.volume)

    def adjust_balance(self, csid, adjust_balance):
        with self.user_uow() as uow:
            user = uow.users.get(csid)
            user.balance += adjust_balance
            uow.commit()

        log.info("[bold magenta]USER[/] [b]CASH[/] %s=%.3f" % (csid, user.balance))

    def adjust_holding(self, csid, symbol, adjust_qty):
        with self.stock_uow() as uow:
            # Try out cached list
            stock_list = uow.stocks.list()
            if not symbol in stock_list:
                raise Exception("Unknown symbol")
            stock = uow.stocks.get(symbol)

        with self.user_uow() as uow:
            user = uow.users.get(csid)
            if symbol not in user.holdings:
                user.holdings[symbol] = 0
            user.holdings[symbol] += adjust_qty
            uow.commit()

        log.info("[bold magenta]USER[/] [b]HOLD[/] %s:%s=%.3f" % (csid, symbol, user.holdings[symbol]))

    def recv(self, msg):
        if msg["txid"] in self.txid_set:
            raise Exception("Duplicate transaction")
        with self.user_uow() as uow:
            user = uow.users.get(msg["csid"])
            if not user:
                raise Exception("Unknown user")

        order = model.Order(
            txid=msg["txid"],
            csid=msg["csid"],
            side=msg["side"],
            symbol=msg["symbol"],
            price=msg["price"],
            volume=msg["volume"],
            ts=int(time.time()),
        )
        with self.stock_uow() as uow:
            stock = uow.stocks.get(order.symbol)
        if not stock:
            raise Exception("Unknown symbol")

        # Check this order can be completed before processing it
        try:
            # Good transaction isolation is going to be needed to ensure balance
            # and holdings stay positive in the event of concurrent order handlers
            with self.user_uow() as uow:
                user = uow.users.get(order.csid)
                if not user:
                    raise Exception("Unknown user")
            self.validate_preorder(user, order)
        except Exception as e:
            raise e

        # Process order
        buys, sells = orderbook.add_order(order.symbol, order)
        self.update_users(buys, sells)

        summary = orderbook.summarise_books(order.symbol)
        log.info("[bold green]BOOK[/] [b]%s[/] %s" % (order.symbol, str(summary)))

        # Need to handle the market tick async from messages but this will do for now
        buys, sells, trades = orderbook.match_orderbook(order.symbol)
        for trade in trades:
            self.stalls[order.symbol].log_trade(trade)
        summary = orderbook.summarise_books(order.symbol)
        log.info("[bold green]BOOK[/] [b]%s[/] %s" % (order.symbol, str(summary)))

        buy_str, sell_str = orderbook.summarise_orderbook(order.symbol)
        log.info(buy_str)
        log.info(sell_str)

        self.update_users(buys, sells, executed=True)

        # Idempotent txid
        self.txid_set.add(msg["txid"])
