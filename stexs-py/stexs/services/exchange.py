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
        self.brokers = {}

        # TODO Little hack for now
        self.stock_uow = iop.stock.StockSqliteUoW

    def add_stocks(self, stocks: List[model.Stock]):
        with self.stock_uow() as uow:
            for stock in stocks:
                uow.stocks.add(stock)
                uow.commit()
                self.stalls[stock.symbol] = model.MarketStall(stock=stock)

    def list_stocks(self):
        with self.stock_uow() as uow:
            # Try out cached list
            return uow.stocks.list()

    def add_broker(self, broker):
        self.brokers[broker.code] = broker

    def update_users(self, buys, sells, executed=False):
        # Emit buys and sells to brokers
        for broker in self.brokers:
            self.brokers[broker].update_users(buys, sells, executed=executed)

    def handle_order(self, msg):
        if msg["broker"] not in self.brokers:
            raise Exception("Malformed Broker")
        user = self.brokers[msg["broker"]].get_user(msg["csid"])
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
            try:
                symbol = uow.stocks.get(order.symbol).symbol
            except AttributeError:
                raise Exception("Unknown symbol")

        # Check this order can be completed before processing it
        # Good transaction isolation is going to be needed to ensure balance
        # and holdings stay positive in the event of concurrent order handlers
        try:
            self.brokers[msg["broker"]].validate_preorder(user, order)
        except Exception as e:
            raise e

        # Process order
        buys, sells = orderbook.add_order(order)
        self.update_users(buys, sells)

        summary = orderbook.summarise_books_for_symbol(symbol)
        log.info("[bold green]BOOK[/] [b]%s[/] %s" % (symbol, str(summary)))

        # Repeat trading until no trades are left
        while True:
            # Need to handle the market tick async from messages but this will do for now
            proposed_trades = orderbook.match_orderbook(symbol)
            if len(proposed_trades) == 0:
                break

            for trade in proposed_trades:
                buys, sells = orderbook.execute_trade(trade) # commit the Trade and close the orders
                self.update_users(buys, sells, executed=True) # update client holdings and balances

                self.stalls[symbol].log_trade(trade)
                log.info(trade)

            summary = orderbook.summarise_books_for_symbol(symbol)
            log.info("[bold green]BOOK[/] [b]%s[/] %s" % (symbol, str(summary)))


    def clear_trade(self):
        pass

    def recv(self, msg):
        if msg["txid"] in self.txid_set:
            raise Exception("Duplicate transaction")

        if msg["type"] == "order":
            self.handle_order(msg)

        # Idempotent txid
        self.txid_set.add(msg["txid"])
