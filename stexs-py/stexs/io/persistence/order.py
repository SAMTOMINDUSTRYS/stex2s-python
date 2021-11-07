import copy
from stexs.io.persistence.base import AbstractUoW, GenericVersionedMemoryDictWrapper
from stexs.domain.order import Order, OrderRepository
from stexs.services.logger import log

class MatcherMemoryRepository(OrderRepository):
    from collections import namedtuple
    MatcherOrder = namedtuple("MatcherOrder", "symbol side price volume ts txid")

    orderbooks = {}
    txid_map = {}

    def add_book(self, symbol, reference_price):
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = {
                "reference_price": reference_price,
                "highest_bid": None,
                "lowest_ask": None,
                "BUY": {
                    "book": [],
                },
                "SELL": {
                    "book": [],
                },
            }

    def add(self, order):
        self.add_order(order.symbol, order.side, order.price, order.volume, order.ts, order.txid)
        self.txid_map[order.txid] = copy.copy(order)

    def get(self, txid):
        return self.txid_map.get(txid)
    def clear(self):
        self.orderbooks = {}
    def _commit(self):
        pass

    def get_buy_book_for_symbol(self, symbol):
        return self.get_book["BUY"]["book"]
    def get_sell_book_for_symbol(self, symbol):
        return self.get_book["SELL"]["book"]

    def get_book(self, symbol):
        return self.orderbooks[symbol]

    # Exchange sends bare minimum required for the matcher to do work
    # We don't need Order objects here (maybe)
    def add_order(self, symbol, side, price, volume, ts, txid):

        if not price:
            # Handle market order price hack
            price = float("inf") if side == "BUY" else float("-inf")
        else:
            # Set current highest_bid or lowest_ask
            if side == "BUY":
                if not self.orderbooks[symbol]["highest_bid"] or price > self.orderbooks[symbol]["highest_bid"]:
                    self.orderbooks[symbol]["highest_bid"] = price
            elif side == "SELL":
                if not self.orderbooks[symbol]["lowest_ask"] or price < self.orderbooks[symbol]["lowest_ask"]:
                    self.orderbooks[symbol]["lowest_ask"] = price

        # Add the order to the right book and set to inf if market order
        self.orderbooks[symbol][side]["book"].append(self.MatcherOrder(symbol, side, price, volume, ts, txid))

        # TODO LOW PERF
        # This is fine for now but we'd be better off using something like `bisect`
        # which even supports a straightforward `key` parameter in 3.10, avoiding
        # the need to import something like `sortedcollection`
        side_sign = -1 if side == "BUY" else 1
        self.orderbooks[symbol][side]["book"] = sorted(self.orderbooks[symbol][side]["book"], key=lambda order: (order.price * side_sign, order.ts, order.txid))

    def delete(self, txid):
        try:
            order = self.txid_map.pop(txid)
        except:
            return None

        self.orderbooks[order.symbol][order.side]["book"] = [o for o in self.orderbooks[order.symbol][order.side]["book"] if o.txid != txid]
        return order



class OrderMemoryRepository(OrderRepository):

    txid_map = {}
    store = GenericVersionedMemoryDictWrapper()

    def add(self, order: Order):
        obj_id = "%s>%s" % (order.symbol, order.txid)
        self.store._add(obj_id, order)
        self.txid_map[order.txid] = obj_id # Primary transaction index
        log.info("[bold white]ORDR[/] [b]%s[/] %s" % (order.symbol, order))

    def get_buy_book_for_symbol(self, symbol: str):
        book = self.store._store._xget("%s" % symbol)
        if book:
            book = [order for order in book.values() if not order.closed and order.side == "BUY"]
            for b in book:
                if b.price is None:
                    b.price = float("inf")
            book = sorted(book, key=lambda order: (-order.price, order.ts, order.txid))
            return book
        return []

    def get_sell_book_for_symbol(self, symbol: str):
        book = self.store._store._xget("%s" % symbol)
        if book:
            book = [order for order in book.values() if not order.closed and order.side == "SELL"]
            for b in book:
                if b.price is None:
                    b.price = float("-inf")
            book = sorted(book, key=lambda order: (order.price, order.ts, order.txid))
            return book
        return []

    def get(self, txid: str):
        if txid not in self.txid_map:
            return None
        else:
            obj_id = self.txid_map[txid]
            obj = self.store._get(obj_id)
            return obj

    def _commit(self):
        self.store._commit()

    def clear(self):
        self.store._clear()

class OrderMemoryUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = OrderMemoryRepository()

    def commit(self):
        self.orders._commit()
        self.committed = True

    def rollback(self):
        pass

class MatcherMemoryUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = MatcherMemoryRepository()

    def commit(self):
        self.orders._commit()
        self.committed = True

    def rollback(self):
        pass

