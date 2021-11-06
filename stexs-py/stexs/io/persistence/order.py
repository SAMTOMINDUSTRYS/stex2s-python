import copy
from stexs.io.persistence.base import AbstractUoW, GenericVersionedMemoryDictWrapper
from stexs.domain.order import Order, OrderRepository
from stexs.services.logger import log

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

