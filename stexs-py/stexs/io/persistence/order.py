import copy
import heapq

from stexs.io.persistence.base import AbstractUoW, GenericVersionedMemoryDictWrapper
from stexs.domain.order import Order, OrderRepository
from stexs.services.logger import log

# should be in domain as ABC?
class OrderbookHeap:

    def __init__(self):
        self.bid_heap = []
        self.deleted_bids = {}
        self.ask_heap = []
        self.deleted_asks = {}

    def add_price(self, side, price):
        if side == "BUY":
            heapq.heappush(self.bid_heap, price * -1) # max-to-min heap
        elif side == "SELL":
            heapq.heappush(self.ask_heap, price)

    def _prune_heap(self, heap, deleted_prices):
        # if the min heap val is deleted, pop it
        if len(heap) == 0:
            return
        while deleted_prices.get(heap[0], 0) > 0:
            min_val = heap[0]
            heapq.heappop(heap)
            deleted_prices[min_val] -= 1

    def remove_price(self, side, price):
        if side == "BUY":
            price *= -1 # convert to min heap price
            self.deleted_bids[price] = self.deleted_bids.get(price, 0) + 1
        elif side == "SELL":
            self.deleted_asks[price] = self.deleted_asks.get(price, 0) + 1

    def highest_bid(self):
        if len(self.bid_heap) == 0:
            return
        self._prune_heap(self.bid_heap, self.deleted_bids)
        return self.bid_heap[0] * -1

    def lowest_ask(self):
        if len(self.ask_heap) == 0:
            return
        self._prune_heap(self.ask_heap, self.deleted_asks)
        return self.ask_heap[0]


class Orderbook:
    def __init__(self, reference_price):
        self.reference_price = reference_price
        self.highest_bid = None
        self.lowest_ask = None
        self._orderheap = OrderbookHeap()
        self._BUY = []
        self._SELL = []

    def add_price(self, side, price):
        if not price or price == float("inf") or price == float("-inf"):
            return

        self._orderheap.add_price(side, price)
        if side == "BUY":
            self.highest_bid = self._orderheap.highest_bid()
        elif side == "SELL":
            self.lowest_ask = self._orderheap.lowest_ask()

    def remove_price(self, side, price):
        self._orderheap.remove_price(side, price)

    def add_order(self, side, order):
        if side == "BUY":
            self._BUY.append(order)
        elif side == "SELL":
            self._SELL.append(order)

        # TODO LOW PERF
        # This is fine for now but we'd be better off using something like `bisect`
        # which even supports a straightforward `key` parameter in 3.10, avoiding
        # the need to import something like `sortedcollection`
        side_sign = -1 if side == "BUY" else 1

        # TODO wtf syntax
        setattr(self, '_'+side, sorted(getattr(self, '_'+side), key=lambda order: (order.price * side_sign, order.ts, order.txid)))

        # Add price
        if order.price:
            self.add_price(order.side, order.price)

    def purge_order(self, side, txid):
        # TODO LOW PERF
        setattr(self, '_'+side, [o for o in getattr(self, '_'+side) if o.txid != txid])

    @property
    def sell_book(self):
        return self._SELL

    @property
    def buy_book(self):
        return self._BUY


class MatcherMemoryRepository(OrderRepository):
    from collections import namedtuple
    MatcherOrder = namedtuple("MatcherOrder", "symbol side price volume ts txid")

    orderbooks = {}
    txid_map = {}

    def add_book(self, symbol, reference_price):
        if symbol not in self.orderbooks:
            self.orderbooks[symbol] = Orderbook(reference_price=reference_price)

    def add(self, order):
        if order.price == float("inf") or order.price == float("-inf"):
            price = None
        else:
            price = order.price
        self.add_order(order.symbol, order.side, price, order.volume, order.ts, order.txid)
        self.txid_map[order.txid] = copy.copy(order)

    def get(self, txid):
        return self.txid_map.get(txid)
    def clear(self):
        self.orderbooks = {}
    def _commit(self):
        pass

    def get_buy_book_for_symbol(self, symbol):
        return self.get_book.BUY
    def get_sell_book_for_symbol(self, symbol):
        return self.get_book.SELL

    def get_book(self, symbol):
        return self.orderbooks[symbol]

    # Exchange sends bare minimum required for the matcher to do work
    # We don't need Order objects here (maybe)
    def add_order(self, symbol, side, price, volume, ts, txid):
        if not price:
            # Market order price hack
            price = float("inf") if side == "BUY" else float("-inf")
        self.orderbooks[symbol].add_order(side, self.MatcherOrder(symbol, side, price, volume, ts, txid))


    def delete(self, txid):
        try:
            order = self.txid_map.pop(txid)
        except:
            return None

        if order.price:
            self.orderbooks[order.symbol].remove_price(order.side, order.price)

        # remove by txid
        self.orderbooks[order.symbol].purge_order(order.side, txid)
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

