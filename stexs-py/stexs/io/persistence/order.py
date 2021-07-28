from stexs.io.persistence.base import AbstractUoW, AbstractRepository
from stexs.domain import model
from stexs.services.logger import log

# Stolen from Base but thats ok, we can modify this to a different path
class OrderMemoryRepository(AbstractRepository):
    # TODO This seems to act more like a UoW than the UoW does !
    # CRIT TODO The refs to self._object only happen to work as the struct id is shared
    #           but should use GenericMemoryRepository._objects to prevent shadowing

    # Class variable allows us to mock a crap memory DB
    # as values will persist across instances of MemoryClientRepository
    _objects = {}

    # Keep tabs on object version checked out by `get` and ensure it is matched
    # when committing as a means to detect concurrent commits, effectively provides
    # compare-and-set (could still be caught in a race condition)
    _versions = {}

    def __init__(self, *args, **kwargs):
        self._staged_objects = {}
        self._staged_versions = {}

    def add(self, order: model.Order):
        if order.symbol not in self._staged_objects:
            self._staged_objects[order.symbol] = []
        self._staged_objects[order.symbol].append(order)
        log.info("[bold white]ORDR[/] [b]%s[/] %s" % (order.symbol, order))

    def get_buy_book_for_symbol(self, symbol: str):
        book = [order for order in self._objects[symbol] if not order.closed and order.side == "BUY"]
        book = sorted(book, key=lambda order: (-order.price, order.ts, order.txid))
        return book

    def get_sell_book_for_symbol(self, symbol: str):
        book = [order for order in self._objects[symbol] if not order.closed and order.side == "SELL"]
        book = sorted(book, key=lambda order: (order.price, order.ts, order.txid))
        return book

    def get(self, obj_id: str):

        # Providing read committed isolation as only committed data can be
        # read from _objects and _staged_objects cannot be read by other UoW
        # Does not guard against read skew and the like...
        self._staged_objects[obj_id] = copy.deepcopy(self._objects.get(obj_id))
        self._staged_versions[obj_id] = self._versions[obj_id]
        return self._staged_objects[obj_id]

    def _commit(self):
        for symbol in self._staged_objects:
            if symbol not in self._objects:
                self._objects[symbol] = []

            for order in self._staged_objects[symbol]:
                #if not self._objects.get(obj_id):
                #    self._versions[obj_id] = 0
                #else:
                #    if self._versions[obj_id] != self._staged_versions[obj_id]:
                #        raise Exception("Concurrent commit rejected")
                self._objects[symbol].append(order)
                #self._versions[obj_id] += 1

        # Reset staged objects?
        # CRIT TODO Could break commit - edit - commit workflow
        self._staged_objects = {}

class OrderMemoryUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = OrderMemoryRepository()

    def commit(self):
        self.orders._commit()
        self.committed = True

    def rollback(self):
        pass

