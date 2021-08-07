from stexs.io.persistence.base import AbstractUoW, GenericSqliteUoW, GenericSqliteRepository, GenericMemoryRepository
from stexs.services.logger import log
from stexs.domain import model
import time

class MemoryStockUoW(AbstractUoW):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stocks = GenericMemoryRepository(prefix="stocks")

    def list(self):
        return self.stocks.list()

    def commit(self):
        for stock_id, version in self.stocks.store._staged_versions.items():
            if version == 0:
                stock = self.stocks.store._staged_objects[stock_id]
                log.info("[bold red]MRKT[/] Listed [b]%s[/] %s" % (stock.symbol, stock.name))
        self.stocks._commit()

    def rollback(self):
        pass

class StockSqliteRepository(GenericSqliteRepository):

    _stock_cache = []
    _stock_stamp = None

    def _get(self, stock_symbol):
        return self.session.query(model.Stock).filter_by(symbol=stock_symbol).one()

    # TODO Should come from an abc for Stock
    def list(self):
        # TODO Probably better to do this with a decorator or the like but still,
        # interesting to see that we can quickly add this sort of stuff from the Repo!
        log.critical(StockSqliteRepository._stock_stamp)
        if not StockSqliteRepository._stock_stamp or (int(time.time()) - StockSqliteRepository._stock_stamp) > 60:
            StockSqliteRepository._stock_cache = [x[0] for x in self.session.query(model.Stock).with_entities(model.Stock.symbol).all()]
            StockSqliteRepository._stock_stamp = int(time.time())
            log.debug("Refreshing Stock cache")
        log.critical(StockSqliteRepository._stock_cache)
        return StockSqliteRepository._stock_cache


class StockSqliteUoW(GenericSqliteUoW):

    def __enter__(self, *args, **kwargs):
        super().__enter__(*args, **kwargs)
        self.stocks = StockSqliteRepository(self.session)
        return self

    def __exit__(self, *args):
        super().__exit__(*args)
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()
