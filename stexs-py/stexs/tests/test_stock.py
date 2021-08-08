import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.services import exchange
from stexs.io.persistence.base import GenericMemoryRepository

@pytest.fixture
def uow():
    repo = GenericMemoryRepository(prefix="stocks")
    repo.store.clear()
    repo.store._clear()
    uow = iop.stock.MemoryStockUoW() # uses the GMR
    return uow

# Checks endpoint will commit a new stock
def test_add_stock(uow):
    expected_stock = model.Stock(
        symbol="STI.",
        name="Sam and Tom Industrys",
    )
    exchange.add_stock(expected_stock, uow=uow)

    with uow:
        actual_stock = uow.stocks.get(expected_stock.symbol)

    assert actual_stock is not None
    assert expected_stock == actual_stock

# Checks endpoint will list a set of stocks
def test_list_stock(uow):
    uow.stocks.store._store._objects["stocks"] = {
        "STI.": None,
        "ELAN": None,
        "ABER": None,
    }
    expected_set = set(uow.stocks.store._store._objects["stocks"].keys())
    actual_set = exchange.list_stocks(uow=uow)
    assert expected_set == actual_set
