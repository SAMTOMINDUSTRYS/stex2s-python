import pytest
import stexs.io.persistence as iop
from stexs.domain import model
from stexs.services import exchange

TEST_STOCK_UOW = iop.stock.MemoryStockUoW

# Checks endpoint will commit a new stock
def test_add_stock():
    expected_stock = model.Stock(
        symbol="STI.",
        name="Sam and Tom Industrys",
    )
    exchange.add_stock(expected_stock, uow=TEST_STOCK_UOW())

    with TEST_STOCK_UOW() as uow:
        actual_stock = uow.stocks.get(expected_stock.symbol)

    assert actual_stock is not None
    assert expected_stock == actual_stock
