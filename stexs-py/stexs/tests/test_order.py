import pytest
from stexs.domain.order import (
    Order,
    SplitOrderBuyException,
    SplitOrderVolumeException,
)

# Tests the Order dataclass
# There should be no Service, Repo or UoW funny business in here

def test_split_sell():
    order = Order(txid="1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1)
    order, remainder = Order.split_sell(order, 25)

    assert order.txid == "1"
    assert order.volume == 75

    assert remainder.txid == "1/1"
    assert remainder.volume == 25
    assert not remainder.closed


def test_split_split_sell():
    order = Order(txid="1/1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1)
    order, remainder = Order.split_sell(order, 25)

    assert order.txid == "1/1"
    assert order.volume == 75

    assert remainder.txid == "1/2"
    assert remainder.volume == 25
    assert not remainder.closed


def test_bad_split_big_volume():
    for bad_vol in [100, 101, 1000]:
        order = Order(txid="1/1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1)

        with pytest.raises(SplitOrderVolumeException, match="Cannot split sell for same or greater volume."):
            order, remainder = Order.split_sell(order, bad_vol)

def test_bad_split_small_volume():
    for bad_vol in [0, -1, -100]:
        order = Order(txid="1/1", csid="1", side="SELL", symbol="STI.", price=1.0, volume=100, ts=1)

        with pytest.raises(SplitOrderVolumeException, match="Cannot split sell without excess volume."):
            order, remainder = Order.split_sell(order, bad_vol)

def test_bad_split_buy():
    order = Order(txid="1", csid="1", side="BUY", symbol="STI.", price=1.0, volume=100, ts=1)

    with pytest.raises(SplitOrderBuyException, match="Cannot split non-sell."):
        order, remainder = Order.split_sell(order, 50)

