from dataclasses import dataclass, field
from dataclasses import replace as dataclass_replace
from . import order_exception

@dataclass
class Order:
    txid: str
    csid: str
    ts: int
    side: str
    symbol: str
    price: float
    volume: int
    closed: bool = False

    @property
    def stexid(self):
        return self.txid

    @staticmethod
    def split_sell(filled_sell: "Order", excess_volume: int):

        if filled_sell.side != "SELL":
            raise order_exception.SplitOrderBuyException("Cannot split non-sell.")
        if excess_volume >= filled_sell.volume:
            raise order_exception.SplitOrderVolumeException("Cannot split sell for same or greater volume.")
        if excess_volume <= 0:
            raise order_exception.SplitOrderVolumeException("Cannot split sell without excess volume.")

        filled_sell.volume -= excess_volume

        # Fiddle the txid so we know it is a split
        if '/' in filled_sell.txid:
            parent, split = filled_sell.txid.split('/')
            split_num = int(split)+1
        else:
            parent = filled_sell.txid
            split_num = 1
        new_txid = '%s/%d' % (parent, split_num)

        remainder_sell = dataclass_replace(filled_sell, txid=new_txid, volume=excess_volume, closed=False)
        return filled_sell, remainder_sell


