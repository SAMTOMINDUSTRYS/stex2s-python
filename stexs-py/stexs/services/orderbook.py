from stexs.domain import model
from stexs.domain.order import Order
from stexs.services.logger import log
import stexs.io.persistence as iop
import copy
from dataclasses import asdict as dataclasses_asdict
from typing import List

#TODO This should probably get injected somewhere but this works for now
ORDER_UOW = iop.order.OrderMemoryUoW

def _default_uow():
    return ORDER_UOW()

def add_order(order: Order, uow=None):
    if not uow:
        uow = _default_uow()
    with uow:
        uow.orders.add(order)
        uow.commit()

        buys = []
        sells = []
        if order.side == "BUY":
            buys.append(order)
        elif order.side == "SELL":
            sells.append(order)

        return buys, sells

#CRIT TODO Look up all txids at the same time
#TODO How to manage interfaces like this that allow for optional uow?
def _close_txids(txids: List[str], uow):
    for txid in txids:
        order = uow.orders.get(txid)
        order.closed = True
def close_txids(txids: List[str], uow=None):
    if not uow:
        uow = _default_uow()
    with uow:
        log.info(txids)
        _close_txids(txids, uow)
        uow.commit()


def split_sell(filled_sell: Order, excess_volume: int):
    filled_sell, remainder_sell = Order.split_sell(filled_sell, excess_volume)
    return filled_sell, remainder_sell


def execute_trade(trade: model.Trade, uow=None):
    if not uow:
        uow = _default_uow()

    with uow:
        # Close all transactions
        # Pass the uow from this scope to join the transactions together
        _close_txids([trade.buy_txid] + trade.sell_txids, uow=uow)

        #TODO CRIT YIKES
        # Set the Sell value to the executed value
        for sell_id in trade.sell_txids:
            sell = uow.orders.get(sell_id)
            if sell.price == float("-inf"):
                sell.price = trade.avg_price

        # Finally, if Sell volume exceeded requirement, split the final sell into a new Order
        if trade.excess > 0:
            last_sell = uow.orders.get(trade.sell_txids[-1])
            filled_sell, remainder_sell = split_sell(last_sell, trade.excess)
            uow.orders.add(remainder_sell)
        uow.commit()

    # TODO Persist the Trade itself
    trade.clear_trade()

    with uow:
        confirmed_buys = [uow.orders.get(trade.buy_txid)]
        confirmed_sells = []
        for sell in trade.sell_txids:
            confirmed_sells.append(uow.orders.get(sell))

    # TODO Join this at the Exchange level to transfer assets in the same transaction
    return confirmed_buys, confirmed_sells, remainder_sell


def summarise_books(buy_book, sell_book, buy=None, sell=None):
    dbuys = dsells = 0
    nbuys = nsells = 0
    vbuys = vsells = 0

    for order in buy_book:
        if order.closed:
            continue
        dbuys +=1

        if buy:
            if order.price == buy:
                nbuys += 1
                vbuys += order.volume

    for order in sell_book:
        if order.closed:
            continue
        dsells +=1

        if sell:
            if order.price == sell:
                nsells += 1
                vsells += order.volume

    return {
        "dbuys": dbuys,
        "dsells": dsells,

        "nbuys": nbuys,
        "nsells": nsells,

        "vbuys": vbuys,
        "vsells": vsells,

        "buy": buy,
        "sell": sell,
    }

def get_serialised_order_books_for_symbol(symbol, n=None, uow=None):
    if not uow:
        uow = _default_uow()

    with uow:
        return {
            "buy_book": [dataclasses_asdict(order) for order in uow.orders.get_buy_book_for_symbol(symbol)[:n]],
            "sell_book": [dataclasses_asdict(order) for order in uow.orders.get_sell_book_for_symbol(symbol)[:n]],
        }

def summarise_books_for_symbol(symbol, reference_price=None, uow=None):
    if not uow:
        uow = _default_uow()

    with uow:
        buy_book = uow.orders.get_buy_book_for_symbol(symbol)
        sell_book = uow.orders.get_sell_book_for_symbol(symbol)

        # TODO This won't work for market orders
        try:
            buy = buy_book[0].price
        except:
            buy = None

        try:
            sell = sell_book[0].price
        except:
            sell = None

        if buy == float("inf"):
            buy = reference_price

        if sell == float("-inf"):
            sell = reference_price

        return summarise_books(buy_book, sell_book, buy=buy, sell=sell)

