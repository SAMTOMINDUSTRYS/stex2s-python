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

def match_one(buy_book, sell_book, highest_bid=None, lowest_ask=None, reference_price=None):
    done = False

    proposed_trades = []

    for buy in buy_book:
        if buy.closed:
            # Abort on closed buy
            break

        buy_sells = []
        curr_volume = 0

        buy_price = buy.price
        buy_volume = buy.volume

        for sell in sell_book:
            if sell.closed:
                # Skip sold sell
                continue

            sell_price = sell.price
            sell_volume = sell.volume

            if buy_price < sell_price:
                # Sells are sorted, so if we cannot afford this sell, there won't
                # be any more sells at the right price range
                done = True
                break

            # If the buy match or exceeds the sell price, we can trade
            curr_volume += sell_volume
            buy_sells.append(sell)

            # Determine price
            if reference_price or (highest_bid and lowest_ask):
                # TODO What about multiple orders?
                if buy.ts < sell.ts:
                    # If the sell is newer, it sells at the highest existing buy
                    execution_price = highest_bid
                else:
                    # If the buy is newer (or equal), it buys at the lowest existing sell
                    execution_price = lowest_ask
            else:
                # Keep old tests from non t7 suite passing
                execution_price = None


            if curr_volume >= buy_volume:
                # Either volume is just right or there is some excess to split into new Order
                proposed_trades.append(
                        propose_trade(buy, buy_sells, excess=curr_volume - buy_volume, execution_price=execution_price)
                )
                done = True # Force update before running match again
                break # Don't keep trying to add sells to this buy!

        if done:
            break

    return proposed_trades


def propose_trade(buy: Order, sells: List[Order], excess=0, execution_price=None):
    return model.Trade.propose_trade(buy, sells, excess, execution_price)


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
    return confirmed_buys, confirmed_sells


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

def summarise_books_for_symbol(symbol, uow=None):
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

        return summarise_books(buy_book, sell_book, buy=buy, sell=sell)

def match_orderbook(symbol, uow=None, reference_price=None):
    if not uow:
        uow = _default_uow()
    # Trigger match process
    #       Will need to think about how this can be thread-safe if we move
    #       matching/trading to a separate thread from adding orders
    with uow:
        buy_book = uow.orders.get_buy_book_for_symbol(symbol)
        sell_book = uow.orders.get_sell_book_for_symbol(symbol)
        summary = summarise_books_for_symbol(symbol, uow=uow)
        return match_one(buy_book, sell_book, highest_bid=summary["buy"], lowest_ask=summary["sell"], reference_price=reference_price)

