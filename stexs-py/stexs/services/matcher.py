import stexs.io.persistence as iop
from stexs.domain.model import Trade
from stexs.domain.order import Order
from typing import List

#TODO This should probably get injected somewhere but this works for now
MATCHER_UOW = iop.order.MatcherMemoryUoW
def _default_uow():
    return MATCHER_UOW()

def add_book(book, reference_price, uow=None):
    if not uow:
        uow = _default_uow()
    with uow:
        uow.orders.add_book(book, reference_price)
        uow.commit()

def add_order(order, uow=None):
    if not uow:
        uow = _default_uow()
    with uow:
        uow.orders.add(order)
        uow.commit()

def delete_order(order, uow=None):
    if not uow:
        uow = _default_uow()
    with uow:
        uow.orders.delete(order)
        uow.commit()

def propose_trade(buy: Order, sells: List[Order], excess=0, execution_price=None):
    return Trade.propose_trade(buy, sells, excess, execution_price)

def match_orderbook(symbol, uow=None):
    if not uow:
        uow = _default_uow()
    with uow:
        book = uow.orders.get_book(symbol)
        highest_bid = book["highest_bid"]
        lowest_ask = book["lowest_ask"]
        reference_price = book["reference_price"]

        done = False

        proposed_trades = []

        for buy in book["BUY"]["book"]:
            buy_sells = []
            curr_volume = 0

            buy_price = buy.price
            buy_volume = buy.volume

            for sell in book["SELL"]["book"]:
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
                execution_price = None
                if buy.ts < sell.ts:
                    if highest_bid:
                        if reference_price and highest_bid < reference_price:
                            if lowest_ask and highest_bid >= lowest_ask:
                                if buy_price == float("inf"):
                                    # EX16
                                    execution_price = reference_price
                                else:
                                    # EX13
                                    execution_price = highest_bid
                            else:
                                # EX4
                                execution_price = reference_price
                        else:
                            if lowest_ask and lowest_ask > reference_price and lowest_ask > highest_bid:
                                # EX18
                                execution_price = lowest_ask
                            else:
                                execution_price = highest_bid
                    else:
                        if lowest_ask and reference_price and reference_price < lowest_ask:
                            # EX10
                            execution_price = lowest_ask
                        else:
                            execution_price = reference_price
                else:
                    # If the buy is newer (or equal), it buys at the lowest existing sell
                    if lowest_ask:
                        if reference_price and lowest_ask > reference_price:
                            # EX6
                            execution_price = reference_price
                        else:
                            execution_price = lowest_ask
                    else:
                        if highest_bid and reference_price and reference_price > highest_bid:
                            execution_price = highest_bid
                        else:
                            execution_price = reference_price

                if curr_volume >= buy_volume:
                    # Either volume is just right or there is some excess to split into new Order
                    proposed_trades.append(
                            propose_trade(buy, buy_sells, excess=curr_volume - buy_volume, execution_price=execution_price)
                    )

                    # Delete the orders from the matcher book
                    for executed_order in [buy.txid] + [sell.txid for sell in buy_sells]:
                        delete_order(executed_order, uow=uow)

                    done = True # Force update before running match again
                    break # Don't keep trying to add sells to this buy!

            if done:
                break

        return proposed_trades
