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

def get_execution_price(buy_ts, sell_ts, buy_price, sell_price, reference_price, highest_bid, lowest_ask):
    if buy_ts > sell_ts:
        is_buying = True
        is_selling = False
        incoming_order_price, book_order_price = buy_price, sell_price
    else:
        is_buying = False
        is_selling = True
        incoming_order_price, book_order_price = sell_price, buy_price

    price = None

    # If we can match without a highest_bid or lowest_ask then these are both
    # market orders and no other information is available to set a price
    if not highest_bid and not lowest_ask:
        # EX1
        price = reference_price

    # Market or limit order meeting a market order
    elif book_order_price == float("inf") or book_order_price == float("-inf"):
        if is_selling:
            # EX16, EX17, EX18
            # Mixed market and limit
            if not highest_bid:
                # EX9, EX10
                # Limit meets only market orders so highest_bid unset
                highest_bid = reference_price
            elif not lowest_ask:
                # EX4, EX5
                # Market order meets market or limit so this side is unset
                lowest_ask = reference_price

            # Sell at highest price
            price = max(reference_price, highest_bid, lowest_ask)

        elif is_buying:
            # EX19, EX20, EX21
            # Mixed market and limit
            if not lowest_ask:
                # EX11, EX12
                # Limit meets only market orders so lowest_ask unset
                lowest_ask = reference_price
            elif not highest_bid:
                # EX6, EX7
                # Market order meets market or limit so this side is unset
                highest_bid = reference_price

            # Buy at lowest price
            price = min(reference_price, highest_bid, lowest_ask)

    # Market or limit order meeting a limit order
    else:
        if is_selling:
            # EX2, EX13
            price = highest_bid
        elif is_buying:
            # EX3, EX14
            price = lowest_ask

    return price


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
                execution_price = get_execution_price(buy.ts, sell.ts, buy_price, sell_price, reference_price, highest_bid, lowest_ask)

                excess = curr_volume - buy_volume
                if curr_volume >= buy_volume:
                    # Either volume is just right or there is some excess to split into new Order
                    proposed_trades.append(
                            propose_trade(buy, buy_sells, excess=excess, execution_price=execution_price)
                    )

                    # Split sell
                    if excess > 0:
                        sell, remainder_sell = Order.split_sell(uow.orders.get(sell.txid), excess)
                        uow.orders.add(remainder_sell)

                    # Delete the orders from the matcher book
                    for executed_order in [buy.txid] + [sell.txid for sell in buy_sells]:
                        delete_order(executed_order, uow=uow)

                    done = True # Force update before running match again
                    break # Don't keep trying to add sells to this buy!

            if done:
                break
        uow.commit()
        return proposed_trades
