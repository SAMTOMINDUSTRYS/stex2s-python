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

        done = False

        proposed_trades = []

        for buy in book.buy_book:
            buy_sells = []
            curr_volume = 0

            buy_price = buy.price
            buy_volume = buy.volume

            for sell in book.sell_book:
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
                execution_price = Trade.get_execution_price(buy.ts, sell.ts, buy_price, sell_price, book.reference_price, book.highest_bid, book.lowest_ask)

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
