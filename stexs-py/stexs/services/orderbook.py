from stexs.domain import model
from stexs.services.logger import log
import stexs.io.persistence as iop
import copy
from typing import List

ORDER_UOW = iop.order.OrderMemoryUoW

def add_order(order: model.Order, uow_cls=ORDER_UOW):
    with uow_cls() as uow:
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
def close_txids(txids: List[str], uow_cls=ORDER_UOW):
    with uow_cls() as uow:
        for txid in txids:
            order = uow.orders.get(txid)
            order.closed = True
        uow.commit()

def match_one(buy_book, sell_book):
    done = False

    proposed_trades = []

    for buy in buy_book:
        if buy.closed:
            # Abort on closed buy
            break

        buy_filled = False
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

            # If the buy match or exceeds the sell price, we can trade
            if buy_price >= sell_price:
                curr_volume += sell_volume
                buy_sells.append(sell)

                if curr_volume >= buy_volume:
                    # Either volume is just right or there is some excess to split into new Order
                    buy_filled = True
                else:
                    # Not enough volume, keep iterating Orders
                    continue

                if buy_filled:
                    proposed_trades.append(
                            propose_trade(buy, buy_sells, excess=curr_volume - buy_volume)
                    )
                    done = True # Force update before running match again
                    break # Don't keep trying to add sells to this buy!

            else:
                # Sells are sorted, so if we have not filled this buy there
                # are no more sells at the right price range
                done = True
                break

        if done:
            break

    return proposed_trades


def propose_trade(buy: model.Order, sells: List[model.Order], excess=0):
    # Calculate average price of fulfilled buy
    tot_price = 0
    sell_txids = []
    for i_sell, sell in enumerate(sells):
        sell_txids.append(sell.txid)

        if i_sell == len(sells)-1:
            tot_price += (sell.price * (sell.volume - excess))
        else:
            tot_price += (sell.price * sell.volume)

    return model.Trade(
        symbol=buy.symbol,
        volume=buy.volume,
        buy_txid=buy.txid,
        sell_txids=sell_txids,
        avg_price=tot_price/buy.volume,
        total_price=tot_price,
        excess=excess,
        closed=False,
    )


def execute_trade(trade: model.Trade, uow_cls=ORDER_UOW):

    with uow_cls() as uow:
        # Close all transactions
        #TODO Pass the uow from this scope in to join them together
        close_txids([trade.buy_txid] + trade.sell_txids)

        # Finally, if Sell volume exceeded requirement, split the final sell into a new Order
        if trade.excess > 0:

                last_sell = uow.orders.get(trade.sell_txids[-1])
                last_sell.volume -= trade.excess

                # new_sell.ts does not get updated
                new_sell = copy.copy(last_sell)
                new_sell.closed = False
                new_sell.volume = trade.excess

                # Fiddle the txid so we know it is a split
                if '/' in new_sell.txid:
                    parent, split = new_sell.txid.split('/')
                    split_num = int(split)+1
                    new_sell.txid = '%s/%d' % (parent, split_num)
                else:
                    new_sell.txid += '/1'

                uow.orders.add(new_sell)
                uow.commit()

    # TODO Persist the Trade itself
    trade.closed = True

    with uow_cls() as uow:
        confirmed_buys = [uow.orders.get(trade.buy_txid)]
        confirmed_sells = []
        for sell in trade.sell_txids:
            confirmed_sells.append(uow.orders.get(sell))

    # TODO Join this at the Exchange level to transfer assets in the same transaction
    return confirmed_buys, confirmed_sells


def current_buy(symbol:str):
    with ORDER_UOW() as uow:
        book = uow.orders.get_buy_book_for_symbol(symbol)
        return book[0].price

def current_sell(symbol:str):
    with ORDER_UOW() as uow:
        book = uow.orders.get_sell_book_for_symbol(symbol)
        return book[0].price


def summarise_books(symbol):
    with ORDER_UOW() as uow:
        buy_book = uow.orders.get_buy_book_for_symbol(symbol)
        sell_book = uow.orders.get_sell_book_for_symbol(symbol)
        try:
            buy = buy_book[0].price
        except:
            buy = None

        try:
            sell = sell_book[0].price
        except:
            sell = None

        dbuys = dsells = 0
        nbuys = nsells = 0
        vbuys = vsells = 0

        for order in buy_book:
            if order.closed:
                continue
            dbuys +=1

            if order.price == buy:
                nbuys += 1
                vbuys += order.volume

        for order in sell_book:
            if order.closed:
                continue
            dsells +=1

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


def match_orderbook(symbol):
    # Trigger match process
    #       Will need to think about how this can be thread-safe if we move
    #       matching/trading to a separate thread from adding orders
    with ORDER_UOW() as uow:
        buy_book = uow.orders.get_buy_book_for_symbol(symbol)
        sell_book = uow.orders.get_sell_book_for_symbol(symbol)
        return match_one(buy_book, sell_book)

def summarise_orderbook(symbol):
    with ORDER_UOW() as uow:
        buy_book = uow.orders.get_buy_book_for_symbol(symbol)
        sell_book = uow.orders.get_sell_book_for_symbol(symbol)

        # some gross logging for now
        buy_str = []
        for order in buy_book:
            buy_str.append("%s#%d@%.3f" % (order.txid, order.volume, order.price))
        sell_str = []
        for order in sell_book:
            sell_str.append("%s#%d@%.3f" % (order.txid, order.volume, order.price))

    return buy_str, sell_str
