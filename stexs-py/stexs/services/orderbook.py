from stexs.domain import model
from stexs.services.logger import log
import stexs.io.persistence as iop
import copy

ORDER_UOW = iop.order.OrderMemoryUoW

def handle_order(symbol:str, order: model.Order):
    with ORDER_UOW() as uow:

        # Assume the order goes through with no trouble
        #self.add_order(msg)
        # and hackily add the order to the actions to be bubbled up to STEX
        uow.orders.add(order)
        uow.commit()

        buys = []
        sells = []
        if order.side == "BUY":
            buys.append(order)
        elif order.side == "SELL":
            sells.append(order)

        return buys, sells

def match_one(symbol, uow):
    done = False

    new_orders = []

    buy_book = uow.orders.get_buy_book_for_symbol(symbol)
    sell_book = uow.orders.get_sell_book_for_symbol(symbol)

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
                    #self.trade(buy=buy, sells=buy_sells, excess=curr_volume-buy_volume)
                    new_orders.append({
                        "buy": buy.txid,
                        "sells": [sell.txid for sell in buy_sells],
                        "excess": curr_volume - buy_volume,
                    })
                    done = True # Force update before running match again
                    break # Don't keep trying to add sells to this buy!

            else:
                # Sells are sorted, so if we have not filled this buy there
                # are no more sells at the right price range
                done = True
                break

        if done:
            break

    return new_orders


def execute_trade(buy, sells, excess=0):
    with ORDER_UOW() as uow:
        # Close buy and sells
        buy = uow.orders.get(buy)
        buy.closed = True
        for sell in sells:
            sell = uow.orders.get(sell)
            sell.closed = True
        uow.commit()

    # Price
    with ORDER_UOW() as uow:
        tot_price = 0
        sell_txids = []
        for i_sell, sell in enumerate(sells):
            sell = uow.orders.get(sell)
            sell_txids.append(sell.txid)

            if i_sell == len(sells)-1:
                tot_price += (sell.price * (sell.volume - excess))
            else:
                tot_price += (sell.price * sell.volume)

    # Record Trade
    trade = model.Trade(
        symbol=buy.symbol,
        volume=buy.volume,
        buy_txid=buy.txid,
        sell_txids=sell_txids,
        avg_price=tot_price/buy.volume,
        total_price=tot_price,
        closed=True,
    )

    # Finally, if Sell volume exceeded requirement, split the final sell into a new Order
    if excess > 0:
        with ORDER_UOW() as uow:

            last_sell = uow.orders.get(sells[-1])
            last_sell.volume -= excess

            # new_sell.ts does not get updated
            new_sell = copy.copy(last_sell)
            new_sell.closed = False
            new_sell.volume = excess

            # Fiddle the txid so we know it is a split
            if '/' in new_sell.txid:
                split_num = int(new_sell.txid.split('/')[1])+1
            else:
                split_num = 1
            new_sell.txid += '/%d' % split_num

            uow.orders.add(new_sell)
            uow.commit()
            #self.orderbook.add_order(new_sell)

    return trade


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


def handle_market(symbol):
    # Trigger match process
    # TODO Needs to be done somewhere else
    #       Will need to think about how this can be thread-safe if we move
    #       matching/trading to a separate thread from adding orders
    summary = summarise_books(symbol)
    log.info("[bold green]BOOK[/] [b]%s[/] %s" % (symbol, str(summary)))
    with ORDER_UOW() as uow:
        fulfilled_orders = match_one(symbol, uow)


    # If we have to return here to execute orders, we can only match once
    buys = []
    sells = []
    trades = []
    with ORDER_UOW() as uow:
        for order in fulfilled_orders:
            trade = execute_trade(order["buy"], order["sells"], excess=order["excess"])
            trades.append(trade)
            log.info(trade)

            buys.append(uow.orders.get(order["buy"]))
            for sell in order["sells"]:
                sells.append(uow.orders.get(sell))

    with ORDER_UOW() as uow:
        buy_book = uow.orders.get_buy_book_for_symbol(symbol)
        sell_book = uow.orders.get_sell_book_for_symbol(symbol)
        # some gross logging for now
        buy_str = []
        for order in buy_book:
            buy_str.append("%s#%d@%.3f" % (order.txid, order.volume, order.price))
        log.info(buy_str)
        sell_str = []
        for order in sell_book:
            sell_str.append("%s#%d@%.3f" % (order.txid, order.volume, order.price))
        log.info(sell_str)
    summary = summarise_books(symbol)
    log.info("[bold green]BOOK[/] [b]%s[/] %s" % (symbol, str(summary)))

    # Sneaky way of getting actions back up to the Exchange, we'll obviously
    # want to do this async, and not in the order handler
    return buys, sells, trades
