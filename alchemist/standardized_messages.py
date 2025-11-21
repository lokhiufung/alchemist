from alchemist.products.base_product import BaseProduct


## data messages
def create_quote_message(ts:int, gateway, pdt, exch, bids: list, asks: list) -> tuple:
    # temp
    quote = {
        'ts': ts,
        'data': {
            'bid': bids,
            'asks': asks,
        }

    }
    return (1, 1, (
        gateway,
        exch,
        pdt,
        quote
    ))


def create_tick_message(ts: int, gateway, exch, pdt, price: float, size: float):
    tick = {
        'ts': ts,
        'data': {
            'price': price, 
            'size': size,
        }
    }
    return (1, 2, (
        gateway,
        exch,
        pdt,
        tick
    ))


def create_bar_message(ts, gateway, exch, pdt, resolution, open_, high, low, close, volume):
    bar = {
        'ts': ts,
        'resolution': resolution,
        'data': {
            'open': open_,
            'high': high,
            'low': low,
            'close': close,
            'volume': volume,
        }
    }
    return (1, 3, (
        gateway,
        exch,
        pdt,
        bar
    ))


## order messages
def create_order_update_message(ts, gateway, strategy, exch, pdt, oid, status, average_filled_price, last_filled_price, last_filled_size, amend_price, amend_size, create_ts, target_price):
    order_update = {
        'ts': ts,
        'data': {
            'oid': oid,
            'pdt': pdt,
            'exch': exch, 
            'oid': oid,
            'status': status,
            'average_filled_price': average_filled_price,
            'last_filled_price': last_filled_price,
            'last_filled_size': last_filled_size,
            'amend_price': amend_price,
            'amend_size': amend_size,
            'create_ts': create_ts,
            'target_price': target_price,
        }
    }
    return (2, 1, (
        gateway,
        strategy,
        order_update
    ))


def create_place_order_message(ts: int, gateway: str, strategy: str, product: BaseProduct, oid: str, side: int, price: float, size: float, order_type: str, create_ts: int, time_in_force: str = None):
    order_dict =  {
        'ts': ts,
        'data': {
            'oid': oid,
            'strategy': strategy,
            'product': product.to_dict(),
            'side': side,
            'price': price,
            'size': size,
            'order_type': order_type,
            'time_in_force': time_in_force,
            'create_ts': create_ts,
        }
    }
    return (2, 2, (
        gateway,
        strategy,
        order_dict,
    ))


# portfolio messages
def create_balance_update_message(ts: int, gateway: str, balance_update: dict):
    """

    Args:
        ts (int): _description_
        gateway (str): _description_
        balance_update (dict): key is the currency, value is the balance

    Returns:
        _type_: _description_
    """
    return (3, 1, (
        gateway,
        {
            'ts': ts,
            'data': balance_update,
        }
    ))


def create_position_update_message(ts, gateway, exch, position_update):
    return (3, 2, (
        gateway,
        exch,
        {
            'ts': ts,
            'data': position_update,
        }
    ))

def create_position_update(pdt, side, size, last_price, average_price, realized_pnl=None, unrealized_pnl=None):
    return {
        pdt: {
            side: {
                'size': size,
                'last_price': last_price,
                'average_price': average_price,
                'realized_pnl': realized_pnl,
                'unrealized_pnl': unrealized_pnl,
            },
        }
    }

# order messages
@DeprecationWarning
def create_order_message(ts, gateway, strategy, oid, exch, pdt, price, size, order_type):
    order_dict = {
        'oid': oid,
        'exch': exch,
        'pdt': pdt,
        'price': price,
        'size': size,
        'order_type': order_type,
    }
    return (2, 2, (
        strategy,
        gateway,
        order_dict
    ))