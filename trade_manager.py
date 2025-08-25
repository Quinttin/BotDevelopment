import logging
from datetime import datetime
from typing import Tuple, Optional
import MetaTrader5 as mt5

# Globals set via initialize
mt5_sync_lock = None
PIP_MULTIPLIER = 10

def initialize(mt5_lock, pip_multiplier=10):
    global mt5_sync_lock, PIP_MULTIPLIER
    mt5_sync_lock = mt5_lock
    PIP_MULTIPLIER = pip_multiplier

def get_trade_direction(trade_type: int) -> str:
    """Return 'buy' or 'sell' based on MT5 order type."""
    if trade_type in [mt5.ORDER_TYPE_BUY, mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT]:
        return "buy"
    elif trade_type in [mt5.ORDER_TYPE_SELL, mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_SELL_STOP, mt5.ORDER_TYPE_SELL_STOP_LIMIT]:
        return "sell"
    return "unknown"

def calculate_lock_in_sl(position, lock_in_value_usd: float) -> Optional[float]:
    """Calculate stop loss price to lock in a specific USD profit."""
    with mt5_sync_lock:
        symbol = position.symbol
        volume = position.volume
        entry = position.price_open
        order_type = position.type
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            logging.error(f"Cannot calculate lock-in SL: Symbol info not found for {symbol}")
            return None
        point = symbol_info.point
        digits = symbol_info.digits
        if point <= 1e-9:
            logging.error(f"Cannot calculate lock-in SL: Invalid point size {point} for {symbol}")
            return None
        small_move_pips = 1.0
        price_up_small = entry + small_move_pips * point * PIP_MULTIPLIER
        price_down_small = entry - small_move_pips * point * PIP_MULTIPLIER
        profit_at_price_up = mt5.order_calc_profit(mt5.ORDER_TYPE_BUY, symbol, volume, entry, price_up_small)
        profit_at_price_down = mt5.order_calc_profit(mt5.ORDER_TYPE_SELL, symbol, volume, entry, price_down_small)
        profit_per_pip = 0.0
        if order_type == mt5.ORDER_TYPE_BUY:
            if profit_at_price_up is None:
                return None
            profit_per_pip = (profit_at_price_up - mt5.order_calc_profit(mt5.ORDER_TYPE_BUY, symbol, volume, entry, entry)) / small_move_pips
        elif order_type == mt5.ORDER_TYPE_SELL:
            if profit_at_price_down is None:
                return None
            profit_per_pip = (mt5.order_calc_profit(mt5.ORDER_TYPE_SELL, symbol, volume, entry, entry) - profit_at_price_down) / small_move_pips
        else:
            logging.warning(f"Cannot calculate lock-in SL for non-buy/sell position type: {order_type}")
            return None
        if abs(profit_per_pip) < 1e-6:
            logging.warning(
                f"Cannot calculate lock-in SL for {symbol} ticket {position.ticket}: Profit per pip is zero or negligible ({profit_per_pip:.5f})."
            )
            return None
        pips_needed = lock_in_value_usd / abs(profit_per_pip)
        logging.debug(
            f"Lock-in {lock_in_value_usd} USD for {symbol} ticket {position.ticket}: requires {pips_needed:.2f} pips (Profit per pip: {profit_per_pip:.5f})"
        )
        if order_type == mt5.ORDER_TYPE_BUY:
            sl = entry + pips_needed * point * PIP_MULTIPLIER
        elif order_type == mt5.ORDER_TYPE_SELL:
            sl = entry - pips_needed * point * PIP_MULTIPLIER
        else:
            sl = None
        if sl is not None:
            sl = round(sl, digits)
            logging.info(f"Calculated lock-in SL for {symbol} ticket {position.ticket}: {sl:.{digits}f}")
    return sl

def parse_lock_in(input: str) -> Tuple[float, float]:
    """Convert 'X/Y' input to (trigger, value) floats."""
    try:
        trigger, value = map(float, input.split('/'))
        return (abs(trigger), abs(value))
    except Exception:
        raise ValueError("\u274c Invalid format. Use 'Trigger/Value' (e.g. 500/100)")

def market_is_open() -> bool:
    """Return True if the forex market is typically open (Mon-Fri)."""
    now_utc = datetime.utcnow()
    return now_utc.weekday() < 5

def calculate_pnl_pips(open_price: float, close_price: float, position_type: int, symbol: str) -> Optional[float]:
    """Calculate PnL in pips for a trade given open and close price."""
    if open_price == 0 or close_price == 0:
        logging.warning(f"Cannot calculate pips for {symbol}: open or close price is zero.")
        return None
    with mt5_sync_lock:
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            logging.error(f"Cannot calculate pnl_pips: Symbol info not found for {symbol}")
            return None
        point = symbol_info.point
    if point <= 1e-9:
        logging.error(f"Invalid point size ({point}) for symbol {symbol}")
        return None
    if position_type == mt5.ORDER_TYPE_BUY:
        price_diff = close_price - open_price
    elif position_type == mt5.ORDER_TYPE_SELL:
        price_diff = open_price - close_price
    else:
        logging.warning(f"Cannot calculate pips for non-buy/sell position type: {position_type}")
        return None
    pips = price_diff / point
    return pips
