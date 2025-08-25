import logging
import time
# datetime.utcnow() deprecation isn't relevant here; keep naive for MT5 timestamps
from datetime import datetime
from threading import Lock
from typing import Optional
from io import StringIO
import pandas as pd
import MetaTrader5 as mt5
import sqlite3
from pathlib import Path
import re

# Globals set via initialize
config = None
runtime_state = None
STATE_LOCK = None
mt5_sync_lock = None
PIP_MULTIPLIER = 10
CACHE_DB = Path(__file__).with_name("history_cache.db")
CACHE_LOCK = Lock()

def _get_cache_conn():
    with sqlite3.connect(CACHE_DB) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS history_cache (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                data TEXT NOT NULL,
                PRIMARY KEY (symbol, timeframe, start_time, end_time)
            )
            """
        )

# Ensure cache table exists at module import
_get_cache_conn()

def _load_from_cache(symbol, timeframe, start, end):
    with CACHE_LOCK:
        with sqlite3.connect(CACHE_DB) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT data FROM history_cache WHERE symbol=? AND timeframe=? AND start_time=? AND end_time=?",
                (symbol, timeframe, start, end),
            )
            row = cur.fetchone()
        if row:
            try:
                # Wrap raw JSON string in StringIO to avoid pandas FutureWarning
                return pd.read_json(StringIO(row[0]), orient="split")
            except (ValueError, TypeError):
                return None
        return None

def _save_to_cache(symbol, timeframe, start, end, df):
    with CACHE_LOCK:
        try:
            with sqlite3.connect(CACHE_DB) as conn:
                df_json = df.to_json(orient="split")
                conn.execute(
                    "REPLACE INTO history_cache(symbol, timeframe, start_time, end_time, data) VALUES (?, ?, ?, ?, ?)",
                    (symbol, timeframe, start, end, df_json),
                )
        except Exception as e:
            logging.error(f"Failed to save cache for {symbol} {timeframe}: {e}")

def _timeframe_to_delta(timeframe: str) -> pd.Timedelta:
    m = re.match(r"([A-Z]+)(\d+)", timeframe)
    if not m:
        return pd.Timedelta(0)
    unit, value = m.groups()
    value = int(value)
    if unit == "M":
        return pd.Timedelta(minutes=value)
    if unit == "H":
        return pd.Timedelta(hours=value)
    if unit == "D":
        return pd.Timedelta(days=value)
    if unit == "W":
        return pd.Timedelta(weeks=value)
    if unit == "MN":
        return pd.Timedelta(days=30 * value)
    return pd.Timedelta(0)

def initialize(cfg, runtime, state_lock, mt5_lock, pip_multiplier=10):
    """Initialize module configuration and synchronization primitives.

    This module also defines ``CACHE_LOCK`` to serialize access to the on-disk
    history cache. If other modules need coordinated cache operations, they
    should import and use this lock.
    """
    global config, runtime_state, STATE_LOCK, mt5_sync_lock, PIP_MULTIPLIER
    config = cfg
    runtime_state = runtime
    STATE_LOCK = state_lock
    mt5_sync_lock = mt5_lock
    PIP_MULTIPLIER = pip_multiplier

def fetch_history_data(symbol: str, timeframe: str, num_bars: int) -> Optional[pd.DataFrame]:
    """Fetch historical data from MT5 with reconnection attempts and caching."""
    tf_mt5 = getattr(mt5, f"TIMEFRAME_{timeframe}", None)
    if tf_mt5 is None:
        logging.error(f"Timeframe {timeframe} not supported in MT5.")
        return None

    delta = _timeframe_to_delta(timeframe)
    if delta == pd.Timedelta(0):
        logging.warning(f"Timeframe {timeframe} not recognized for caching; caching disabled.")
        end_time = pd.Timestamp.utcnow()
        start_time = end_time - pd.Timedelta(minutes=num_bars)
        cache_start = cache_end = None
    else:
        end_time = pd.Timestamp.utcnow().floor(delta)
        start_time = end_time - delta * (num_bars - 1)
        cache_start = start_time.isoformat()
        cache_end = end_time.isoformat()
        cached_df = _load_from_cache(symbol, timeframe, cache_start, cache_end)
        if cached_df is not None:
            return cached_df

    rates = None
    need_symbol_sleep = False
    with mt5_sync_lock:
        if not runtime_state["mt5_initialized"] or not mt5.terminal_info():
            logging.warning(f"MT5 not initialized. Reconnecting for {symbol} {timeframe}.")
            if not init_mt5():
                return None
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            logging.warning(f"Symbol {symbol} not in MarketWatch. Selecting...")
            if not mt5.symbol_select(symbol, True):
                logging.error(f"Failed to select symbol {symbol}.")
                mt5.symbol_select(symbol, False)
                return None
            symbol_info = mt5.symbol_info(symbol)
            if not symbol_info:
                logging.error(f"Symbol {symbol} info unavailable after selecting.")
                return None
            logging.info(f"Successfully selected {symbol}.")
            need_symbol_sleep = True

    if need_symbol_sleep:
        time.sleep(0.5)

    for attempt in range(3):
        err = None
        exc = None
        with mt5_sync_lock:
            try:
                rates = mt5.copy_rates_from(
                    symbol,
                    tf_mt5,
                    end_time.to_pydatetime().replace(tzinfo=None),
                    num_bars,
                )
                if rates is None:
                    err = mt5.last_error()
            except Exception as e:
                exc = e
                rates = None
        if rates is not None and len(rates) >= num_bars:
            break
        elif rates is not None and len(rates) < num_bars:
            logging.warning(f"Fetched only {len(rates)} bars for {symbol} {timeframe}.")
            break
        else:
            if err is not None:
                logging.warning(
                    f"Failed to fetch data for {symbol} {timeframe} attempt {attempt+1}: {err}"
                )
            elif exc is not None:
                logging.error(
                    f"Exception during MT5 data fetch for {symbol} {timeframe} attempt {attempt+1}: {exc}"
                )
        time.sleep(attempt + 1)

    if rates is None or len(rates) == 0:
        logging.error(f"Failed to fetch any data for {symbol} {timeframe} after retries.")
        return None

    try:
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df.set_index("time", inplace=True)
        # Drop timezone to avoid downstream pandas-ta warnings
        try:
            df.index = df.index.tz_localize(None)
        except Exception:
            pass
        if df[['open', 'high', 'low', 'close']].isnull().any().any():
            logging.warning(f"NaN values found in OHLC data for {symbol} {timeframe}.")
        if cache_start and cache_end:
            _save_to_cache(symbol, timeframe, cache_start, cache_end, df)
        return df
    except Exception as e:
        logging.error(f"Error processing fetched rates for {symbol} {timeframe}: {e}")
        return None

def init_mt5() -> bool:
    """Initialize MT5 terminal with credentials from config."""
    if config is None or runtime_state is None:
        logging.error("mt5_utils.initialize must be called before init_mt5()")
        return False
    creds = config.get("mt5_credentials", {})
    with STATE_LOCK:
        runtime_state["mt5_initialized"] = False
    try:
        with mt5_sync_lock:
            if mt5.terminal_info() and mt5.account_info():
                logging.info("MT5 already initialized and connected.")
                with STATE_LOCK:
                    runtime_state["mt5_initialized"] = True
                return True
            logging.info("Attempting MT5 initialization...")
            init_success = mt5.initialize(
                path=creds.get("terminal_path"),
                login=creds.get("account_number"),
                password=creds.get("password"),
                server=creds.get("server"),
                timeout=10000,
            )
            if not init_success:
                err = mt5.last_error()
                logging.error(f"MT5 initialize() failed: {err}")
                mt5.shutdown()
                return False
            account_info = mt5.account_info()
            if not account_info or account_info.login != creds.get("account_number"):
                err = mt5.last_error()
                logging.error(
                    f"MT5 Initialization successful, but Login failed for account {creds.get('account_number')}. Account Info: {account_info}. Error: {err}"
                )
                mt5.shutdown()
                return False
            terminal_info = mt5.terminal_info()
            if not terminal_info or not terminal_info.connected:
                logging.error(
                    f"MT5 Initialization and Login successful, but Terminal is not connected. Terminal Info: {terminal_info}"
                )
                mt5.shutdown()
                return False
            logging.info(
                f"MT5 initialized successfully. Account: {account_info.login}, Server: {account_info.server}, Terminal Connected: {terminal_info.connected}"
            )
            with STATE_LOCK:
                runtime_state["mt5_initialized"] = True
            return True
    except Exception as e:
        logging.error(f"Exception during MT5 initialization: {e}", exc_info=True)
        with STATE_LOCK:
            runtime_state["mt5_initialized"] = False
        try:
            mt5.shutdown()
        except Exception:
            pass
        return False
