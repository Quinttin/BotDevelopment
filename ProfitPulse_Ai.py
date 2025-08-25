#!/usr/bin/env python
# Version 78 - Bollinger Band TP selection

"""
MT5-PYTHON-TELEGRAM-BOT  
-------------------------------------------------------------
This bot integrates with MetaTrader5, scans market data, sends trade signals,
manages trades, and provides advanced commands via Telegram. It now supports
distinct Semi-Automatic and Fully-Automatic operational modes and implements
advanced filtering (Trend Filter, Pip Confirmation) in Full-Auto mode.

Key Features:
- Semi-Auto Mode: Sends signals to Telegram with buttons for user execution. Manual trade management applies to all trades. Cooperative scalping available manually after securing profit on a main trade.
- Full-Auto Mode: Processes signals through automated filters (Trend, Pip Confirmation) and places trades automatically. Cooperative scalping triggers automatically after parent trade profit lock-in.
- Enhanced Full-Auto Filtering: Higher Timeframe Trend Filter and Price Pip Confirmation before automated entry.
- Sequential long_medium trades in Coop mode: new trade opens only after previous pair locks profit.
- Limits scalping to one trade per pair with smaller lot size, re-entering on new signals post-TP.
- Comprehensive trade management and user interaction via Telegram.
- Robust error handling and graceful shutdown.
- Dynamic indicator column naming.
- Fixed background task execution loop.
- Detailed, step-by-step configuration flows.
- Persistency of operational mode in config.
"""

# =============================================================================
# DETAILED COMMENT LIST
#
# 1. Available Telegram Commands:
#    /start - Show help banner listing all commands.
#    /start_scanner - Start scanning and trade-manager pipelines immediately.
#    /configure_scanner - Begin scanner configuration flow.
#    /configure_trade_manager - Begin trade-manager configuration flow.
#    /set_mode <semi|full> - Switch between SEMI_AUTO and FULL_AUTO modes.
#    /lock_in - Manually lock in profit for cooperative scalping.
#    /trail_stop - Configure global trailing-stop settings.
#    /status - Show current bot status and running state.
#    /close <symbol|ticket> - Close a position by symbol or ticket.
#    /close_all [symbol] - Close all positions, optionally filtered by symbol.
#    /cancel - Abort any ongoing configuration conversation.
#    /save_config - Persist current configuration to config.json.
#
#    /stop_scanner - Stop the scanner only, leaving the Trade Manager running.
# 2. Configuration File (config.json):
#    - mt5_credentials: terminal_path, server, account_number, password.
#    - operational_mode: MODE_SEMI_AUTO or MODE_FULL_AUTO.
#    - scanner: timeframes, symbols, indicators, min_bars, num_bars, trend filter settings.
#    - trade_manager: margin_level_stop, default_lot_size, default_stop_loss_pips,
#      max_open_trades, max_spread_pips, pip_confirmation_threshold, pip_confirmation_timeout_sec,
#      strategy_settings for long_medium, scalping_strategy, cooperative.
#    - telegram: bot_token, chat_id, message_rate_limit.
#    - background_tasks_interval, initialized flag, scanner_running, trade_manager_running.
#
# 3. Scanner Configuration Flow (ConversationHandler States SC_*):
#    SC_TIMEFRAMES  - Enter timeframes or 'default'.
#    SC_SYMBOLS     - Enter symbols or 'default'.
#    SC_INDICATORS_BB_PERIOD - Enter Bollinger Bands period.
#    SC_INDICATORS_BB_DEV    - Enter BB deviation.
#    SC_INDICATORS_STOCH_K   - Enter Stochastic %K.
#    SC_INDICATORS_STOCH_D   - Enter Stochastic %D.
#    SC_INDICATORS_STOCH_SLOW - Enter Stochastic slowing.
#    SC_INDICATORS_STOCH_OVERSOLD - Enter Stochastic oversold.
#    SC_INDICATORS_STOCH_OVERBOUGHT - Enter Stochastic overbought.
#    SC_INDICATORS_RSI_PERIOD - Enter RSI period.
#    SC_INDICATORS_RSI_OVERSOLD - Enter RSI oversold.
#    SC_INDICATORS_RSI_OVERBOUGHT - Enter RSI overbought.
#    SC_TREND_TF - Enter trend filter timeframe (FULL_AUTO).
#    SC_TREND_USE_BB - Choose if BB is referenced for trend bias.
#
# 4. Trade Manager Configuration Flow (States TM_*):
#    TM_COOP_INCLUDE   - Include cooperative scalping block.
#    TM_COOP_LOT       - Cooperative scalping lot size.
#    TM_COOP_PROFIT    - Cooperative scalping profit target.
#    TM_COOP_SL        - Cooperative scalping stop loss.
#    TM_MARGIN_LEVEL   - Margin level stop.
#    TM_DEFAULT_LOT    - Default lot size.
#    TM_DEFAULT_SL     - Default stop-loss pips.
#    TM_MAX_TRADES     - Maximum total open trades.
#    TM_MAX_SPREAD     - Maximum allowed spread.
#    TM_PIP_CONFIRM_THRESH  - Pip confirmation threshold (FULL_AUTO).
#    TM_PIP_CONFIRM_TIMEOUT - Pip confirmation timeout seconds (FULL_AUTO).
#    TM_PIP_CONFIRM_INTERVAL - Pip confirmation polling interval seconds (FULL_AUTO).
#    TM_LOCKIN_METHOD  - Lock-in method (fixed or trailing).
#    TM_LOCKIN_VALUE   - Lock-in trigger/value in X/Y format.
#    TM_OPEN_MODE      - Sequential or concurrent trade opening.
#
# 5. Runtime Modes:
#    MODE_SEMI_AUTO  - Manual confirm trades with inline buttons.
#    MODE_FULL_AUTO  - Auto-execute trades through filters (Trend, Pip Confirmation).
#
# 6. Scheduler & Jobs:
#    - Uses APScheduler for timeouts, scanner polling, trade-manager checks.
#    - _trigger_timeout auto-ends idle conversations.
#
# 7. Utility Functions:
#    - load_config()/save_config(): JSON I/O with deep merge logic.
#    - safe_reply()/safe_send_message(): Telegram messaging with rate-limit and retries.
#    - Callback data encoding/decoding with UUID tokens.
#
# 8. Cooperative Scalping Strategy:
#    - Enabled via SC_INCLUDE_SCALP prompt.
#    - Parameters: lot size, profit target USD, stop-loss USD default 50%.
#    - Integrated into trade placement logic under opened_by 'coop_scalp'.
#
# 9. Database (aiosqlite):
#    - Tables: signals (id, timestamp, symbol, timeframe, strategy, action, taken, trade_ticket).
#              trades  (ticket, symbol, strategy, open_time, close_time, open_price, close_price,
#                       volume, profit, pnl_pips, comment, opened_by, status).
#    - init_db(), insert_signal(), update_signal_status(), insert_trade(), update_trade_close(), update_trade_status().
#
# 10. MT5 Integration:
#    - init_mt5(): terminal initialization and login checks.
#    - fetch_history_data(): Retries, DataFrame conversion with pandas_ta.
#    - place_order_sync()/async: Synchronous and threaded order placement with full pre-trade checks.
#    - calculate_lock_in_sl(): Compute stop-loss price for USD lock-in.
#    - calculate_pnl_pips(): Compute P&L in pips.
# =============================================================================


from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

import asyncio

def main_loop():
    """Return the global asyncio loop, creating one if needed."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

# Event loop used for thread safe coroutine execution
GLOBAL_LOOP = main_loop()
import json
import logging
import math
import statistics
import time
import uuid
import inspect
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:  # pragma: no cover - environment may lack requests
    requests = None
    REQUESTS_AVAILABLE = False
    logging.warning("'requests' package not installed; news updates disabled.")
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set, Tuple, List
from threading import Lock, RLock
import threading
from apscheduler.job import Job
import traceback # Added for detailed error logging

import numpy as np
np.NaN = np.nan
import pandas as pd
import pandas_ta as ta
import pytz
import sqlite3
import os
import warnings
from telegram.warnings import PTBUserWarning

# Directory to store generated trade charts
SCREENSHOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
import aiosqlite
import MetaTrader5 as mt5
import telegram
from sklearn.model_selection import train_test_split
from sklearn.linear_model import SGDClassifier
import joblib
from tensorflow import keras
import optuna
from enum import Enum, auto
warnings.filterwarnings("ignore", category=PTBUserWarning)

# pandas_market_calendars provides accurate market hours but is optional
try:
    import pandas_market_calendars as mcal
    MCAL_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    mcal = None
    MCAL_AVAILABLE = False
    logging.warning(
        "'pandas_market_calendars' not installed; defaulting to basic 24/5 market hours. "
        "Install via 'pip install pandas_market_calendars' for accurate market-hour detection."
    )
from mt5_utils import init_mt5, fetch_history_data, initialize as mt5_utils_initialize
from trade_manager import get_trade_direction, calculate_lock_in_sl, parse_lock_in, calculate_pnl_pips, initialize as tm_initialize


# --- FINALIZE TRADE-MANAGER CONFIGURATION ---



# Initialize MetaTrader5 connection
if not mt5.initialize():
    logging.error('MT5 initialization failed')


# Mapping for order types since mt5.order_type_name is unavailable
ORDER_TYPE_NAMES = {
    mt5.ORDER_TYPE_BUY: 'BUY',
    mt5.ORDER_TYPE_SELL: 'SELL',
    mt5.ORDER_TYPE_BUY_LIMIT: 'BUY_LIMIT',
    mt5.ORDER_TYPE_SELL_LIMIT: 'SELL_LIMIT',
    mt5.ORDER_TYPE_BUY_STOP: 'BUY_STOP',
    mt5.ORDER_TYPE_SELL_STOP: 'SELL_STOP',
}


FX_CALENDAR = mcal.get_calendar('24/5') if MCAL_AVAILABLE else None

class BotState(Enum):
    ACTIVE = auto()
    IDLE = auto()
    RECOVERING = auto()


def is_market_open(now: Optional[datetime] = None) -> bool:
    """Return True if the market is currently open.

    Falls back to a naive 24/5 schedule when pandas_market_calendars is unavailable.
    """
    now = now or datetime.now(timezone.utc)
    if MCAL_AVAILABLE:
        try:
            schedule = FX_CALENDAR.schedule(start_date=now.date(), end_date=now.date())
            if schedule.empty:
                return False
            open_time = schedule.iloc[0]["market_open"].to_pydatetime()
            close_time = schedule.iloc[0]["market_close"].to_pydatetime()
            return open_time <= now <= close_time
        except Exception:
            logging.warning(
                "pandas_market_calendars lookup failed; falling back to naive 24/5 schedule"
            )

    wd = now.weekday()
    if wd == 5:
        return False
    if wd == 6 and now.hour < 22:
        return False
    if wd == 4 and now.hour >= 22:
        return False
    return True

def run_health_check() -> bool:
    """Ping MT5 to ensure the connection is alive."""
    try:
        symbol = config["scanner"]["symbols"][0]
    except Exception:
        return True
    with mt5_sync_lock:
        tick = mt5.symbol_info_tick(symbol)
    if not tick:
        logging.warning("Health check failed; reinitializing MT5")
        mt5.shutdown()
        return init_mt5()
    return True


from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

# --- FINALIZE TRADE-MANAGER CONFIGURATION ---



# Capture the main asyncio loop for thread-safe coroutines will be done in `main()`
# Import telegram module to catch specific errors

import sys
from telegram.ext import ContextTypes

async def cmd_start_scanner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Starts scanning and trade manager."""
    # ensure mt5_utils.initialize was run and MT5 is up
    if not MT5_UTILS_INITIALIZED:
        await safe_reply(update, context, "⚠️ MT5 utilities not initialized. Please restart the bot.")
        return
    if not runtime_state.get("mt5_initialized", False):
        if not init_mt5():
            await safe_reply(update, context, "⚠️ MT5 connection failed. Please check your credentials and restart the bot.")
            return
    # start scanner and trade-manager
    with STATE_LOCK:
        runtime_state["scanner_running"] = True
        runtime_state["trade_manager_running"] = True

    await safe_reply(update, context, "✅ Scanning started, and Trade manager started.")

async def cmd_stop_scanner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stops the scanner only, leaving the Trade Manager running."""
    with STATE_LOCK:
        runtime_state["scanner_running"] = False
    await safe_reply(update, context, "🛑 Scanner stopped. Trade Manager remains running.")


# Suppress noisy PTB HTTP logs unless needed for debugging
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.INFO) # Keep PTB core logs
logging.getLogger("apscheduler").setLevel(logging.ERROR)  # Suppress routine scheduler messages

last_message_time = 0  # Initialize for rate limiting
# =============================================================================
# Global States and Constants
# =============================================================================
MODEL_FILE_KERAS = "keras_trading_model.keras"
MODEL_FILE_SKLEARN = "sklearn_trading_model.pkl"
HISTORICAL_DATA_FILE = "historical_seed.csv"
AI_MODEL = None  # Loaded model instance
MODEL_MTIME = 0.0
_cached_training_df: Optional[pd.DataFrame] = None
_missing_rate_warnings: Set[Tuple[str, str]] = set()
_trend_warning_cache: Set[Tuple[str, str, str]] = set()
PIP_MULTIPLIER: int = 10 # Assuming 5-digit prices, adjust for others (e.g., JPY pairs)
MAGIC_DEFAULT: int = 123456 # For long_medium and manual trades managed by bot
MAGIC_SCALPING: int = 999999 # For cooperative scalping trades
MIN_TRAINING_ROWS: int = 25  # Minimum rows required to train AI model

# Operational Modes
MODE_SEMI_AUTO = "SEMI_AUTO"
MODE_FULL_AUTO = "FULL_AUTO"

# Default configuration structure
DEFAULT_CONFIG = {
    "mt5_credentials": {
        "terminal_path": "",
        "server": "",
        "account_number": 0,
        "password": "",
    },
    "operational_mode": MODE_FULL_AUTO,  # Added operational mode
    "scanner": {
        "timeframes": [],
        "symbols": [],
        "indicators": {
            "bollinger_period": 20,
            "bollinger_dev": 2.0,
            "stoch_k": 14,
            "stoch_d": 3,
            "stoch_slowing": 3,
            "stoch_oversold": 20,
            "stoch_overbought": 80,
            "rsi_period": 14, # Currently unused by default strategies but kept
            "rsi_oversold": 30,
            "rsi_overbought": 70,
        },
        "num_bars": 700, # Bars to fetch for indicator calculations
        "min_bars": 350, # Increased min bars for indicator stability
        # Overall Trend Filter settings (used in FULL_AUTO)
        "overall_trend_timeframe": "MN1", # Default higher TF for trend
        "trend_bb_period": 20, # BB period for trend filter (could use scanner BB, but separate allows flexibility)
        "trend_bb_dev": 2.0, # BB dev for trend filter
        "min_bars_trend": 300, # Min bars for trend TF (ensure enough data for TEMA/DEMA)
        "trend_use_bb": False, # Reference flag for using BB for trend bias
    },
    "trade_manager": {
        "margin_level_stop": 100.0,
        "default_lot_size": 0.01, # Used for manual trades and fallback
        "default_stop_loss_pips": 20.0, # Used for manual trades and fallback
        "max_open_trades": 10,
        "max_spread_pips": None, # None means no limit
        # Full Auto filtering settings
        "pip_confirmation_threshold": 0, # Pips movement needed after signal candle close in auto mode
        "pip_confirmation_timeout_sec": 180, # Max seconds to wait for pip confirmation
        "pip_confirm_check_interval_sec": 0.5, # How often to poll for pip confirmation
        "use_news_filter": False,
        "ai_training": False,
        # How often (in hours) to retrain the AI model when ai_training is enabled
        "ai_training_interval_hours": 24,
        "lock_in_method": "default",
        "open_mode": "default",

        # Strategy-specific settings (override defaults)
        "strategy_settings": {
            "long_medium": {
                "lot_size": 0.02, # Example override
                "stop_loss_pips": 50.0, # Example override
                "take_profit_pips": None, # Example fixed TP (or None for BBM)
                "take_profit_mode": "pips",
                "take_profit_value": None,
                "take_profit_bb_type": "middle",
            },
            "scalping_strategy": {
                "lot_size_multiplier": 0.5, # Multiplier of long_medium/default lot
                "stop_loss_pips": 10.0,
                "take_profit_pips": 5.0,
            },
            "cooperative": {
                # These settings define *when* scalping is enabled on a locked-in main trade
                "lock_in_method": None, # 'pips' or 'money' (USD)
                "lock_in_value": None, # Minimum value to lock before enabling scalping
            },
        },
    },
    "telegram": {
        "bot_token": "",
        "chat_id": 0,  # Default user chat or channel
        "channel_id": 0,  # Channel for trade notifications
        "message_rate_limit": 1.0,  # Seconds between messages
    },
    "background_tasks_interval": 60.0, # Interval for the background runner job
    "initialized": False, # MT5 initialized flag
    "scanner_running": False,
    "trade_manager_running": False, # Controls monitors and auto trading/management
    # cooperative_auto_trading_active flag is implicitly linked to operational_mode == FULL_AUTO
    # and the cooperative settings in trade_manager["strategy_settings"]
}

# Load configuration from file or use defaults
def save_config(cfg: Dict[str, Any], filename: str = "config.json") -> None:
    """Saves the current configuration to a file."""
    try:
        with open(filename, 'w') as f:
            cfg["initialized"] = True
            json.dump(cfg, f, indent=4)
        logging.info(f"Configuration saved to {filename}.")
    except Exception as e:
        logging.error(f"Failed to save configuration: {e}", exc_info=True)


AVAILABLE_TIMEFRAMES = [
    "M1", "M2", "M3", "M4", "M5", "M6", "M10", "M12", "M15", "M20", "M30",
    "H1", "H2", "H3", "H4", "H6", "H8", "H12", "D1", "W1", "MN1"
]

# Allowed Timeframes per Strategy (used in scanner config validation)
STRATEGY_ALLOWED_TFS = {
    "long_medium": ["D1", "W1", "MN1", "H4", "H12"], # Expanded slightly for flexibility
    "scalping_strategy": ["M1", "M5", "M15", "M30", "H1"],
}


# =============================================================================
# Callback Data Helpers (UUID-based, short token)
# =============================================================================
CALLBACK_DATA_CACHE: Dict[str, Dict[str, Any]] = {}
CALLBACK_DATA_LOCK = Lock()

def load_config(filename="config.json") -> Dict[str, Any]:
    """Load configuration from a file, overriding defaults with saved values."""
    try:
        with open(filename, 'r') as f:
            loaded = json.load(f)
    except FileNotFoundError:
        logging.info(f"Config file {filename} not found. Using default configuration.")
        return DEFAULT_CONFIG.copy()
    except json.JSONDecodeError:
        logging.error(f"Error decoding {filename}. Using default configuration.", exc_info=True)
        return DEFAULT_CONFIG.copy()
    # Start with defaults
    cfg = DEFAULT_CONFIG.copy()
    # Override top-level keys except nested ones
    for key, value in loaded.items():
        if key not in ("scanner", "trade_manager"):
            cfg[key] = value
    # Merge scanner settings
    if "scanner" in loaded and isinstance(loaded["scanner"], dict):
        cfg["scanner"] = DEFAULT_CONFIG["scanner"].copy()
        cfg["scanner"].update(loaded["scanner"])
        # Merge indicators separately
        if "indicators" in loaded["scanner"] and isinstance(loaded["scanner"]["indicators"], dict):
            cfg["scanner"]["indicators"] = DEFAULT_CONFIG["scanner"]["indicators"].copy()
            cfg["scanner"]["indicators"].update(loaded["scanner"]["indicators"])
    # Merge trade manager settings
    if "trade_manager" in loaded and isinstance(loaded["trade_manager"], dict):
        cfg["trade_manager"] = DEFAULT_CONFIG["trade_manager"].copy()
        cfg["trade_manager"].update(loaded["trade_manager"])
        # Merge strategy settings separately
        if "strategy_settings" in loaded["trade_manager"] and isinstance(loaded["trade_manager"]["strategy_settings"], dict):
            cfg["trade_manager"]["strategy_settings"] = DEFAULT_CONFIG["trade_manager"]["strategy_settings"].copy()
            cfg["trade_manager"]["strategy_settings"].update(loaded["trade_manager"]["strategy_settings"])

    # Environment variable overrides
    cfg["mt5_credentials"]["terminal_path"] = os.getenv(
        "MT5_TERMINAL_PATH", cfg["mt5_credentials"].get("terminal_path", "")
    )
    cfg["mt5_credentials"]["server"] = os.getenv(
        "MT5_SERVER", cfg["mt5_credentials"].get("server", "")
    )
    acc = os.getenv("MT5_ACCOUNT")
    if acc is not None:
        try:
            cfg["mt5_credentials"]["account_number"] = int(acc)
        except ValueError:
            logging.warning("Invalid MT5_ACCOUNT environment variable; using config value.")
    cfg["mt5_credentials"]["password"] = os.getenv(
        "MT5_PASSWORD", cfg["mt5_credentials"].get("password", "")
    )

    cfg["telegram"]["bot_token"] = os.getenv(
        "TELEGRAM_BOT_TOKEN", cfg["telegram"].get("bot_token", "")
    )
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if chat is not None:
        try:
            cfg["telegram"]["chat_id"] = int(chat)
        except ValueError:
            logging.warning("Invalid TELEGRAM_CHAT_ID environment variable; using config value.")
    channel = os.getenv("TELEGRAM_CHANNEL_ID")
    if channel is not None:
        try:
            cfg["telegram"]["channel_id"] = int(channel)
        except ValueError:
            logging.warning("Invalid TELEGRAM_CHANNEL_ID environment variable; using config value.")

    return cfg

config: Dict[str, Any] = load_config()
def encode_callback_data(data: Dict[str, Any]) -> str:
    """Encodes callback data into a unique token using UUID."""
    token = uuid.uuid4().hex[:8] # Reduced length for safety
    with CALLBACK_DATA_LOCK:
        CALLBACK_DATA_CACHE[token] = data
    # TODO: Add a cleanup mechanism for old tokens if memory becomes a concern
    # Simple cleanup: remove tokens older than e.g. 1 hour?
    # current_time = time.time()
    # to_remove = [t for t, d in CALLBACK_DATA_CACHE.items() if d.get("_timestamp", 0) < current_time - 3600]
    # for t in to_remove:
    #     del CALLBACK_DATA_CACHE[t]
    # Add timestamp to data for cleanup
    # data["_timestamp"] = time.time() # Might exceed max size depending on dict contents
    return token

def decode_callback_data(token: str) -> Optional[Dict[str, Any]]:
    """Decodes callback token to retrieve original data."""
    with CALLBACK_DATA_LOCK:
        return CALLBACK_DATA_CACHE.pop(token, None) # Use pop to remove data after use (one-time buttons)

def compute_required_bars(settings: Dict[str, Any]) -> Tuple[int, int]:
    """Return `(min_bars, num_bars)` for history based on indicator periods and trend filter."""
    inds = settings.get("indicators", {})
    bb_p = inds.get("bollinger_period", DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_period"])
    stoch_k = inds.get("stoch_k", DEFAULT_CONFIG["scanner"]["indicators"]["stoch_k"])
    rsi_p = inds.get("rsi_period", DEFAULT_CONFIG["scanner"]["indicators"]["rsi_period"])
    trend_p = settings.get("trend_bb_period", DEFAULT_CONFIG["scanner"]["trend_bb_period"])

    max_period = max(bb_p, stoch_k, rsi_p, trend_p)
    min_bars = max_period * 5
    num_bars = min_bars * 10
    return min_bars, num_bars

# =============================================================================
# Conversation States
# =============================================================================

# Scanner Config States (reordered, scalping removed)
(SC_START, SC_TIMEFRAMES, SC_SYMBOLS,
 SC_INDICATORS_BB_PERIOD, SC_INDICATORS_BB_DEV, SC_INDICATORS_STOCH_K, SC_INDICATORS_STOCH_D,
 SC_INDICATORS_STOCH_SLOW, SC_INDICATORS_STOCH_OVERSOLD, SC_INDICATORS_STOCH_OVERBOUGHT,
 SC_INDICATORS_RSI_PERIOD, SC_INDICATORS_RSI_OVERSOLD, SC_INDICATORS_RSI_OVERBOUGHT,
 SC_TREND_TF, SC_TREND_USE_BB) = range(15)


# Trade Manager Config States (scalping moved here)
(TM_START, TM_COOP_INCLUDE, TM_COOP_LOT, TM_COOP_PROFIT, TM_COOP_SL,
 TM_MARGIN_LEVEL, TM_DEFAULT_LOT, TM_DEFAULT_SL, TM_MAX_TRADES,
 TM_MAX_SPREAD, TM_PIP_CONFIRM_THRESH, TM_PIP_CONFIRM_TIMEOUT, TM_PIP_CONFIRM_INTERVAL,
 TM_USE_NEWS_FILTER, TM_AI_TRAINING, TM_LOCKIN_METHOD, TM_LOCKIN_VALUE, TM_OPEN_MODE,
 TM_LONG_TP_METHOD, TM_LONG_TP_VALUE, TM_LONG_TP_BB_TYPE,
 TM_STRATEGY_SETTINGS, TM_STRAT_SELECT, TM_STRAT_LM_LOT, TM_STRAT_LM_SL, TM_STRAT_LM_TP,
 TM_STRAT_SC_LOT_MULT, TM_STRAT_SC_SL, TM_STRAT_SC_TP,
 TM_COOP_METHOD, TM_COOP_VALUE) = range(14, 45)


# Specific Command States
LIMIT_ORDER_ENTRY, LIMIT_ORDER_SL = range(45, 47)
LOCK_IN_PIPS_MANUAL = 47 # Renamed for clarity in manual lock-in
# FULL_AUTO config states are integrated into TM_CONFIG


# =============================================================================
# Global Runtime Variables
# =============================================================================
mt5_sync_lock = RLock() # Reentrant lock for all MT5 calls
# Use a separate lock for runtime state flags if they can be accessed by multiple threads/async tasks
# Though with PTB JobQueue and handler structure, often less critical if access is mostly in main loop
STATE_LOCK = Lock() # Added lock for flags like scanner_running
runtime_state: Dict[str, Any] = {
    "mt5_initialized": False,
    "scanner_running": False,
    "trade_manager_running": False, # Controls monitors etc.
    "scanner_auto_paused": False,   # Auto pause flag when max trades reached
    "last_scanner_run": datetime.now(timezone.utc),
    "last_trade_manager_run": datetime.now(timezone.utc),
    "market_state": BotState.ACTIVE,
    # cooperative_auto_trading_active is now derived from config["operational_mode"] == MODE_FULL_AUTO
}

# News filter globals
NEWS_API_URL = "https://www.jblanked.com/news/api/list/"
NEWS_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "jfqloeYH.3l6LGtc4QFhhhM0lLrFshB2rUiKcCWSf",
}
NEWS_EVENTS: List[Dict[str, Any]] = []
NEWS_FILTER_STATE: Dict[str, bool] = {}


INDICATORS_CACHE: Dict[str, Dict[str, Any]] = {} # Consider TTL or size limit if memory grows
last_bar_times: Dict[str, Dict[str, Optional[datetime]]] = {} # Tracks last bar time for each symbol/tf

# Cooperative Mode State
# Stores active LM trades and their associated scalping status, PnL, and open scalp tickets
# Key: symbol, Value: Dict
# state = {
#    "long_medium_pending_order": ticket, # If waiting for activation
#    "long_medium_ticket": ticket,
#    "direction": "buy" | "sell",
#    "lock_in_status": "pending_activation" | "pending" | "achieved" | "stopped_risk",
#    "locked_in_profit_usd": float, # The USD amount locked in
#    "scalping_status": "inactive" | "active" | "stopped_risk",
#    "current_scalping_pnl_usd": float, # Cumulative PnL from associated scalp trades for this pair
#    "open_scalp_tickets": List[int], # List of active scalp trade tickets for this pair
#    "parent_tf": str, # Timeframe of the long_medium signal
#    "bbm_tp": float, # Original TP based on BBM (for reference)
#    "signal_id": int, # ID of the signal that triggered the parent trade
# }
cooperative_states: Dict[str, Dict[str, Any]] = {}

# Tracks trades detected by the bot (either via signals, manual detection, or coop auto)
# Key: position_ticket, Value: Dict (lightweight info)
# open_trades = { ticket: { "symbol": str, "strategy": str, "opened_by": str, ... }, ... }
# Used by monitors to quickly check if a position is one the bot should manage/track
open_trades: Dict[int, Dict[str, Any]] = {}
MT5_UTILS_INITIALIZED = False
try:
    mt5_utils_initialize(config, runtime_state, STATE_LOCK, mt5_sync_lock, PIP_MULTIPLIER)
    MT5_UTILS_INITIALIZED = True
except Exception as e:
    logging.error(f"mt5_utils.initialize failed: {e}")
tm_initialize(mt5_sync_lock, PIP_MULTIPLIER)

# Tracks manually opened trades *already processed* by the manual_trade_detector once
# Used to prevent reprocessing the same manual trade on every monitor cycle
processed_manual_trades: Set[int] = set()

# Tracks trades waiting for Pip Confirmation in FULL_AUTO mode
# Key: f"{symbol}_{timeframe}_{action}", Value: Dict
# confirmation_pending_trades = { key: {
#    "signal_id": int,
#    "symbol": str,
#    "timeframe": str,
#    "strategy": str,
#    "action": str, # BUY/SELL
#    "signal_candle_close_price": float,
#    "required_price": float, # Calculated price based on confirmation threshold
#    "start_time": datetime, # When confirmation started
#    "prob": float,  # AI confidence probability
#    "parent_trade_data": Dict, # Info about the potential parent LM trade for coop mode
# }, ...}
confirmation_pending_trades: Dict[str, Dict[str, Any]] = {}
CONFIRMATION_PENDING_LOCK = Lock() # Lock for confirmation_pending_trades dict
pip_monitor_job: Optional[Job] = None

def maybe_stop_pip_monitor() -> None:
    """Unschedule the pip confirmation monitor if no trades are pending."""
    global pip_monitor_job
    with CONFIRMATION_PENDING_LOCK:
        empty = not confirmation_pending_trades
    if empty and pip_monitor_job:
        pip_monitor_job.schedule_removal()
        pip_monitor_job = None

# Time of last successful check for closed trades
last_trade_history_check_time = datetime.now(pytz.utc)

# Basic Trailing Stop settings (global or per-symbol if extended)
# trailing_settings = { symbol or 'global': {"method": 'pips'|'money', "value": float, "active": bool}, ...}
trailing_settings: Dict[str, Dict[str, Any]] = {}

async def sync_open_trades() -> None:
    """Synchronize the in-memory open_trades with current MT5 positions."""
    try:
        with mt5_sync_lock:
            positions = mt5.positions_get()
        if positions is None:
            logging.error("sync_open_trades: failed to fetch positions from MT5")
            return

        current_tickets = {p.ticket for p in positions}

        for pos in positions:
            if pos.ticket not in open_trades and pos.magic in (MAGIC_DEFAULT, MAGIC_SCALPING):
                open_trades[pos.ticket] = {
                    "symbol": pos.symbol,
                    "strategy": "long_medium" if pos.magic == MAGIC_DEFAULT else "scalping_strategy",
                    "opened_by": "bot_signal",
                }

        for ticket in list(open_trades.keys()):
            if ticket not in current_tickets:
                open_trades.pop(ticket, None)
                await update_trade_status(ticket, "closed")
    except Exception as e:
        logging.error(f"Error in sync_open_trades: {e}", exc_info=True)

# =============================================================================
# Database Functions (Asyncio)
# =============================================================================
DB_FILE = "trading_bot.db"



async def cmd_stop_scanner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stops scanning (Trade Manager continues)."""
    with STATE_LOCK:
        runtime_state["scanner_running"] = False
    # Note: Trade Manager remains running
    await safe_reply(update, context, "🛑 Scanner stopped. Trade Manager remains running.")


async def init_db() -> None:
    """Initializes SQLite database asynchronously."""
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("""CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                strategy TEXT NOT NULL,
                action TEXT NOT NULL,
                bb_upper REAL,
                bb_lower REAL,
                taken INTEGER DEFAULT 0, -- 0: pending, 1: taken (trade attempted), -1: dismissed/ignored
                trade_ticket INTEGER -- Ticket of the placed order/position (if any)
            )""")
            await db.execute("""CREATE TABLE IF NOT EXISTS trades (
                ticket INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                strategy TEXT NOT NULL,
                open_time DATETIME NOT NULL,
                close_time DATETIME,
                open_price REAL NOT NULL,
                close_price REAL,
                volume REAL NOT NULL,
                profit REAL, -- Net profit after commission/swap
                pnl_pips REAL,
                comment TEXT,
                opened_by TEXT -- e.g., 'bot_signal', 'manual_detected', 'signal_click', 'limit_manual', 'coop_long_medium', 'coop_scalp', 'confirmation_auto'
            )""")
            # Add status field to trades for better tracking (e.g., 'open', 'closed', 'pending')
            # Add the status column only if it does not already exist
            cur = await db.execute("PRAGMA table_info(trades)")
            cols = [row[1] async for row in cur]
            if "status" not in cols:
                await db.execute("ALTER TABLE trades ADD COLUMN status TEXT DEFAULT 'open'")
            # Add a unique constraint to prevent duplicate signals based on key fields within a short time window? (More advanced)
            # For now, rely on sequential processing/bar-based signals.
            await db.execute("CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals (timestamp);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_close_time ON trades (close_time);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status);")
            await db.commit()
            logging.info("Database initialized successfully.")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}", exc_info=True)
        raise # Re-raise to potentially stop the bot if DB is critical

async def insert_signal(timestamp: str, symbol: str, timeframe: str, strategy: str, action: str,
                        bb_upper: Optional[float] = None, bb_lower: Optional[float] = None) -> Optional[int]:
    """Inserts a trading signal into the database."""
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            cursor = await db.execute(
                """INSERT INTO signals (timestamp, symbol, timeframe, strategy, action, bb_upper, bb_lower, taken)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (timestamp, symbol, timeframe, strategy, action, bb_upper, bb_lower, 0) # Initially not taken
            )
            await db.commit()
            signal_id = cursor.lastrowid
            logging.debug(f"Inserted signal for {symbol} {timeframe} {action}, ID: {signal_id}")
            return signal_id
    except Exception as e:
        logging.error(f"Failed to insert signal: {e}", exc_info=True)
        return None

async def update_signal_status(signal_id: int, taken_status: int, trade_ticket: Optional[int] = None) -> None:
    """Updates a signal's taken status and associates a trade ticket."""
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                """UPDATE signals SET taken=?, trade_ticket=? WHERE id=?""",
                (taken_status, trade_ticket, signal_id)
            )
            await db.commit()
            logging.debug(f"Updated signal ID {signal_id} status to {taken_status} with ticket {trade_ticket}")
    except Exception as e:
        logging.error(f"Failed to update signal {signal_id}: {e}", exc_info=True)


async def insert_trade(ticket: int, symbol: str, strategy: str, open_time: float, open_price: float,
                       volume: float, comment: str, opened_by: str) -> None:
    """Inserts a trade into the database, initially as 'open'."""
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                """INSERT INTO trades (ticket, symbol, strategy, open_time, open_price, volume, comment, opened_by, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ticket, symbol, strategy, datetime.fromtimestamp(open_time, pytz.utc), open_price, volume, comment, opened_by, 'open')
            )
            await db.commit()
            logging.info(f"Inserted trade: Ticket={ticket}, Symbol={symbol}, Strategy={strategy}, By={opened_by}, Status='open'")
    except sqlite3.IntegrityError:
         logging.warning(f"Trade ticket {ticket} already exists in DB. Skipping insertion.") # Handle potential duplicates from monitors
    except Exception as e:
        logging.error(f"Failed to insert trade {ticket}: {e}", exc_info=True)


async def update_trade_close(ticket: int, close_time: datetime, close_price: float, profit: float, pnl_pips: Optional[float] = None) -> None:
    """Updates trade details upon closure and sets status to 'closed'."""
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                """UPDATE trades SET close_time=?, close_price=?, profit=?, pnl_pips=?, status='closed' WHERE ticket=?""",
                (close_time, close_price, profit, pnl_pips, ticket)
            )
            await db.commit()
            # Check if any row was actually updated
            async with db.execute("SELECT changes() as changes") as cursor:
                row = await cursor.fetchone()
                if row and row[0] > 0:
                    logging.info(f"Updated closed trade: Ticket={ticket}, Profit={profit:.2f}, Pips={pnl_pips}, Status='closed'")
                    if ticket in AI_DECISIONS:
                        if profit > 0:
                            AI_STATS["accepted_profit"] += 1
                        else:
                            AI_STATS["accepted_loss"] += 1
                        AI_DECISIONS.pop(ticket, None)
                        logging.info(f"AI Stats: {AI_STATS}")
                else:
                    logging.warning(f"Update failed: Trade ticket {ticket} not found in DB.") # Trade was perhaps not inserted initially?
    except Exception as e:
        logging.error(f"Failed to update closed trade {ticket}: {e}", exc_info=True)

async def update_trade_status(ticket: int, status: str) -> None:
     """Updates the status of a trade in the database."""
     try:
          async with aiosqlite.connect(DB_FILE) as db:
              await db.execute("""UPDATE trades SET status=? WHERE ticket=?""", (status, ticket))
              await db.commit()
              # Check if any row was updated
              async with db.execute("SELECT changes() as changes") as cursor:
                  row = await cursor.fetchone()
                  if row and row[0] > 0:
                       logging.debug(f"Updated trade {ticket} status to '{status}'")
                  else:
                       logging.warning(f"Update status failed: Trade ticket {ticket} not found in DB.")
     except Exception as e:
          logging.error(f"Failed to update trade status {ticket}: {e}", exc_info=True)


# =============================================================================
# AI Training & Prediction Helpers
# =============================================================================

# Track how much training data has been used previously.  This allows the
# periodic training job to skip expensive retraining runs when no new data (or
# too little new data) has been collected since the last model update.
_last_training_sample_count = 0
# Minimum number of fresh samples required to trigger a new training run.
MIN_NEW_TRAINING_SAMPLES = 5

# Track AI decision statistics
AI_DECISIONS: Dict[int, bool] = {}
AI_STATS = {
    "accepted": 0,
    "rejected": 0,
    "accepted_profit": 0,
    "accepted_loss": 0,
}


def _timeframe_to_timedelta(timeframe: str) -> timedelta:
    """Convert MT5 timeframe string (e.g., 'M5', 'H1') to pandas Timedelta."""
    unit = ''.join(filter(str.isalpha, timeframe))
    value = int(''.join(filter(str.isdigit, timeframe)) or 0)
    if unit == "M":
        return timedelta(minutes=value)
    if unit == "H":
        return timedelta(hours=value)
    if unit == "D":
        return timedelta(days=value)
    if unit == "W":
        return timedelta(weeks=value)
    if unit == "MN":
        return timedelta(days=30 * value)
    return timedelta(minutes=1)


def _compute_features(symbol: str, timeframe: str, timestamp: float) -> Tuple[float, float, float, float]:
    """Fetch price history and compute RSI, Stochastic %K/%D, and momentum."""
    global _missing_rate_warnings
    try:
        tf = getattr(mt5, f"TIMEFRAME_{timeframe}", None)
        if tf is None:
            return float("nan"), float("nan"), float("nan"), float("nan")
        end = datetime.fromtimestamp(timestamp, timezone.utc)
        delta = _timeframe_to_timedelta(timeframe)
        start = end - delta * 50
        rates = mt5.copy_rates_range(symbol, tf, start, end)
        if rates is None or len(rates) == 0:
            key = (symbol, timeframe)
            if key not in _missing_rate_warnings:
                logging.warning(
                    "No MT5 rates for %s %s at %s; using default features.",
                    symbol,
                    timeframe,
                    end,
                )
                _missing_rate_warnings.add(key)
            return 0.0, 0.0, 0.0, 0.0
        df = pd.DataFrame(rates)
        close = df["close"]
        high = df["high"]
        low = df["low"]
        rsi_series = ta.rsi(close)
        stoch_df = ta.stoch(high, low, close)
        rsi_val = float(rsi_series.iloc[-1]) if not rsi_series.empty else float("nan")
        stoch_k = float(stoch_df.iloc[-1, 0]) if not stoch_df.empty else float("nan")
        stoch_d = float(stoch_df.iloc[-1, 1]) if stoch_df.shape[1] > 1 else float("nan")
        momentum = float(close.diff().iloc[-1]) if len(close) > 1 else float("nan")
        return rsi_val, stoch_k, stoch_d, momentum
    except Exception as e:
        logging.error(
            "Indicator computation failed for %s %s: %s. Using default features.",
            symbol,
            timeframe,
            e,
        )
        return 0.0, 0.0, 0.0, 0.0

def _build_training_data() -> Optional[pd.DataFrame]:
    """Load signal/trade history for model training."""
    try:
        conn = sqlite3.connect(DB_FILE)
        df = pd.read_sql_query(
            """
            SELECT s.bb_upper, s.bb_lower, s.action, s.symbol, s.timeframe, s.timestamp, t.profit
            FROM signals s
            JOIN trades t ON s.trade_ticket = t.ticket
            WHERE t.profit IS NOT NULL
            """,
            conn,
        )
        conn.close()
        if df.empty:
            if os.path.exists(HISTORICAL_DATA_FILE):
                df = pd.read_csv(HISTORICAL_DATA_FILE)
                df.columns = [c.strip() for c in df.columns]
                logging.info(
                    "Loaded historical seed file %s with columns: %s",
                    os.path.abspath(HISTORICAL_DATA_FILE),
                    ", ".join(df.columns),
                )
            else:
                logging.warning(
                    "Historical seed file not found: %s",
                    os.path.abspath(HISTORICAL_DATA_FILE),
                )
                return None
        elif len(df) < MIN_TRAINING_ROWS:
            if os.path.exists(HISTORICAL_DATA_FILE):
                seed_df = pd.read_csv(HISTORICAL_DATA_FILE)
                seed_df.columns = [c.strip() for c in seed_df.columns]
                logging.info(
                    "Augmenting training data with seed file %s (columns: %s)",
                    os.path.abspath(HISTORICAL_DATA_FILE),
                    ", ".join(seed_df.columns),
                )
                df = pd.concat([df, seed_df], ignore_index=True)
            else:
                logging.warning(
                    "Seed file missing; cannot augment training data: %s",
                    os.path.abspath(HISTORICAL_DATA_FILE),
                )
        if df.empty:
            logging.warning("Training data is empty after loading.")
            return None
        required = {"bb_upper", "bb_lower", "action", "symbol", "timeframe", "timestamp", "profit"}
        missing = required.difference(df.columns)
        if missing:
            logging.error(
                "Training data missing required columns: %s",
                ", ".join(sorted(missing)),
            )
            return None
        df.dropna(inplace=True)
        feature_rows = df.apply(
            lambda r: _compute_features(
                r["symbol"], r["timeframe"], pd.Timestamp(r["timestamp"]).timestamp()
            ),
            axis=1,
        )
        feature_df = pd.DataFrame(
            feature_rows.tolist(),
            columns=["rsi", "stoch_k", "stoch_d", "momentum"],
            index=df.index,
        )
        feature_df.fillna(0, inplace=True)
        df = pd.concat([df, feature_df], axis=1)
        df.fillna(0, inplace=True)
        df["action"] = df["action"].map({"BUY": 1, "SELL": 0})
        df["label"] = (df["profit"] > 0).astype(int)
        return df[["bb_upper", "bb_lower", "action", "rsi", "stoch_k", "stoch_d", "momentum", "label"]]
    except Exception as e:
        logging.error("Failed building training data: %s", e, exc_info=True)
        return None
def _train_ai_model_sync() -> None:
    """Train the Keras model with Optuna optimization."""
    global _last_training_sample_count, _cached_training_df
    df = _cached_training_df if _cached_training_df is not None else _build_training_data()
    _cached_training_df = None

    if df is None or len(df) < MIN_TRAINING_ROWS:
        logging.warning("Not enough data to train AI model.")
        return

    new_samples = len(df) - _last_training_sample_count
    if new_samples < MIN_NEW_TRAINING_SAMPLES:
        logging.info(
            f"AI training skipped: only {new_samples} new samples since last training."
        )
        return

    feature_cols = ["bb_upper", "bb_lower", "action", "rsi", "stoch_k", "stoch_d", "momentum"]
    X = df[feature_cols]
    y = df["label"]

    backend = config["trade_manager"].get("ai_backend", "keras")
    model_file = MODEL_FILE_KERAS if backend == "keras" else MODEL_FILE_SKLEARN

    if backend == "keras":
        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

        def objective(trial):
            keras.backend.clear_session()
            units = trial.suggest_int("units", 16, 64, step=16)
            dropout = trial.suggest_float("dropout", 0.0, 0.5)
            epochs = trial.suggest_int("epochs", 5, 20)
            model = keras.Sequential([
                keras.layers.Input(shape=(X_train.shape[1],)),
                keras.layers.Dense(units, activation="relu"),
                keras.layers.Dropout(dropout),
                keras.layers.Dense(1, activation="sigmoid"),
            ])
            model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
            model.fit(X_train, y_train, epochs=epochs, verbose=0)
            _, acc = model.evaluate(X_val, y_val, verbose=0)
            return 1 - acc

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=5)
        params = study.best_trial.params

        keras.backend.clear_session()
        final_model = keras.Sequential([
            keras.layers.Input(shape=(X_train.shape[1],)),
            keras.layers.Dense(params["units"], activation="relu"),
            keras.layers.Dropout(params["dropout"]),
            keras.layers.Dense(1, activation="sigmoid"),
        ])
        final_model.compile(optimizer="adam", loss="binary_crossentropy", metrics=["accuracy"])
        final_model.fit(X_train, y_train, epochs=params["epochs"], verbose=0)
        final_model.save(model_file)
    else:
        if os.path.exists(model_file):
            model = joblib.load(model_file)
            model.partial_fit(X, y)
        else:
            model = SGDClassifier(loss="log_loss", max_iter=1000)
            model.partial_fit(X, y, classes=np.array([0, 1]))
        joblib.dump(model, model_file)

    logging.info("AI model trained and saved.")
    _last_training_sample_count = len(df)


async def train_ai_model() -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _train_ai_model_sync)


def _seed_training_data_sync() -> int:
    """Seed training data if needed and return available row count."""
    global _cached_training_df
    df = _build_training_data()
    if df is None:
        logging.error(
            "Training data build failed; required columns may be missing or seed file absent."
        )
        _cached_training_df = None
        return 0
    _cached_training_df = df
    rows = len(df)
    logging.info("Training data prepared with %d rows.", rows)
    return rows

async def seed_training_data() -> int:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _seed_training_data_sync)


def _load_model():
    global AI_MODEL, MODEL_MTIME
    backend = config["trade_manager"].get("ai_backend", "keras")
    model_file = MODEL_FILE_KERAS if backend == "keras" else MODEL_FILE_SKLEARN
    if not os.path.exists(model_file):
        AI_MODEL = None
        return None
    mtime = os.path.getmtime(model_file)
    if AI_MODEL is None or mtime != MODEL_MTIME:
        if backend == "keras":
            AI_MODEL = keras.models.load_model(model_file)
        else:
            AI_MODEL = joblib.load(model_file)
        MODEL_MTIME = mtime
        logging.info("AI model loaded from disk.")
    return AI_MODEL


def _predict_sync(action: str, bb_upper: float, bb_lower: float, symbol: str, timeframe: str) -> Tuple[bool, float]:
    """Return AI decision bool and raw probability synchronously."""
    model = _load_model()
    if model is None:
        return True, 1.0
    rsi, stoch_k, stoch_d, momentum = _compute_features(symbol, timeframe, datetime.now(timezone.utc).timestamp())
    data = np.array([[bb_upper, bb_lower, 1 if action == "BUY" else 0, rsi, stoch_k, stoch_d, momentum]])
    backend = config["trade_manager"].get("ai_backend", "keras")
    if backend == "keras":
        pred = model.predict(data, verbose=0)
        prob = float(pred[0] if pred.ndim == 1 else pred[0][0])
    else:
        prob = float(model.predict_proba(data)[0][1])
    decision = prob >= 0.5
    if decision:
        AI_STATS["accepted"] += 1
    else:
        AI_STATS["rejected"] += 1
    return decision, prob


async def ai_decision(action: str, bb_upper: float, bb_lower: float, symbol: str, timeframe: str) -> Tuple[bool, float]:
    """Run AI prediction asynchronously and return decision and probability."""
    if not config["trade_manager"].get("ai_training"):
        return True, 1.0
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _predict_sync, action, bb_upper, bb_lower, symbol, timeframe)


# =============================================================================
# Helper Functions
# =============================================================================
async def cfg_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the current conversation."""
    message_sent = False
    if update.message:
        await safe_reply(update, context, "⛔️ Configuration canceled.")
        message_sent = True
    elif update.callback_query:
        # Try to edit the message if it originated from a button, fallback to new message
        try:
            await update.callback_query.edit_message_text("⛔️ Configuration canceled.", reply_markup=None)
            await update.callback_query.answer()
            message_sent = True
        except telegram.error.BadRequest:
             # Message might be too old to edit, send a new one
             await safe_send_message(context, update.effective_chat.id, "⛔️ Configuration canceled.")
             message_sent = True
        except Exception as e:
             logging.error(f"Error during cfg_cancel callback edit/answer: {e}", exc_info=True)


    if not message_sent:
         logging.warning("cfg_cancel called without update.message or update.callback_query. Sending generic cancel message.")
         # Fallback just in case, though should be covered by checks above
         await safe_send_message(context, config["telegram"]["chat_id"], "⛔️ Configuration canceled.")


    context.user_data.clear() # Clear any partial data for this user
    return ConversationHandler.END

async def safe_send_message(context: ContextTypes.DEFAULT_TYPE, chat_id: Optional[int] = None, text: str = "",
                            reply_markup: Optional[InlineKeyboardMarkup] = None,
                            parse_mode: Optional[str] = ParseMode.HTML,
                            is_reply: bool = False, reply_to_message_id: Optional[int] = None) -> bool:
    """Sends a message to Telegram with rate limiting, retries, and error logging. Returns True on success."""
    global last_message_time
    target_chat_id = chat_id or config["telegram"]["chat_id"]
    if not target_chat_id:
        logging.error("Cannot send message: chat_id is not configured or provided.")
        return False
    if not text:
        logging.debug("Suppressed empty message send.")
        return False

    # Rate Limiting
    try:
        current_time = time.time()
        delay = config["telegram"]["message_rate_limit"] - (current_time - last_message_time)
        if delay > 0:
            await asyncio.sleep(delay)
    except Exception as e:
        logging.error(f"Error during rate limit delay calculation: {e}", exc_info=True)

    # Send with Retries
    retries = 3
    backoff = 1
    for attempt in range(retries):
        try:
            if is_reply and reply_to_message_id:
                 await context.bot.send_message(chat_id=target_chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)
            else:
                 await context.bot.send_message(chat_id=target_chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
            last_message_time = time.time()
            logging.debug(f"Message sent/replied to {target_chat_id} (attempt {attempt+1})")
            return True # Success
        except telegram.error.TimedOut:
            logging.warning(f"Telegram send timed out (attempt {attempt + 1}/{retries}). Retrying in {backoff}s.")
            await asyncio.sleep(backoff)
            backoff *= 2
        except telegram.error.RetryAfter as e:
            wait_time = max(e.retry_after, backoff, 5) # Use suggested time or backoff
            logging.warning(f"Rate limited by Telegram (attempt {attempt + 1}/{retries}). Waiting {wait_time}s.")
            await asyncio.sleep(wait_time)
            backoff *= 2
        except telegram.error.BadRequest as e:
            logging.error(f"Telegram BadRequest sending to {target_chat_id}: {e}. Message: '{text[:100]}...' - Not retrying.", exc_info=True)
            return False # Don't retry bad requests
        except Exception as e:
            logging.error(f"Unexpected error sending Telegram message (attempt {attempt+1}/{retries}): {e}", exc_info=True)
            await asyncio.sleep(backoff)
            backoff *= 2

    logging.error(f"Failed to send message to {target_chat_id} after {retries} attempts. Message: '{text[:100]}...'")
    return False # Failed after retries

async def safe_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, **kwargs) -> bool:
    """Wrapper for safe_send_message using update's chat ID and reply mechanism."""
    chat_id = update.effective_chat.id if update.effective_chat else None
    if not chat_id:
         chat_id = config["telegram"]["chat_id"] # Fallback to config chat_id

    # If it's a message update, try reply_text, otherwise just send
    if update.message:
        return await safe_send_message(context, chat_id, text, is_reply=True, reply_to_message_id=update.message.message_id, **kwargs)
    elif update.callback_query and update.callback_query.message:
         # If callback query from a message, reply to that message
         return await safe_send_message(context, chat_id, text, is_reply=True, reply_to_message_id=update.callback_query.message.message_id, **kwargs)
    else:
        # If no message context, just send a new message
        return await safe_send_message(context, chat_id, text, **kwargs)


async def safe_send_photo(context: ContextTypes.DEFAULT_TYPE, photo_path: str, caption: str = "",
                          chat_id: Optional[int] = None) -> bool:
    """Send a photo to Telegram with retries and rate limiting."""
    global last_message_time
    target_chat_id = chat_id or config["telegram"].get("chat_id")
    if not target_chat_id or not photo_path:
        logging.error("Cannot send photo: missing chat_id or photo_path")
        return False

    try:
        current_time = time.time()
        delay = config["telegram"]["message_rate_limit"] - (current_time - last_message_time)
        if delay > 0:
            await asyncio.sleep(delay)
    except Exception as e:
        logging.error(f"Error during rate limit delay calculation: {e}", exc_info=True)

    retries = 3
    backoff = 1
    for attempt in range(retries):
        try:
            with open(photo_path, "rb") as img:
                await context.bot.send_photo(chat_id=target_chat_id, photo=img, caption=caption)
            last_message_time = time.time()
            logging.debug(f"Photo sent to {target_chat_id} (attempt {attempt+1})")
            return True
        except telegram.error.TimedOut:
            logging.warning(f"Telegram photo send timed out (attempt {attempt+1}/{retries}). Retrying in {backoff}s.")
            await asyncio.sleep(backoff)
            backoff *= 2
        except telegram.error.RetryAfter as e:
            wait_time = max(e.retry_after, backoff, 5)
            logging.warning(f"Rate limited by Telegram (attempt {attempt+1}/{retries}). Waiting {wait_time}s.")
            await asyncio.sleep(wait_time)
            backoff *= 2
        except telegram.error.BadRequest as e:
            logging.error(f"Telegram BadRequest sending photo to {target_chat_id}: {e}. Not retrying.", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"Unexpected error sending Telegram photo (attempt {attempt+1}/{retries}): {e}", exc_info=True)
            await asyncio.sleep(backoff)
            backoff *= 2

    logging.error(f"Failed to send photo to {target_chat_id} after {retries} attempts.")
    return False


def capture_trade_chart(symbol: str, timeframe: str, stop_loss: Optional[float]) -> Optional[str]:
    """Capture a chart screenshot for the given trade details."""
    os.makedirs(SCREENSHOT_DIR, exist_ok=True)
    filename = f"{symbol}_{int(time.time())}.png"
    path = os.path.join(SCREENSHOT_DIR, filename)
    tf = getattr(mt5, f"TIMEFRAME_{timeframe}", None)
    if tf is None:
        logging.error(f"Unknown timeframe {timeframe} for chart screenshot")
        return None
    chart_open = getattr(mt5, "chart_open", None)
    chart_screenshot = getattr(mt5, "chart_screenshot", None)
    chart_close = getattr(mt5, "chart_close", None)
    object_create = getattr(mt5, "object_create", None)
    object_set_integer = getattr(mt5, "object_set_integer", None)
    object_delete = getattr(mt5, "object_delete", None)
    if None in (chart_open, chart_screenshot, chart_close, object_create, object_set_integer, object_delete):
        logging.warning("MT5 chart functions unavailable; skipping chart capture")
        return None
    try:
        chart_id = chart_open(symbol, tf)
        if chart_id <= 0:
            logging.error(f"Failed to open chart for {symbol} {timeframe}")
            return None
        line_name = None
        if stop_loss is not None:
            line_name = f"sl_{uuid.uuid4().hex}"
            object_create(chart_id, line_name, mt5.OBJ_HLINE, 0, 0, stop_loss)
            object_set_integer(chart_id, line_name, mt5.OBJPROP_COLOR, 0x0000FF)
        time.sleep(0.2)
        chart_screenshot(chart_id, path, 1280, 720)
        if line_name:
            object_delete(chart_id, line_name)
        chart_close(chart_id)
        return path
    except Exception as e:
        logging.error(f"Error capturing chart for {symbol} {timeframe}: {e}", exc_info=True)
        return None


async def notify_trade_open(context: ContextTypes.DEFAULT_TYPE, symbol: str, timeframe: str,
                            entry_price: Optional[float], stop_loss: Optional[float], order_type: int) -> None:
    """Generate chart screenshot and send trade notification to the configured channel."""
    loop = asyncio.get_running_loop()
    try:
        chart_path = await loop.run_in_executor(None, capture_trade_chart, symbol, timeframe, stop_loss)
    except Exception as e:
        logging.error(f"Chart capture failed for {symbol} {timeframe}: {e}", exc_info=True)
        chart_path = None
    if not chart_path:
        return
    direction = get_trade_direction(order_type).upper()
    caption = (
        f"Entry: {entry_price if entry_price is not None else 'N/A'}\n"
        f"SL: {stop_loss if stop_loss is not None else 'N/A'}\n"
        f"Symbol: {symbol}\nType: {direction}"
    )
    await safe_send_photo(context, chart_path, caption=caption, chat_id=config["telegram"].get("channel_id"))


def _parse_event_time(event: Dict[str, Any]) -> Optional[datetime]:
    for key in ("time", "timestamp", "datetime", "date"):
        if key in event:
            try:
                val = event[key]
                if isinstance(val, (int, float)):
                    return datetime.fromtimestamp(val, tz=pytz.utc)
                return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
            except Exception:
                continue
    return None


def _event_symbols(event: Dict[str, Any]) -> List[str]:
    for key in ("symbols", "pairs", "pair", "symbol", "currency", "instrument"):
        if key in event:
            val = event[key]
            if isinstance(val, str):
                return [val]
            if isinstance(val, list):
                return [str(v) for v in val]
    return []


def event_affects_symbol(event: Dict[str, Any], symbol: str) -> bool:
    s = symbol.replace("/", "").upper()
    for ev_sym in _event_symbols(event):
        ev_sym = ev_sym.replace("/", "").upper()
        if s in ev_sym or ev_sym in s:
            return True
    return False


def should_close_for_news(symbol: str, now: Optional[datetime] = None) -> bool:
    if now is None:
        now = datetime.now(pytz.utc)
    for ev in NEWS_EVENTS:
        t = _parse_event_time(ev)
        if not t:
            continue
        if event_affects_symbol(ev, symbol) and timedelta(0) <= t - now <= timedelta(minutes=5):
            return True
    return False


def should_wait_for_news(symbol: str, now: Optional[datetime] = None) -> bool:
    if now is None:
        now = datetime.now(pytz.utc)
    for ev in NEWS_EVENTS:
        t = _parse_event_time(ev)
        if not t:
            continue
        if event_affects_symbol(ev, symbol) and -timedelta(minutes=5) <= t - now <= timedelta(minutes=5):
            return True
    return False


async def update_news_state(context: ContextTypes.DEFAULT_TYPE, symbol: str) -> None:
    """Notify on news filter activation/resumption for a symbol."""
    active = should_wait_for_news(symbol)
    prev = NEWS_FILTER_STATE.get(symbol)
    if active and not prev:
        NEWS_FILTER_STATE[symbol] = True
        await safe_send_message(context, f"📰 News filter activated for {symbol}. Trading paused.")
    elif not active and prev:
        NEWS_FILTER_STATE[symbol] = False
        await safe_send_message(context, f"📰 News filter cleared for {symbol}. Trading may resume.")


async def update_news_events() -> None:
    global NEWS_EVENTS
    if not REQUESTS_AVAILABLE:
        logging.debug("Skipping news update; 'requests' not installed")
        return
    try:
        resp = requests.get(NEWS_API_URL, headers=NEWS_HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, dict):
                NEWS_EVENTS = data.get("data") or data.get("news") or data.get("items") or []
            elif isinstance(data, list):
                NEWS_EVENTS = data
    except Exception as e:
        logging.error(f"Failed to fetch news events: {e}")

def place_order_sync(order_type: int, symbol: str, strategy: str, timeframe: str,
                     signal_id: Optional[int] = None,
                     lot_size: Optional[float] = None, # Specific lot size passed
                     sl_pips: Optional[float] = None, tp_pips: Optional[float] = None, # Specific pips SL/TP passed
                     entry_price: Optional[float] = None, # For pending orders
                     sl_price: Optional[float] = None, tp_price: Optional[float] = None, # Specific price SL/TP passed
                     opened_by: str = "bot_signal",
                     comment_suffix: str = "" # Optional suffix for the comment
                     ) -> Tuple[Optional[Any], Optional[int]]:
    """Places an order in MT5 synchronously and returns (result, position_ticket/order_ticket)."""
    result = None
    trade_ticket = None # This will be position ticket for market, order ticket for pending

    try:
        with mt5_sync_lock: # Ensure exclusive MT5 access
            # --- Pre-Trade Checks ---
            with STATE_LOCK:
                if not runtime_state["mt5_initialized"] or not mt5.terminal_info():
                    logging.error("MT5 not initialized or disconnected. Cannot place order.")
                    return None, None

            account_info = mt5.account_info()
            if not account_info:
                logging.error("Failed to get account info. Cannot place order.")
                return None, None

            # Check margin level against configured threshold
            if account_info.margin > 0 and account_info.margin_level < config["trade_manager"]["margin_level_stop"]:
                logging.warning(f"Margin level {account_info.margin_level:.2f}% below threshold ({config['trade_manager']['margin_level_stop']}%). Order rejected for {symbol}.")
                return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_NO_MONEY, "comment": "Margin level too low"}), None # Mimic result object

            # Check max open trades (only if placing a MARKET order - pending doesn't add to positions count immediately)
            is_pending_order_req = entry_price is not None # Check if the *request* is for a pending order
            # Prevent duplicate trades for same symbol
            existing_positions = mt5.positions_get(symbol=symbol)
            if existing_positions:
                logging.info(f"Trade for {symbol} already open. Skipping new trade.")
                return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_TOO_MANY_REQUESTS, "comment": "Duplicate trade for symbol"}), None
            if config["trade_manager"].get("open_mode") == "sequential":
                positions_all = mt5.positions_get() or []
                for pos in positions_all:
                    if pos.magic == MAGIC_DEFAULT:
                        state = cooperative_states.get(pos.symbol, {})
                        if state.get("lock_in_status") not in ("achieved", "stopped_risk"):
                            logging.info("Sequential mode: Rejecting new order until existing trade locks profit.")
                            return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_TOO_MANY_REQUESTS, "comment": "Sequential wait"}), None
            if config["trade_manager"].get("max_open_trades"):
                current_count = len(open_trades)
                if not is_pending_order_req:
                    positions = mt5.positions_get()
                    current_count = max(current_count, len(positions) if positions else 0)
                if current_count >= config["trade_manager"]["max_open_trades"]:
                    logging.info(
                        f"Max open trades ({config['trade_manager']['max_open_trades']}) reached. Order rejected for {symbol}."
                    )
                    return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_TOO_MANY_REQUESTS, "comment": "Max trades reached"}), None

            # News filter check (prevent trading around scheduled events)
            if config["trade_manager"].get("use_news_filter") and should_wait_for_news(symbol):
                logging.info(f"News filter active. Order rejected for {symbol}.")
                ret = getattr(mt5, "TRADE_RETCODE_TRADE_DISABLED", -100)
                return type("Result", (object,), {"retcode": ret, "comment": "News filter active"}), None


            # --- Symbol and Price Info ---
            # Select symbol if not already selected, this is important for getting correct ticks/info
            if not mt5.symbol_select(symbol, True):
                logging.warning(f"Failed to select symbol {symbol}, attempting anyway...")
                # Don't necessarily fail here, MT5 might allow trade anyway
            time.sleep(0.05) # Small delay after select


            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                logging.error(f"Symbol info not found for {symbol}. Cannot place order.")
                mt5.symbol_select(symbol, False) # Deselect if we failed
                return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_INVALID_PARAMETERS, "comment": "Symbol not found"}), None

            tick = mt5.symbol_info_tick(symbol)
            # Check for valid tick prices needed for Market/SL/TP calcs if not pending with explicit prices
            if tick is None or tick.ask == 0 or tick.bid == 0:
                 # If placing a pending order with explicit entry/sl/tp prices, ticks are less critical immediately
                 # But still need point/digits from symbol_info
                 if not is_pending_order_req or sl_price is None or tp_price is None:
                     logging.error(f"Cannot retrieve valid tick data for {symbol}. Cannot place order.")
                     mt5.symbol_select(symbol, False)
                     return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_NO_CHANGES, "comment": "Invalid tick data"}), None


            # --- Spread Check (only for market orders or pending near market) ---
            if not is_pending_order_req and config["trade_manager"]["max_spread_pips"] is not None and tick:
                 spread_points = (tick.ask - tick.bid) / symbol_info.point
                 current_spread_pips = spread_points / PIP_MULTIPLIER # Assuming point is size of one pip
                 if current_spread_pips > config["trade_manager"]["max_spread_pips"]:
                     logging.info(f"Spread too high for {symbol}: {current_spread_pips:.1f} pips > {config['trade_manager']['max_spread_pips']:.1f} pips. Market order rejected.")
                     mt5.symbol_select(symbol, False)
                     return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_PRICE_CHANGED, "comment": "Spread too high"}), None


            # --- Lot Size Calculation ---
            final_lot_size = config["trade_manager"]["default_lot_size"] # Start with default

            # Apply strategy-specific lot if available
            strat_settings = config["trade_manager"]["strategy_settings"].get(strategy, {})
            if "lot_size" in strat_settings and strat_settings["lot_size"] is not None:
                 final_lot_size = strat_settings["lot_size"]
                 logging.debug(f"Using strategy-specific lot size {final_lot_size} for {strategy}")

            # Apply scalping multiplier if applicable
            # Note: The "opened_by" field is a better indicator for Scalping trades placed as part of the Coop logic
            if opened_by == "coop_scalp" and strategy == "scalping_strategy":
                scalping_settings = config["trade_manager"]["strategy_settings"].get("scalping_strategy", {})
                lot_multiplier = scalping_settings.get("lot_size_multiplier", 1.0)
                if lot_multiplier is not None: # Allow multiplier 0? Maybe?
                     final_lot_size *= lot_multiplier
                     logging.debug(f"Applied scalping lot multiplier {lot_multiplier}. Final lot: {final_lot_size}")


            # Validate and adjust Lot Size
            min_lot = symbol_info.volume_min
            max_lot = symbol_info.volume_max
            step_lot = symbol_info.volume_step

            # Adjust to step and min, then clamp to max
            adjusted_lot_size = math.floor(final_lot_size / step_lot) * step_lot
            final_lot_size = max(min_lot, adjusted_lot_size)
            final_lot_size = min(max_lot, final_lot_size)
            final_lot_size = min(final_lot_size, config["trade_manager"]["default_lot_size"])

            if final_lot_size < min_lot: # This case should theoretically not happen after max(min_lot, ...)
                 logging.error(f"Calculated/adjusted lot size {final_lot_size} is below minimum {min_lot} for {symbol}. Order rejected.")
                 mt5.symbol_select(symbol, False)
                 return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_INVALID_VOLUME, "comment": f"Lot size {final_lot_size} < min {min_lot}"}), None
            if final_lot_size > max_lot:
                 logging.warning(f"Calculated lot size {adjusted_lot_size} exceeds max volume {max_lot} for {symbol}. Clamping to max: {final_lot_size}.")


            # --- Determine Order Type and Price ---
            request_price = 0.0
            request_type = 0
            request_action = 0
            digits = symbol_info.digits # For rounding prices

            if is_pending_order_req and entry_price is not None:
                request_action = mt5.TRADE_ACTION_PENDING
                request_price = round(entry_price, digits)

                # Determine Limit/Stop type based on entry price relative to current market price (if available)
                if tick:
                     if order_type == mt5.ORDER_TYPE_BUY:
                         request_type = mt5.ORDER_TYPE_BUY_LIMIT if request_price < tick.ask else mt5.ORDER_TYPE_BUY_STOP
                     elif order_type == mt5.ORDER_TYPE_SELL:
                         request_type = mt5.ORDER_TYPE_SELL_LIMIT if request_price > tick.bid else mt5.ORDER_TYPE_SELL_STOP
                     else:
                         logging.error(f"Invalid base order_type {order_type} for pending order.")
                         mt5.symbol_select(symbol, False)
                         return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_INVALID_PARAMETERS, "comment": "Invalid pending type"}), None
                else:
                     # If no tick, cannot automatically determine Limit/Stop. Use base type? Or require explicit type?
                     # Let's assume the 'order_type' parameter directly indicates LIMIT/STOP for manual entry
                     # For auto coop entry, the logic placing the order *should* have decided type.
                     request_type = order_type # Trust the caller to pass BUY_LIMIT/STOP etc.
                     logging.warning(f"No tick data for {symbol}, using provided type {order_type} for pending order {request_price}.")

                order_name = ORDER_TYPE_NAMES.get(request_type, str(request_type))
                logging.info(f"Preparing PENDING {order_name} order for {symbol} at {request_price}")

            else: # Market Order (TRADE_ACTION_DEAL)
                request_action = mt5.TRADE_ACTION_DEAL
                request_type = order_type # Should be BUY or SELL
                if tick:
                     request_price = round(tick.ask if order_type == mt5.ORDER_TYPE_BUY else tick.bid, digits)
                else:
                     logging.error(f"No tick data for {symbol}. Cannot determine market price for {order_type} order.")
                     mt5.symbol_select(symbol, False)
                     return type("Result", (object,), {"retcode": mt5.TRADE_RETCODE_NO_CHANGES, "comment": "Invalid tick data"}), None

                order_name = ORDER_TYPE_NAMES.get(request_type, str(request_type))
                logging.info(f"Preparing MARKET {order_name} order for {symbol} at ~{request_price}")


            # --- Calculate and Apply SL/TP ---
            point = symbol_info.point
            request_sl = 0.0
            request_tp = 0.0

            # Priority: Explicit SL/TP Price > Explicit SL/TP Pips > Strategy-Specific Pips > Default Config Pips
            if sl_price is not None and sl_price > 0:
                request_sl = round(sl_price, digits)
                logging.debug(f"Using explicit SL price: {request_sl}")
            elif sl_pips is not None and sl_pips > 0:
                 # Calculate SL price based on Pips from the EXECUTION price (request_price for pending, market price for deal)
                 price_for_sl_calc = request_price # Use the price at which the order is intended/executed
                 if order_type == mt5.ORDER_TYPE_BUY:
                      request_sl = round(price_for_sl_calc - sl_pips * point * PIP_MULTIPLIER, digits)
                 elif order_type == mt5.ORDER_TYPE_SELL:
                      request_sl = round(price_for_sl_calc + sl_pips * point * PIP_MULTIPLIER, digits)
                 logging.debug(f"Using explicit SL pips: {sl_pips}, calculated SL price: {request_sl}")
            elif strat_settings.get("stop_loss_pips") is not None and strat_settings["stop_loss_pips"] > 0:
                 sl_pips_calc = strat_settings["stop_loss_pips"]
                 price_for_sl_calc = request_price
                 if order_type == mt5.ORDER_TYPE_BUY:
                      request_sl = round(price_for_sl_calc - sl_pips_calc * point * PIP_MULTIPLIER, digits)
                 elif order_type == mt5.ORDER_TYPE_SELL:
                      request_sl = round(price_for_sl_calc + sl_pips_calc * point * PIP_MULTIPLIER, digits)
                 logging.debug(f"Using strategy SL pips: {sl_pips_calc}, calculated SL price: {request_sl}")
            elif config["trade_manager"]["default_stop_loss_pips"] is not None and config["trade_manager"]["default_stop_loss_pips"] > 0:
                 sl_pips_calc = config["trade_manager"]["default_stop_loss_pips"]
                 price_for_sl_calc = request_price
                 if order_type == mt5.ORDER_TYPE_BUY:
                      request_sl = round(price_for_sl_calc - sl_pips_calc * point * PIP_MULTIPLIER, digits)
                 elif order_type == mt5.ORDER_TYPE_SELL:
                      request_sl = round(price_for_sl_calc + sl_pips_calc * point * PIP_MULTIPLIER, digits)
                 logging.debug(f"Using default SL pips: {sl_pips_calc}, calculated SL price: {request_sl}")


            if tp_price is not None and tp_price > 0:
                 request_tp = round(tp_price, digits)
                 logging.debug(f"Using explicit TP price: {request_tp}")
            elif tp_pips is not None and tp_pips > 0:
                 # Calculate TP price based on Pips from EXECUTION price
                 price_for_tp_calc = request_price
                 if order_type == mt5.ORDER_TYPE_BUY:
                      request_tp = round(price_for_tp_calc + tp_pips * point * PIP_MULTIPLIER, digits)
                 elif order_type == mt5.ORDER_TYPE_SELL:
                      request_tp = round(price_for_tp_calc - tp_pips * point * PIP_MULTIPLIER, digits)
                 logging.debug(f"Using explicit TP pips: {tp_pips}, calculated TP price: {request_tp}")
            elif strat_settings.get("take_profit_pips") is not None and strat_settings["take_profit_pips"] > 0:
                 tp_pips_calc = strat_settings["take_profit_pips"]
                 price_for_tp_calc = request_price
                 if order_type == mt5.ORDER_TYPE_BUY:
                      request_tp = round(price_for_tp_calc + tp_pips_calc * point * PIP_MULTIPLIER, digits)
                 elif order_type == mt5.ORDER_TYPE_SELL:
                      request_tp = round(price_for_tp_calc - tp_pips_calc * point * PIP_MULTIPLIER, digits)
                 logging.debug(f"Using strategy TP pips: {tp_pips_calc}, calculated TP price: {request_tp}")

            
            # Validation: Check SL and TP individually for valid distances
            min_distance_points = symbol_info.trade_stops_level * point
            # Validate SL only
            if request_sl > 0.0:
                if (order_type == mt5.ORDER_TYPE_BUY and (request_price - request_sl < min_distance_points)) or \
                   (order_type == mt5.ORDER_TYPE_SELL and (request_sl - request_price < min_distance_points)):
                    logging.warning(f"Calculated SL ({request_sl:.{digits}f}) too close to entry {request_price:.{digits}f} for {symbol}. Removing SL.")
                    request_sl = 0.0
            # Validate TP only
            if request_tp > 0.0:
                if (order_type == mt5.ORDER_TYPE_BUY and (request_tp - request_price < min_distance_points)) or \
                   (order_type == mt5.ORDER_TYPE_SELL and (request_price - request_tp < min_distance_points)):
                    logging.warning(f"Calculated TP ({request_tp:.{digits}f}) too close to entry {request_price:.{digits}f} for {symbol}. Removing TP.")
                    request_tp = 0.0
    # --- Build Request ---
            magic = MAGIC_SCALPING if strategy == "scalping_strategy" and opened_by == "coop_scalp" else MAGIC_DEFAULT
            # Construct comment: Strategy_TF_Origin_SignalID_Suffix (max 31 chars)
            comment_base = f"{strategy[:8]}_{timeframe}_{opened_by[:8]}_{signal_id if signal_id else 'NoSig'}"
            comment = (comment_base + comment_suffix)[:31] # Ensure comment length


            request = {
                "action": request_action,
                "symbol": symbol,
                "volume": final_lot_size,
                "type": request_type,
                "price": request_price,
                "sl": request_sl if request_sl > 1e-9 else 0.0, # Explicitly 0.0 if not set or near zero
                "tp": request_tp if request_tp > 1e-9 else 0.0, # Explicitly 0.0 if not set or near zero
                "magic": magic,
                "comment": comment,
                "type_time": mt5.ORDER_TIME_GTC, # Good Till Cancelled
                "deviation": 20 if request_action == mt5.TRADE_ACTION_DEAL and tick else 0, # Deviation only for market orders where tick matters
            }

            # --- Set Filling Mode (only for market/exchange execution) ---
            if request_action == mt5.TRADE_ACTION_DEAL:
                filling_mode_flags = symbol_info.filling_mode
                # Broker demo rejected RETURN (2), so try IOC first, then FOK
                if filling_mode_flags & mt5.ORDER_FILLING_IOC:
                    request["type_filling"] = mt5.ORDER_FILLING_IOC
                elif filling_mode_flags & mt5.ORDER_FILLING_FOK:
                    request["type_filling"] = mt5.ORDER_FILLING_FOK
                else:
                    logging.warning(f"No supported filling mode for {symbol} (flags: {filling_mode_flags}), defaulting to IOC.")
                    request["type_filling"] = mt5.ORDER_FILLING_IOC
            # --- Send Order ---
            logging.info(f"Sending order request for {symbol}: {request}")
            result = mt5.order_send(request)

            # --- Process Result ---
            if result is None:
                err = mt5.last_error()
                logging.error(f"Order send failed for {symbol}. Request: {request}. MT5 Error: {err}")
                return type("Result", (object,), {"retcode": -1, "comment": f"MT5 Send Error: {err}"}), None

            elif result.retcode == mt5.TRADE_RETCODE_DONE:
                pos_attr = getattr(result, "position", None)
                logging.info(f"Order placed successfully for {symbol}. Retcode: {result.retcode}, Order: {result.order}, Deal: {result.deal}, Position: {pos_attr}, Comment: {result.comment}")

                if request_action == mt5.TRADE_ACTION_DEAL:
                    # For market orders, the resulting position ticket is in pos_attr
                    # pos_attr might be 0 if deal wasn't instantly processed or for some reason
                    if pos_attr and pos_attr > 0:
                         trade_ticket = pos_attr
                         logging.info(f"Market order {result.order} resulted directly in position {trade_ticket}")
                         # Fetch position details immediately if ticket is available
                         pos_list = mt5.positions_get(ticket=trade_ticket)
                         if pos_list:
                              pos_detail = pos_list[0]
                              # Add to internal tracking and DB immediately
                              # This will be the *initial* DB entry, to be updated on close
                              open_trades[trade_ticket] = {
                                 "symbol": symbol, "strategy": strategy, "type": "market", # Assuming market type for position
                                 "open_time": datetime.fromtimestamp(pos_detail.time, pytz.utc),
                                 "open_price": pos_detail.price_open,
                                 "volume": pos_detail.volume,
                                 "comment": pos_detail.comment,
                                 "opened_by": opened_by
                             }
                              asyncio.run_coroutine_threadsafe(
                                  insert_trade(ticket=trade_ticket, symbol=symbol, strategy=strategy, open_time=pos_detail.time,
                                               open_price=pos_detail.price_open, volume=pos_detail.volume, comment=pos_detail.comment, opened_by=opened_by),
                                  GLOBAL_LOOP # Assumes event loop is running
                              )
                              if signal_id is not None:
                                   # Mark signal as taken (successful attempt led to trade)
                                   asyncio.run_coroutine_threadsafe(
                                       update_signal_status(signal_id, 1, trade_ticket),
                                       GLOBAL_LOOP
                                   )

                         else:
                              logging.warning(f"Market order {result.order} placed, got ticket {trade_ticket}, but failed to fetch position details immediately.")
                              # Trade will be added to tracking/DB by position_monitor / trade_close_monitor later

                    elif result.deal > 0:
                        # Sometimes position is 0 but deal is available
                         logging.warning(f"Market order {result.order} placed, deal {result.deal} created, but position {pos_attr} is 0. Will rely on monitors.")
                         # No ticket confirmed immediately, monitors will handle. Use deal ticket for signal update if needed?
                         if signal_id is not None:
                                   asyncio.run_coroutine_threadsafe(
                                       update_signal_status(signal_id, 1, result.deal), # Use deal ticket as temporary link
                                       GLOBAL_LOOP
                                   )
                    else:
                         logging.warning(f"Market order {result.order} placed, but no deal or position ticket in result. Will rely on monitors.")
                         if signal_id is not None:
                                   asyncio.run_coroutine_threadsafe(
                                       update_signal_status(signal_id, 1, result.order), # Use order ticket as temporary link
                                       GLOBAL_LOOP
                                   )

                    return result, trade_ticket # Return result object and ticket (might be None)

                else: # Pending Order (TRADE_ACTION_PENDING)
                    # For pending orders, the resulting order ticket is in result.order
                    if result.order > 0:
                         trade_ticket = result.order
                         logging.info(f"Pending order {trade_ticket} placed successfully for {symbol}.")
                         # Pending orders don't have an immediate position in MT5 positions_get
                         # They trigger positions upon execution. We track the order ticket here.
                         open_trades[trade_ticket] = { # Track pending orders too
                              "symbol": symbol, "strategy": strategy, "type": "pending",
                              "open_time": datetime.now(pytz.utc), # Use current time as proxy for placement time
                              "open_price": request_price,
                              "volume": final_lot_size,
                              "comment": comment,
                              "opened_by": opened_by
                         }
                         asyncio.run_coroutine_threadsafe( # Insert into DB with status 'pending'
                              insert_trade(ticket=trade_ticket, symbol=symbol, strategy=strategy, open_time=time.time(), # Use raw timestamp
                                           open_price=request_price, volume=final_lot_size, comment=comment, opened_by=opened_by),
                              GLOBAL_LOOP
                         )
                         asyncio.run_coroutine_threadsafe(
                             update_trade_status(ticket=trade_ticket, status='pending'),
                             GLOBAL_LOOP
                         )

                         if signal_id is not None:
                                   asyncio.run_coroutine_threadsafe(
                                       update_signal_status(signal_id, 1, trade_ticket), # Link signal to order ticket
                                       GLOBAL_LOOP
                                   )


                    else:
                         logging.warning(f"Pending order placed for {symbol}, but order ticket is 0 in result. Will rely on monitors.")
                         if signal_id is not None:
                                   asyncio.run_coroutine_threadsafe(
                                       update_signal_status(signal_id, 1, result.order), # Link signal to order ticket (0?)
                                       GLOBAL_LOOP
                                   )

                    return result, trade_ticket # Return result object and order ticket (might be None/0)
            else:
                # Handle specific failure codes
                logging.error(f"Order failed for {symbol}. Retcode: {result.retcode}, Comment: {result.comment}. Request: {request}")
                if signal_id is not None:
                     # Mark signal as not taken if trade failed
                     asyncio.run_coroutine_threadsafe(
                         update_signal_status(signal_id, -1), # Mark as ignored/failed
                         GLOBAL_LOOP
                     )
                return result, None

    except Exception as e:
        logging.error(f"Unhandled exception in place_order_sync for {symbol}: {e}", exc_info=True)
        return type("Result", (object,), {"retcode": -2, "comment": f"Exception: {e}"}), None
    finally:
        # Deselect symbol only if specifically required or managing selections globally
        pass # Avoid deselecting for now


async def place_order_async(order_type: int, symbol: str, strategy: str, timeframe: str,
                            signal_id: Optional[int] = None, lot_size: Optional[float] = None,
                            sl_pips: Optional[float] = None, tp_pips: Optional[float] = None,
                            entry_price: Optional[float] = None,
                            sl_price: Optional[float] = None, tp_price: Optional[float] = None,
                            opened_by: str = "bot_signal",
                            comment_suffix: str = "",
                            context: Optional[ContextTypes.DEFAULT_TYPE] = None
                            ) -> Tuple[Optional[Any], Optional[int]]:
    """Places an order in MT5 asynchronously by running the sync function in an executor."""
    loop = asyncio.get_running_loop()
    # Run the synchronous MT5 function in a separate thread
    result, trade_ticket = await loop.run_in_executor(
        None,  # Uses the default thread pool executor
        place_order_sync,
        order_type, symbol, strategy, timeframe, signal_id,
        lot_size, sl_pips, tp_pips, entry_price, sl_price, tp_price, opened_by, comment_suffix
    )
    if context and result and getattr(result, "retcode", None) == mt5.TRADE_RETCODE_DONE and entry_price is None:
        entry = entry_price
        sl = sl_price
        order_dir = order_type
        if trade_ticket:
            with mt5_sync_lock:
                pos = mt5.positions_get(ticket=trade_ticket)
            if pos:
                entry = pos[0].price_open
                sl = pos[0].sl
                order_dir = pos[0].type
        await notify_trade_open(context, symbol, timeframe, entry, sl, order_dir)

    # open_trades and initial DB insert are handled inside place_order_sync now if ticket is available

    return result, trade_ticket

# =============================================================================
# Signal Handling & Operational Modes
# =============================================================================

async def send_signal_to_telegram(context: ContextTypes.DEFAULT_TYPE, signal_id: int, symbol: str, timeframe: str,
                                  action: str, strategy: str,
                                  bb_upper: Optional[float] = None, bb_lower: Optional[float] = None,
                                  low: Optional[float] = None, high: Optional[float] = None,
                                  bbm: Optional[float] = None, signal_candle_close: Optional[float] = None,
                                  prob: Optional[float] = None) -> None:
    """Sends a trading signal to Telegram based on operational mode.

    Parameters
    ----------
    prob : Optional[float]
        Probability returned by the AI model. Included in Telegram messages when provided.
    """
    timestamp = datetime.now(pytz.utc).isoformat() # Use UTC

    # Insert signal into DB (initially not taken)
    db_signal_id = await insert_signal(timestamp, symbol, timeframe, strategy, action, bb_upper, bb_lower)

    if db_signal_id is None:
        logging.error(f"Failed to insert signal for {symbol} {timeframe} into DB. Cannot proceed.")
        return

    # Use the DB signal ID for tracking
    signal_id = db_signal_id

    logging.info(
        f"Processing signal {signal_id}: {symbol} {timeframe} {action} (Strategy: {strategy}, Prob: {prob if prob is not None else 'N/A'})"
    )

    # --- Check Operational Mode ---
    if config["operational_mode"] == MODE_FULL_AUTO:
        # Process automatically via filters
        await process_auto_signal(context, signal_id, symbol, timeframe, action, strategy,
                                   low, high, bbm, bb_upper, bb_lower, signal_candle_close, prob)

    elif config["operational_mode"] == MODE_SEMI_AUTO:
        # Send signal with buttons for manual action
        await send_manual_signal_buttons(context, signal_id, symbol, timeframe, action, strategy,
                                         bb_upper, bb_lower, low, high, bbm, prob)
    else:
        logging.error(f"Unknown operational mode configured: {config['operational_mode']}. Signal {signal_id} ignored.")
        await update_signal_status(signal_id, -1) # Mark as ignored
        await safe_send_message(context, text=f"⚠️ Signal Ignored: Unknown mode '{config['operational_mode']}'. Please configure modes.")


async def send_manual_signal_buttons(context: ContextTypes.DEFAULT_TYPE, signal_id: int, symbol: str, timeframe: str,
                                     action: str, strategy: str, bb_upper: Optional[float], bb_lower: Optional[float],
                                     low: Optional[float], high: Optional[float], bbm: Optional[float],
                                     prob: Optional[float]) -> None:
    """Sends a trading signal to Telegram with inline action buttons for Semi-Auto mode.

    If ``prob`` is provided, it is displayed to the user and included in callback data
    so that subsequent trade placement notifications can show the same confidence.
    """
    bb_upper_str = f"{bb_upper:.5f}" if bb_upper is not None else "N/A"
    bb_lower_str = f"{bb_lower:.5f}" if bb_lower is not None else "N/A"

    # Base data for callback buttons
    # Include low/high/bbm here as they might be needed for limit order suggestions?
    # Or fetch latest data in callback? Fetching in callback is safer if price changes.
    base_data = {
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy": strategy,
        "orig_action": action,  # Store the original signal direction
        "prob": prob,
        # Optional: include bb_upper/lower/bbm/low/high if needed later, but might exceed callback data size
        # "bb_upper": bb_upper, "bb_lower": bb_lower, "low": low, "high": high, "bbm": bbm # Float values can be problematic
    }

    keyboard = []
    signal_action_upper = action.upper()

    # Common buttons
    data_dismiss = {**base_data, "user_action": "DISMISS"}
    dismiss_button = InlineKeyboardButton("Dismiss ⛔️", callback_data=encode_callback_data(data_dismiss))

    # Buttons based on signal action
    if signal_action_upper in ["BUY", "SELL"]: # Both long_medium and scalping signal types
        data_market = {**base_data, "user_action": f"MARKET_{signal_action_upper}"} # Use explicit type
        data_limit = {**base_data, "user_action": "LIMIT_ORDER"}
        keyboard = [
            [InlineKeyboardButton(f"Execute {signal_action_upper} (Market) ✅", callback_data=encode_callback_data(data_market))],
            [InlineKeyboardButton(f"Set {signal_action_upper} Limit/Stop 📈📉", callback_data=encode_callback_data(data_limit))],
            [dismiss_button]
        ]
    else: # Should not happen, but defensive
        logging.warning(f"Manual signal generated with unexpected action: {action} for {symbol} {timeframe}")
        keyboard = [[dismiss_button]]


    reply_markup = InlineKeyboardMarkup(keyboard)
    message = (
        f"🔔 <b>Manual Signal ({config['operational_mode']})</b> 🔔\n\n"
        f"<b>Action: {signal_action_upper}</b>\n"
        f"Symbol: <code>{symbol}</code>\n"
        f"Timeframe: {timeframe}\n"
        f"Strategy: {strategy.replace('_', ' ').title()}\n"
        # Indicator values can be added back if needed
        # f"BB Upper: {bb_upper_str}\n"
        # f"BB Lower: {bb_lower_str}\n"
        f"Signal ID: {signal_id}"
        + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
    )

    await safe_send_message(context, config["telegram"]["chat_id"], message, reply_markup, parse_mode=ParseMode.HTML)


async def process_auto_signal(context: ContextTypes.DEFAULT_TYPE, signal_id: int, symbol: str, timeframe: str,
                              action: str, strategy: str, low: Optional[float], high: Optional[float],
                              bbm: Optional[float], bb_upper: Optional[float], bb_lower: Optional[float],
                              signal_candle_close: Optional[float], prob: Optional[float]) -> None:
    """Processes signals in FULL_AUTO mode through filters and triggers trade placement."""
    logging.info(
        f"Processing AUTO signal {signal_id}: {symbol} {timeframe} {action} (Strategy: {strategy}, Prob: {prob if prob is not None else 'N/A'})"
    )

    if config["trade_manager"].get("use_news_filter"):
        await update_news_state(context, symbol)
        if should_wait_for_news(symbol):
            logging.info(f"AUTO Signal {signal_id}: Postponed due to news for {symbol}.")
            await update_signal_status(signal_id, -1)
            await safe_send_message(context, text=f"📰 AUTO Signal Ignored ({symbol} {timeframe} {action}): News event near.")
            return

    # --- 1. Overall Trend Filter ---
    overall_trend_tf = config["scanner"]["overall_trend_timeframe"]
    # Ensure enough bars for TEMA/DEMA trend calculation
    min_bars_trend = max(config["scanner"]["min_bars_trend"], 300)

    trend_df = fetch_history_data(symbol, overall_trend_tf, min_bars_trend)
    if trend_df is None or len(trend_df) < min_bars_trend:
        key = (symbol, overall_trend_tf, "data")
        if key not in _trend_warning_cache:
            logging.warning(
                f"AUTO Signal {signal_id}: Skipping trend filter for {symbol} {overall_trend_tf}. Not enough data."
            )
            _trend_warning_cache.add(key)
        await update_signal_status(signal_id, -1) # Mark as ignored
        await safe_send_message(
            context,
            text=f"⚠️ AUTO Signal Ignored ({symbol} {timeframe} {action}): Failed trend data fetch on {overall_trend_tf}.",
        )
        return

    # Calculate Trend Indicators and VWAP
    tema_col = "TEMA_100"
    dema_col = "DEMA_100"
    try:
        trend_df.ta.tema(length=100, append=True)
        trend_df.ta.dema(length=100, append=True)
        trend_df = trend_df.dropna(subset=[tema_col, dema_col])
        if len(trend_df) < 2:
            key = (symbol, overall_trend_tf, "nan")
            if key not in _trend_warning_cache:
                logging.warning(
                    f"AUTO Signal {signal_id}: Skipping trend filter for {symbol} {overall_trend_tf}. NaN indicator values on trendbar."
                )
                _trend_warning_cache.add(key)
            await update_signal_status(signal_id, -1)
            await safe_send_message(
                context,
                text=f"⚠️ AUTO Signal Ignored ({symbol} {timeframe} {action}): NaN trend indicators on {overall_trend_tf}.",
            )
            return

        last_bar = trend_df.iloc[-1]
        prev_bar = trend_df.iloc[-2]
        tema_val = last_bar[tema_col]
        dema_val = last_bar[dema_col]
        tema_prev = prev_bar[tema_col]
        dema_prev = prev_bar[dema_col]

        daily_df = fetch_history_data(symbol, "D1", 10)
        weekly_df = fetch_history_data(symbol, "W1", 10)
        if daily_df is None or weekly_df is None or daily_df.empty or weekly_df.empty:
            key = (symbol, overall_trend_tf, "vwap")
            if key not in _trend_warning_cache:
                logging.warning(
                    f"AUTO Signal {signal_id}: Skipping trend filter for {symbol}. Unable to fetch VWAP data."
                )
                _trend_warning_cache.add(key)
            await update_signal_status(signal_id, -1)
            await safe_send_message(
                context,
                text=f"⚠️ AUTO Signal Ignored ({symbol} {timeframe} {action}): Missing VWAP data.",
            )
            return

        # pandas-ta emits warnings if the index carries timezone info
        try:
            daily_df.index = daily_df.index.tz_localize(None)
            weekly_df.index = weekly_df.index.tz_localize(None)
        except Exception:
            pass
        daily_vwap = ta.vwap(daily_df['high'], daily_df['low'], daily_df['close'], daily_df['tick_volume']).iloc[-1]
        weekly_vwap = ta.vwap(weekly_df['high'], weekly_df['low'], weekly_df['close'], weekly_df['tick_volume']).iloc[-1]

        trend_bias = "Neutral"
        if (
            tema_val > dema_val
            and tema_val > tema_prev
            and dema_val > dema_prev
            and daily_vwap > weekly_vwap
        ):
            trend_bias = "Bullish"
        elif (
            tema_val < dema_val
            and tema_val < tema_prev
            and dema_val < dema_prev
            and daily_vwap < weekly_vwap
        ):
            trend_bias = "Bearish"

        logging.info(
            f"AUTO Signal {signal_id} Trend Filter: {symbol} {overall_trend_tf} bias {trend_bias}. "
            f"TEMA/DEMA: {tema_val:.{mt5.symbol_info(symbol).digits}f}/{dema_val:.{mt5.symbol_info(symbol).digits}f}, "
            f"Daily VWAP: {daily_vwap:.{mt5.symbol_info(symbol).digits}f}, Weekly VWAP: {weekly_vwap:.{mt5.symbol_info(symbol).digits}f}"
        )

    except Exception as trend_err:
        logging.error(
            f"AUTO Signal {signal_id}: Error calculating trend indicators for {symbol} {overall_trend_tf}: {trend_err}",
            exc_info=True,
        )
        await update_signal_status(signal_id, -1)
        await safe_send_message(
            context,
            text=f"⚠️ AUTO Signal Ignored ({symbol} {timeframe} {action}): Error calculating trend on {overall_trend_tf}.",
        )
        return  # Skip if trend calculation fails

    # Check if signal aligns with trend bias
    signal_aligns_with_trend = False
    if action == "BUY" and trend_bias == "Bullish":
        signal_aligns_with_trend = True
    elif action == "SELL" and trend_bias == "Bearish":
        signal_aligns_with_trend = True

    if not signal_aligns_with_trend:
        logging.info(f"AUTO Signal {signal_id}: Filtered OUT. Signal ({action}) does NOT align with {overall_trend_tf} trend bias ({trend_bias}).")
        await update_signal_status(signal_id, -1) # Mark as ignored
        await safe_send_message(context, text=f"ℹ️ AUTO Signal Filtered ({symbol} {timeframe} {action}): No trend alignment on {overall_trend_tf}.")
        return # Stop processing if trend filter fails

    logging.info(f"AUTO Signal {signal_id}: Passed Trend Filter ({trend_bias} bias). Proceeding...")

    # --- 2. Pip Confirmation Filter ---
    pip_confirm_threshold = config["trade_manager"]["pip_confirmation_threshold"]
    pip_confirm_timeout = config["trade_manager"]["pip_confirmation_timeout_sec"]

    if pip_confirm_threshold > 0:
        if signal_candle_close is None:
            logging.error(f"AUTO Signal {signal_id}: Pip confirmation enabled but signal candle close price is missing. Cannot apply filter.")
            await update_signal_status(signal_id, -1)
            await safe_send_message(context, text=f"⚠️ AUTO Signal Ignored ({symbol} {timeframe} {action}): Missing close price for pip confirmation.")
            return

        with mt5_sync_lock:
             symbol_info = mt5.symbol_info(symbol)
             if not symbol_info or symbol_info.point <= 1e-9:
                 logging.error(f"AUTO Signal {signal_id}: Invalid symbol info for {symbol}. Cannot apply pip confirmation.")
                 await update_signal_status(signal_id, -1)
                 await safe_send_message(context, text=f"⚠️ AUTO Signal Ignored ({symbol} {timeframe} {action}): Invalid symbol info for pip confirmation.")
                 return
             point = symbol_info.point

        # Calculate the required price level for confirmation
        required_price = 0.0
        if action == "BUY":
            required_price = signal_candle_close + pip_confirm_threshold * point
        elif action == "SELL":
            required_price = signal_candle_close - pip_confirm_threshold * point

        # Add to the list of trades waiting for confirmation
        confirmation_key = f"{symbol}_{timeframe}_{action}" # Use unique key for potential multiple pending per symbol/tf
        with CONFIRMATION_PENDING_LOCK:
            if confirmation_key in confirmation_pending_trades:
                 logging.warning(f"AUTO Signal {signal_id}: Another signal {confirmation_key} is already waiting for pip confirmation. Skipping this one.")
                 await update_signal_status(signal_id, -1) # Mark as ignored
                 await safe_send_message(context, text=f"ℹ️ AUTO Signal Ignored ({symbol} {timeframe} {action}): Another signal awaiting pip confirmation.")
                 return

            confirmation_pending_trades[confirmation_key] = {
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy": strategy,
                "action": action,
                "signal_candle_close_price": signal_candle_close,
                "required_price": required_price,
                "start_time": datetime.now(pytz.utc),
                "prob": prob,
                "parent_trade_data": { # Store necessary info for potential LM trade and coop state
                     "low": low, "high": high, "bbm": bbm, "bb_upper": bb_upper, "bb_lower": bb_lower # Pass original signal data
                }
            }
        global pip_monitor_job
        if not pip_monitor_job or pip_monitor_job.next_run_time is None:
            interval = config["trade_manager"].get("pip_confirm_check_interval_sec", config["background_tasks_interval"])
            params = inspect.signature(context.job_queue.run_repeating).parameters
            if "job_kwargs" in params:
                pip_monitor_job = context.job_queue.run_repeating(
                    pip_confirmation_monitor,
                    interval=interval,
                    first=0,
                    job_kwargs={"coalesce": False, "max_instances": 3, "misfire_grace_time": 2}  # add grace to reduce missed run warnings
                )
            else:
                pip_monitor_job = context.job_queue.run_repeating(
                    pip_confirmation_monitor,
                    interval=interval,
                    first=0,
                    coalesce=False,
                    max_instances=3,
                    misfire_grace_time=2,  # add grace to reduce missed run warnings
                )
        logging.info(f"AUTO Signal {signal_id}: Passed Trend Filter. Now waiting for {pip_confirm_threshold} pips confirmation. Target price: {required_price:.{symbol_info.digits}f}")
        await safe_send_message(context, text=f"⏳ AUTO Signal {signal_id} ({symbol} {timeframe} {action}): Passed Trend Filter. Waiting for {pip_confirm_threshold} pips confirmation (Target: {required_price:.{symbol_info.digits}f}).")

        # Signal status is still 0 (pending confirmation), will be updated by confirmation monitor
        return # Don't place order yet, wait for confirmation monitor

    else: # Pip confirmation threshold is 0 or disabled
        logging.info(f"AUTO Signal {signal_id}: Passed Trend Filter. Pip confirmation disabled. Placing trade immediately.")
        await safe_send_message(
            context,
            text=(
                f"✅ AUTO Signal {signal_id} ({symbol} {timeframe} {action}): Passed Trend Filter. Placing trade immediately."
                + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
            ),
        )
        await place_auto_trade(context, signal_id, symbol, timeframe, action, strategy,
                               low=low, high=high, bbm=bbm, bb_upper=bb_upper, bb_lower=bb_lower,
                               prob=prob)  # Place trade directly

# Called by process_auto_signal (if no pip confirmation) OR by pip_confirmation_monitor
async def place_auto_trade(context: ContextTypes.DEFAULT_TYPE, signal_id: int, symbol: str, timeframe: str,
                           action: str, strategy: str,
                           low: Optional[float] = None, high: Optional[float] = None,
                           bbm: Optional[float] = None, bb_upper: Optional[float] = None, bb_lower: Optional[float] = None,
                           prob: Optional[float] = None
                           ) -> Tuple[Optional[Any], Optional[int]]:
    """Places the actual trade after all auto filters/waits are passed.

    The ``prob`` parameter carries the AI confidence from the originating signal
    and is echoed back in any Telegram notifications about the placed trade.
    """
    order_type = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL

    # --- Long/Medium Strategy Trade Placement (Auto Mode) ---
    if strategy == "long_medium":
        if config["trade_manager"].get("open_mode") == "sequential":
            with mt5_sync_lock:
                positions = mt5.positions_get() or []
            for pos in positions:
                if pos.magic == MAGIC_DEFAULT:
                    state = cooperative_states.get(pos.symbol, {})
                    if state.get("lock_in_status") not in ("achieved", "stopped_risk"):
                        logging.info(
                            f"Sequential mode: Delaying new long_medium trade while {pos.symbol} is not locked in."
                        )
                        await update_signal_status(signal_id, -1)
                        await safe_send_message(
                            context,
                            text="ℹ️ Sequential mode: Waiting for previous trade to lock profit before opening a new one."
                        )
                        return None, None

        # Place Long/Medium Limit Order
        entry_price = low if action == "BUY" else high  # Entry at the candle's extreme that triggered signal
        if entry_price is None:
            logging.error(
                f"Coop AUTO: Cannot place limit order for {symbol} signal {signal_id}, missing low/high price."
            )
            await update_signal_status(signal_id, -1)
            await safe_send_message(
                context,
                text=f"❌ Coop AUTO: Failed to place limit order for signal {signal_id} ({symbol} {action}). Missing price."
            )
            return None, None

        # Calculate SL/TP for LM trade. Use strategy settings or defaults.
        lm_settings = config["trade_manager"]["strategy_settings"].get("long_medium", {})
        sl_pips = lm_settings.get("stop_loss_pips", config["trade_manager"]["default_stop_loss_pips"])

        tp_price = None
        tp_mode = lm_settings.get("take_profit_mode")
        tp_value = lm_settings.get("take_profit_value")
        tp_bb_type = lm_settings.get("take_profit_bb_type", "middle")

        with mt5_sync_lock:
            symbol_info = mt5.symbol_info(symbol)
            digits = symbol_info.digits if symbol_info else 5
            point = symbol_info.point if symbol_info else 0

        if tp_mode == "pips" and tp_value:
            if point > 0:
                if action == "BUY":
                    tp_price = round(entry_price + tp_value * point * PIP_MULTIPLIER, digits)
                else:
                    tp_price = round(entry_price - tp_value * point * PIP_MULTIPLIER, digits)
        elif tp_mode == "money" and tp_value and point > 0 and symbol_info:
            lot = lm_settings.get("lot_size", config["trade_manager"]["default_lot_size"])
            one_pip_price = point * PIP_MULTIPLIER
            try:
                profit_per_pip = mt5.order_calc_profit(order_type, symbol, lot, entry_price, entry_price + one_pip_price if action == "BUY" else entry_price - one_pip_price)
                if profit_per_pip:
                    required_pips = tp_value / profit_per_pip
                    if action == "BUY":
                        tp_price = round(entry_price + required_pips * point * PIP_MULTIPLIER, digits)
                    else:
                        tp_price = round(entry_price - required_pips * point * PIP_MULTIPLIER, digits)
            except Exception:
                pass
        elif tp_mode == "bollingerbands" and bbm is not None:
            if tp_bb_type == "middle":
                tp_price = round(bbm, digits)
            else:
                if action == "BUY" and bb_upper is not None:
                    tp_price = round(bb_upper, digits)
                elif action == "SELL" and bb_lower is not None:
                    tp_price = round(bb_lower, digits)

        if tp_price is None and lm_settings.get("take_profit_pips") is not None:
            tp_pips_calc = lm_settings.get("take_profit_pips")
            if point > 0:
                if action == "BUY":
                    tp_price = round(entry_price + tp_pips_calc * point * PIP_MULTIPLIER, digits)
                else:
                    tp_price = round(entry_price - tp_pips_calc * point * PIP_MULTIPLIER, digits)
        if tp_price is None and bbm is not None:
            tp_price = round(bbm, digits)

        logging.info(
            f"Coop AUTO: Placing long_medium limit order for signal {signal_id}: {symbol} {action} @ {entry_price}. SL pips: {sl_pips}. TP price: {tp_price}"
        )

        result, pending_order_ticket = await place_order_async(
            order_type,
            symbol,
            "long_medium",
            timeframe,
            signal_id=signal_id,
            entry_price=entry_price,
            sl_pips=sl_pips,
            tp_price=tp_price,
            opened_by="coop_long_medium",
        )
        if pending_order_ticket:
            AI_DECISIONS[pending_order_ticket] = True

        if result and result.retcode == mt5.TRADE_RETCODE_DONE and pending_order_ticket:
            # Order placed successfully. Update signal status and cooperative state.
            await update_signal_status(signal_id, 1, pending_order_ticket)

            # Add/Update cooperative state for this symbol to track the pending order
            cooperative_states[symbol] = {
                "long_medium_pending_order": pending_order_ticket,
                "long_medium_ticket": None,  # Will be filled when order triggers
                "direction": action.lower(),
                "lock_in_status": "pending_activation",
                "locked_in_profit_usd": 0,
                "scalping_status": "inactive",
                "current_scalping_pnl_usd": 0,
                "open_scalp_tickets": [],
                "parent_tf": timeframe,
                "bbm_tp": tp_price,  # Store the intended TP
                "signal_id": signal_id,
            }
            logging.info(
                f"Coop state updated for {symbol} with pending order {pending_order_ticket}. Status: pending_activation."
            )
            # active_long_medium_pairs is now derived from cooperative_states keys if LM ticket is not None

            digits = 5
            with mt5_sync_lock:  # Get digits for message formatting
                info = mt5.symbol_info(symbol)
                if info:
                    digits = info.digits
            sl_price_msg = ""
            if sl_pips is not None and sl_pips > 0:
                # Estimate SL price for the message (might differ slightly from actual due to rounding/execution price)
                estimated_sl_price = round((entry_price - sl_pips * mt5.symbol_info(symbol).point) if action == "BUY" else (entry_price + sl_pips * mt5.symbol_info(symbol).point), digits)
                sl_price_msg = f", SL ~{estimated_sl_price:.{digits}f}"

            tp_price_msg = f", TP ~{tp_price:.{digits}f}" if tp_price is not None else ""

            await safe_send_message(
                context,
                text=(
                    f"📈 Coop AUTO: Placed long_medium pending order {pending_order_ticket} for {symbol} {action} @ {entry_price:.{digits}f}{sl_price_msg}{tp_price_msg}"
                    + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
                ),
            )
        else:
            error_msg = getattr(result, 'comment', 'Unknown error') if result else 'Send failed'
            retcode = getattr(result, 'retcode', 'N/A') if result else 'N/A'
            logging.error(f"Coop AUTO: Failed to place long_medium pending order for {symbol} signal {signal_id}. Reason: {error_msg} (Code: {retcode})")
            await update_signal_status(signal_id, -1)
            await safe_send_message(context, text=f"❌ Coop AUTO: Failed to place long_medium pending order for {symbol} signal {signal_id}. Reason: {error_msg}")

        return result, pending_order_ticket  # Return pending order ticket

    # --- Scalping Strategy Trade Placement (Auto Mode) ---
    elif strategy == "scalping_strategy":
        # This is only placed if there is an *achieved* LM trade for this pair
        # This logic is handled by the tick_processor calling process_auto_signal,
        # and process_auto_signal checking the cooperative_states["scalping_status"]

        # Get scalping-specific settings
        scalping_settings = config["trade_manager"]["strategy_settings"].get("scalping_strategy", {})
        sl_pips = scalping_settings.get("stop_loss_pips") # Use strategy-specific SL/TP for scalping
        tp_pips = scalping_settings.get("take_profit_pips")

        logging.info(f"Coop AUTO: Placing scalping market order for signal {signal_id}: {symbol} {action}. SL pips: {sl_pips}, TP pips: {tp_pips}")

        result, scalp_ticket = await place_order_async(
            order_type, symbol, "scalping_strategy", timeframe,
            signal_id=signal_id,
            sl_pips=sl_pips, tp_pips=tp_pips,
            opened_by="coop_scalp",
            context=context
        )
        if scalp_ticket:
            AI_DECISIONS[scalp_ticket] = True

        if result and result.retcode == mt5.TRADE_RETCODE_DONE and scalp_ticket is not None:
            # Order placed successfully. Update signal status and cooperative state.
            await update_signal_status(signal_id, 1, scalp_ticket)

            # Update state with the open scalp ticket and reset scalp PnL for this new scalp trade
            # Note: Cumulative scalp PnL is tracked elsewhere (cooperative_states)
            # Here we just add the ticket
            if symbol in cooperative_states: # Should be if we reached here
                state = cooperative_states[symbol]
                if "open_scalp_tickets" not in state:
                    state["open_scalping_tickets"] = []  # Ensure list exists
                state["open_scalp_tickets"].append(scalp_ticket)  # Add the new scalp ticket
                # state["current_scalping_pnl_usd"] = 0 # NOT HERE, CUMULATIVE IS TRACKED

            await safe_send_message(
                context,
                text=(
                    f"💸 Coop AUTO: Placed scalping market order {scalp_ticket} for {symbol} {action}."
                    + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
                ),
            )
        else:
             error_msg = getattr(result, 'comment', 'Unknown error') if result else 'Send failed'
             retcode = getattr(result, 'retcode', 'N/A') if result else 'N/A'
             logging.error(f"Coop AUTO: Failed to place scalping order for {symbol} signal {signal_id}. Reason: {error_msg} (Code: {retcode})")
             await update_signal_status(signal_id, -1)
             await safe_send_message(context, text=f"❌ Coop AUTO: Failed to place scalping order for {symbol} signal {signal_id}. Reason: {error_msg}")

        return result, scalp_ticket # Return resulting position ticket


    # Add other strategy placement logic here if needed

    elif strategy == "Bollingerband_Kachu_Strategy":
        # AUTO: Place market order immediately for Bollingerband_Kachu_Strategy
        logging.info(f"AUTO: Placing market order for Bollingerband_Kachu_Strategy signal {signal_id}: {symbol} {action}")
        result, trade_ticket = await place_order_async(
            order_type, symbol, strategy, timeframe,
            signal_id=signal_id,
            context=context
        )

        if trade_ticket:
            AI_DECISIONS[trade_ticket] = True

        # Treat any TRADE_RETCODE_DONE as success, even if ticket is None
        if result and getattr(result, "retcode", None) == mt5.TRADE_RETCODE_DONE:
            if trade_ticket:
                # We got a valid ticket immediately
                asyncio.run_coroutine_threadsafe(update_signal_status(signal_id, 1, trade_ticket), GLOBAL_LOOP)
                await safe_send_message(
                    context,
                    text=(
                        f"💼 AUTO: Placed market order {trade_ticket} for {symbol} {action} ({strategy})."
                        + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
                    ),
                )
            else:
                # MT5 reports DONE but no ticket yet—assume it will appear shortly
                asyncio.run_coroutine_threadsafe(update_signal_status(signal_id, 1, None), GLOBAL_LOOP)
                await safe_send_message(
                    context,
                    text=(
                        f"💼 AUTO: Market order sent for {symbol} {action} ({strategy}). Ticket to follow."
                        + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
                    ),
                )
        else:
            # Order failed
            error_msg = getattr(result, "comment", "Unknown error") if result else "Send failed"
            retcode = getattr(result, "retcode", "N/A") if result else "N/A"
            logging.error(f"AUTO: Failed to place market order for {symbol} signal {signal_id} ({strategy}). Reason: {error_msg} (Code: {retcode})")
            await update_signal_status(signal_id, -1)
            await safe_send_message(context, text=f"❌ AUTO: Failed to place market order for {symbol} signal {signal_id} ({strategy}). Reason: {error_msg}")
    else:
        logging.error(f"AUTO Signal {signal_id}: Cannot place trade. Unsupported strategy '{strategy}' or placement logic not implemented.")
        await update_signal_status(signal_id, -1)
        await safe_send_message(context, text=f"❌ AUTO Signal Ignored ({symbol} {timeframe} {action}): Unsupported strategy '{strategy}'.")
        return None, None


async def inline_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Optional[int]:
    """Handles Telegram inline button callbacks for manual signal actions."""
    query = update.callback_query
    # Always answer the query first
    try:
        await query.answer()
    except Exception as e:
        logging.warning(f"Failed to answer callback query {query.id}: {e}")

    if not query.data:
         logging.warning("Inline button callback received without data.")
         await safe_reply(update, context, "Internal error: Missing callback data.")
         return None

    data = decode_callback_data(query.data)
    if not data or "signal_id" not in data:
        logging.error(f"Invalid or expired callback data token: {query.data}")
        # Try to edit the original message to indicate it's expired/invalid
        try:
            await query.edit_message_text("⛔️ Error: Invalid or expired signal action.", reply_markup=None)
        except telegram.error.BadRequest:
             pass # Ignore if message cannot be edited (too old)
        except Exception as e:
             logging.error(f"Failed to edit message on invalid callback data: {e}", exc_info=True)

        # Send a new message as fallback if edit failed or wasn't applicable
        # Only send a new message if query.message exists, otherwise avoid flooding
        if query.message:
             await safe_reply(update, context, "⛔️ Error: Invalid or expired signal action.")

        return None

    # Extract data (use .get for safety)
    signal_id = data.get("signal_id")
    user_action = data.get("user_action") # What the user clicked
    symbol = data.get("symbol")
    timeframe = data.get("timeframe")
    strategy = data.get("strategy")
    orig_action = data.get("orig_action") # The original signal direction (BUY/SELL)
    prob = data.get("prob")

    logging.info(f"Callback received for signal {signal_id}: user_action={user_action}, symbol={symbol}, strategy={strategy}")

    # --- Handle Dismiss ---
    if user_action == "DISMISS":
        new_text = f"Signal {signal_id} ({symbol} {timeframe} {orig_action}) dismissed by user. ✅"
        await update_signal_status(signal_id, -1) # Mark as dismissed
        try:
            await query.edit_message_text(new_text, reply_markup=None, parse_mode=ParseMode.HTML)
        except telegram.error.BadRequest:
             # Message might be too old to edit, send as new message
             await safe_reply(update, context, new_text)
        except Exception as e: # Catch other potential errors
             logging.error(f"Failed to edit message for dismissed signal {signal_id}: {e}", exc_info=True)
             await safe_reply(update, context, new_text) # Fallback to new message

        return None

    # --- Handle Limit Order Setup ---
    elif user_action == "LIMIT_ORDER":
        # Store necessary data for the conversation
        context.user_data["limit_order"] = {
            "signal_id": signal_id,
            "action": orig_action, # Base action (BUY or SELL)
            "symbol": symbol,
            "timeframe": timeframe,
            "strategy": strategy,
            "opened_by": "limit_manual_button", # Origin
            # Other data needed for placement can be fetched or added here if small enough
        }
        progress_text = f"⏳ Setting Limit/Stop Order for {symbol} {orig_action} (Signal {signal_id})..."
        # Edit message to show progress and ask for input
        try:
            await query.edit_message_text(progress_text, reply_markup=None)
        except telegram.error.BadRequest:
            logging.warning(f"Failed to edit message for limit order setup {signal_id}, sending new message.")
            await safe_send_message(context, update.effective_chat.id, progress_text)
        except Exception as e:
             logging.error(f"Failed to edit message for limit order setup {signal_id}: {e}", exc_info=True)
             await safe_send_message(context, update.effective_chat.id, progress_text)


        await safe_reply(update, context, "Enter desired entry price:")

        return LIMIT_ORDER_ENTRY # Start the conversation

    # --- Handle Execute Market Order (MARKET_BUY/MARKET_SELL) ---
    elif user_action in ["MARKET_BUY", "MARKET_SELL"]:
        order_type = mt5.ORDER_TYPE_BUY if user_action == "MARKET_BUY" else mt5.ORDER_TYPE_SELL
        signal_action_upper = user_action.split("_")[1] # BUY or SELL

        # Inform user action is in progress
        progress_text = f"⏳ Processing {signal_action_upper} Market order for {symbol} from signal {signal_id}..."
        try:
            await query.edit_message_text(progress_text, reply_markup=None)
        except telegram.error.BadRequest:
             logging.warning(f"Failed to edit message for market order execution {signal_id}, sending new message.")
             await safe_send_message(context, update.effective_chat.id, progress_text)
        except Exception as e:
             logging.error(f"Failed to edit message for market order execution {signal_id}: {e}", exc_info=True)
             await safe_send_message(context, update.effective_chat.id, progress_text)


        # Place the order (SL/TP will be determined by default/strategy settings in place_order_async)
        result, trade_ticket = await place_order_async(
            order_type, symbol, strategy, timeframe,
            signal_id=signal_id, # Link order to signal
            opened_by="signal_button_market", # Mark as opened via button click (market)
            context=context
        )

        # --- Report Outcome ---
        final_text = ""
        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
            # DB insertion and signal update happen inside place_order_async now if successful
            if trade_ticket:
                final_text = (
                    f"✅ Market {signal_action_upper} order placed for {symbol} (Signal {signal_id}).\nTrade Ticket: <code>{trade_ticket}</code>"
                    + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
                )
            else:
                # Success code but no ticket (confirmation delay or pending)
                final_text = (
                    f"✅ Market {signal_action_upper} order sent for {symbol} (Signal {signal_id})."
                    + (f"\nConfidence: {prob*100:.2f}%" if prob is not None else "")
                )
                # The initial signal status update happens in place_order_async

            # Try editing original message first
            try:
                await query.edit_message_text(final_text, reply_markup=None, parse_mode=ParseMode.HTML)
            except telegram.error.BadRequest:
                await safe_send_message(context, update.effective_chat.id, final_text, parse_mode=ParseMode.HTML) # Fallback to new message
            except Exception as e:
                 logging.error(f"Failed to edit message for market order result {signal_id}: {e}", exc_info=True)
                 await safe_send_message(context, update.effective_chat.id, final_text, parse_mode=ParseMode.HTML) # Fallback to new message

        else: # Order failed
            error_comment = getattr(result, 'comment', 'Unknown MT5 Error') if result else 'Order Send Failed'
            retcode = getattr(result, 'retcode', 'N/A') if result else 'N/A'
            final_text = f"❌ Failed to place Market {signal_action_upper} order for {symbol} (Signal {signal_id}).\nReason: {error_comment} (Code: {retcode})"
            # Signal status update happens inside place_order_async on failure

            # Try editing original message first
            try:
                await query.edit_message_text(final_text, reply_markup=None, parse_mode=ParseMode.HTML)
            except telegram.error.BadRequest:
                 await safe_send_message(context, update.effective_chat.id, final_text, parse_mode=ParseMode.HTML) # Fallback to new message
            except Exception as e:
                 logging.error(f"Failed to edit message for market order failure {signal_id}: {e}", exc_info=True)
                 await safe_send_message(context, update.effective_chat.id, final_text, parse_mode=ParseMode.HTML) # Fallback to new message


        return None # End conversation state for market orders

    else:
        logging.warning(f"Unhandled user_action in inline_button_callback: {user_action}")
        await safe_reply(update, context, "⚠️ Unknown action selected.")
        return None


# =============================================================================
# Limit Order Conversation Handlers
# =============================================================================
async def limit_order_entry(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles entry price input for limit/stop orders."""
    if not update.message or not context.user_data.get("limit_order"):
        logging.warning("limit_order_entry called without message or user_data.")
        await safe_reply(update, context, "Session expired or data missing. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

    user_input = update.message.text.strip()
    limit_data = context.user_data["limit_order"]
    symbol = limit_data["symbol"]
    action = limit_data["action"]

    if user_input.lower() == 'cancel':
        await cfg_cancel(update, context) # Use generic cancel handler
        return ConversationHandler.END

    try:
        entry_price = float(user_input)
        limit_data["entry_price"] = entry_price

        digits = 5 # Default digits
        with mt5_sync_lock:
             symbol_info = mt5.symbol_info(symbol)
             if symbol_info: digits = symbol_info.digits
             else: logging.warning(f"Could not get symbol info for {symbol} during limit entry processing.")

        # Determine Pending Type for display message
        pending_type_str = action # Fallback
        with mt5_sync_lock:
             tick = mt5.symbol_info_tick(symbol)
             if tick:
                 if action == "BUY":
                      pending_type_str = "BUY LIMIT" if entry_price < tick.ask else "BUY STOP"
                 elif action == "SELL":
                      pending_type_str = "SELL LIMIT" if entry_price > tick.bid else "SELL STOP"

        await safe_reply(update, context, f"Entry price for {pending_type_str} set to {entry_price:.{digits}f}.")
        await safe_reply(update, context, "Now, enter stop loss price (or 0 for no SL):\nType /cancel to abort.")

        return LIMIT_ORDER_SL # Proceed to SL input

    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid number format. Please enter a valid entry price:")
        return LIMIT_ORDER_ENTRY # Ask again
    except Exception as e:
         logging.error(f"Error in limit_order_entry: {e}", exc_info=True)
         await safe_reply(update, context, "An unexpected error occurred. Cancelling order setup.")
         context.user_data.clear()
         return ConversationHandler.END


async def limit_order_sl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles stop loss price input for limit orders."""
    if not update.message or not context.user_data.get("limit_order"):
        logging.warning("limit_order_sl called without message or user_data.")
        await safe_reply(update, context, "Session expired or data missing. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

    user_input = update.message.text.strip()
    limit_data = context.user_data["limit_order"]
    symbol = limit_data["symbol"]

    if user_input.lower() == 'cancel':
        await cfg_cancel(update, context) # Use generic cancel handler
        return ConversationHandler.END

    try:
        sl_price = float(user_input)
        limit_data["sl_price"] = sl_price if sl_price > 1e-9 else 0.0 # Store 0.0 if user enters 0 or near zero

        digits = 5 # Default digits
        with mt5_sync_lock:
            info = mt5.symbol_info(symbol)
            if info: digits = info.digits
            else: logging.warning(f"Could not get symbol info for {symbol} during limit SL processing.")

        sl_text = f"{limit_data['sl_price']:.{digits}f}" if limit_data['sl_price'] > 1e-9 else "None"
        await safe_reply(update, context, f"Stop loss set to {sl_text}. Placing pending order...")

        # Proceed to place the order
        await place_limit_order(update, context)

        return ConversationHandler.END # End the conversation after attempting to place
    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid number format. Please enter a valid stop loss price (or 0 for no SL):")
        return LIMIT_ORDER_SL # Ask again
    except Exception as e:
         logging.error(f"Error in limit_order_sl: {e}", exc_info=True)
         await safe_reply(update, context, "An unexpected error occurred. Cancelling order setup.")
         context.user_data.clear()
         return ConversationHandler.END

async def place_limit_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Places a limit/stop order based on user input collected in context.user_data."""
    data = context.user_data.get("limit_order")
    if not data:
        logging.error("place_limit_order called without limit_order data in context.")
        await safe_reply(update, context, "Internal error: Order data missing.")
        context.user_data.clear()
        return

    symbol = data.get("symbol")
    strategy = data.get("strategy", "Unknown") # Default strategy if missing?
    timeframe = data.get("timeframe", "Unknown")
    action = data.get("action") # Original BUY/SELL intention
    signal_id = data.get("signal_id")
    entry_price = data.get("entry_price")
    sl_price = data.get("sl_price") # Can be 0.0 or a float > 0
    opened_by = data.get("opened_by", "limit_manual")

    if not all([symbol, action, entry_price is not None]):
        logging.error(f"place_limit_order missing essential data: symbol={symbol}, action={action}, entry_price={entry_price}")
        await safe_reply(update, context, "Internal error: Order data incomplete. Cancelling.")
        context.user_data.clear()
        return

    order_type_base = mt5.ORDER_TYPE_BUY if action == "BUY" else mt5.ORDER_TYPE_SELL
    # place_order_async determines the specific LIMIT/STOP type based on entry_price vs market, but it needs the base BUY/SELL type
    # We pass entry_price and let place_order_async decide the MT5 request type (BUY_LIMIT/STOP etc.)

    # Lot size for manual limit orders could use the default lot size or strategy default
    lot_size = config["trade_manager"]["default_lot_size"]
    if strategy in config["trade_manager"]["strategy_settings"] and config["trade_manager"]["strategy_settings"][strategy].get("lot_size") is not None:
        lot_size = config["trade_manager"]["strategy_settings"][strategy]["lot_size"]


    logging.info(f"Attempting to place manual limit order for signal {signal_id}: {symbol} {action} @ {entry_price} with SL {sl_price}")

    result, order_ticket = await place_order_async(
        order_type=order_type_base,
        symbol=symbol,
        strategy=strategy,
        timeframe=timeframe, # Pass timeframe for comment/tracking
        signal_id=signal_id,
        lot_size=lot_size, # Use calculated lot size
        entry_price=entry_price,
        sl_price=sl_price, # Pass the explicit SL price
        opened_by=opened_by # Mark origin
    )

    # Report outcome
    digits = 5
    with mt5_sync_lock: # Get digits for formatting
        info = mt5.symbol_info(symbol)
        if info: digits = info.digits

    final_text = ""
    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
        order_ticket = result.order if result.order > 0 else order_ticket # Prefer result.order for pending
        sl_display = f"SL at {sl_price:.{digits}f}" if sl_price is not None and sl_price > 1e-9 else "no SL"
        final_text = f"✅ Pending {action} order placed for {symbol} at {entry_price:.{digits}f} ({sl_display}).\nOrder Ticket: <code>{order_ticket}</code>"
        # Signal status update happens inside place_order_async
    else:
        error_comment = getattr(result, 'comment', 'Unknown MT5 Error') if result else 'Order Send Failed'
        retcode = getattr(result, 'retcode', 'N/A') if result else 'N/A'
        final_text = f"❌ Failed to place pending {action} order for {symbol} at {entry_price:.{digits}f}.\nReason: {error_comment} (Code: {retcode})"
        # Signal status update happens inside place_order_async on failure

    await safe_reply(update, context, final_text)
    context.user_data.clear() # Clear data after attempt


# =============================================================================
# Configuration Conversations Handlers (Scanner & Trade Manager)
# Expanded for detailed prompts and new settings
# =============================================================================

async def cmd_configure_scanner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().lower() if update.message else ""
    if text == "default":
        # Apply all default scanner settings at once
        config["scanner"] = DEFAULT_CONFIG["scanner"].copy()
        mb, nb = compute_required_bars(config["scanner"])
        config["scanner"]["min_bars"] = mb
        config["scanner"]["num_bars"] = nb
        config["scanner"]["min_bars_trend"] = max(
            config["scanner"]["trend_bb_period"] * 10, 300
        )
        save_config(config)
        await safe_reply(update, context, "✅ Applied default scanner settings and auto-saved configuration.")
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data["configuring_scanner"] = {}
    await safe_reply(update, context,
        "🚀 Scanner Setup: (or send 'default' to accept all defaults immediately)\n"
        "Enter timeframes (comma-separated, e.g., M1,M5,H1) from:\n"
        f"<code>{', '.join(AVAILABLE_TIMEFRAMES)}</code>\nType /cancel to abort."
    )
    return SC_TIMEFRAMES

async def process_scanner_timeframes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END
    if user_input.lower() == 'default':
        default_tfs = DEFAULT_CONFIG['scanner']['timeframes']
        context.user_data['configuring_scanner']['timeframes'] = default_tfs
        await safe_reply(update, context, f"✔️ Using default timeframes: {', '.join(default_tfs)}\n"
                                     "🔣 Step 2: Enter symbols (comma-separated), e.g., EURUSD,GBPUSD:")
        return SC_SYMBOLS
    timeframes = [tf.strip().upper() for tf in user_input.split(',')]
    valid = [tf for tf in timeframes if tf in AVAILABLE_TIMEFRAMES]
    if not valid:
        await safe_reply(update, context,
                         f"⛔️ No valid timeframes entered. Choose from: {', '.join(AVAILABLE_TIMEFRAMES)} or 'default'.")
        return SC_TIMEFRAMES
    context.user_data['configuring_scanner']['timeframes'] = valid
    await safe_reply(update, context, f"✔️ Timeframes set: {', '.join(valid)}\n"
                                 "🔣 Step 2: Enter symbols (comma-separated), e.g., EURUSD,GBPUSD:")
    return SC_SYMBOLS


async def process_scanner_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END

    # Accept 'default' or custom symbols without validation
    if user_input.lower() == 'default':
        symbols = DEFAULT_CONFIG["scanner"]["symbols"]
    else:
        symbols = [s.strip() for s in user_input.split(',')]

    context.user_data['configuring_scanner']['symbols'] = symbols
    def_bb_period = DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_period"]
    await safe_reply(update, context, f"✔️ Symbols set: {', '.join(symbols)}\n"
                                 f"🔣 Step 3: Enter Bollinger Bands period for signal generation (default {def_bb_period}, type 'default' to use):\nType /cancel")
    return SC_INDICATORS_BB_PERIOD


async def process_scanner_strategy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes strategy selection for scanner."""
    if not update.message: return SC_STRATEGY
    user_input = update.message.text.strip().lower()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strategy = user_input.replace(" ", "_") # Normalize to underscore
    if strategy in STRATEGY_ALLOWED_TFS:
        # Validate selected TFs are appropriate for the strategy
        selected_tfs = context.user_data["configuring_scanner"].get("timeframes", [])
        allowed_for_strategy = STRATEGY_ALLOWED_TFS[strategy]
        incompatible_tfs = [tf for tf in selected_tfs if tf not in allowed_for_strategy]

        if incompatible_tfs:
            compatible_tfs = [tf for tf in selected_tfs if tf in allowed_for_strategy]
            if not compatible_tfs:
                 await safe_reply(update, context, f"⛔️ Selected strategy '{strategy.title().replace('_', ' ')}' is incompatible with ALL selected timeframes ({', '.join(selected_tfs)}). Allowed for {strategy}: {', '.join(allowed_for_strategy)}\nPlease choose a strategy compatible with your TFs, or re-run /configure_scanner to select appropriate TFs.\nType /cancel to abort.")
                 return SC_STRATEGY
            else:
                 await safe_reply(update, context, f"⚠️ Selected strategy '{strategy.title().replace('_', ' ')}' is incompatible with timeframes: {', '.join(incompatible_tfs)}.\nIgnoring incompatible timeframes. Scanning on: {', '.join(compatible_tfs)}.")
                 context.user_data["configuring_scanner"]["timeframes"] = compatible_tfs # Update to only compatible TFs


        context.user_data["configuring_scanner"]["strategy"] = strategy
        def_bb_period = DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_period"]
        await safe_reply(update, context, f"✔️ Using strategy: {strategy.title().replace('_', ' ')}\n"
                                          f"✔️ Step 4 (Indicators): Enter Bollinger Bands period for signal generation (default {def_bb_period}, type 'default' to use):\nType /cancel")
        return SC_INDICATORS_BB_PERIOD
    else:
        await safe_reply(update, context, "⛔️ Invalid strategy. Please choose 'long_medium' or 'Scalping_strategy'.\nType /cancel to abort.")
        return SC_STRATEGY

# Helper for processing numeric config values in conversations, handling 'default'
async def _process_numeric_config_conv(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       state_key: int, temp_config_key: Tuple[str, ...],
                                       prompt: str, next_state: int,
                                       is_int: bool = True, min_val=None, max_val=None, default_val=None) -> int:
    if not update.message: return state_key
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = default_val
        logging.debug(f"User chose default '{default_val}' for {temp_config_key[-1]}.")
    else:
        try:
            value = int(user_input) if is_int else float(user_input)
            valid = True
            if min_val is not None and value < min_val: valid = False
            if max_val is not None and value > max_val: valid = False

            if valid:
                value_to_set = value
            else:
                range_str = ""
                if min_val is not None: range_str += f" >= {min_val}"
                if max_val is not None: range_str += f" <= {max_val}"
                await safe_reply(update, context, f"⛔️ Invalid value. Please enter a {'integer' if is_int else 'number'}{range_str} or type 'default' ({default_val}):\nType /cancel to abort.")
                return state_key # Ask again
        except ValueError:
            await safe_reply(update, context, f"⛔️ Invalid format. Please enter a valid {'integer' if is_int else 'number'} or type 'default' ({default_val}):\nType /cancel to abort.")
            return state_key # Ask again
        except Exception as e:
            logging.error(f"Error processing numeric config for {temp_config_key[-1]}: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return state_key # Ask again

    # Navigate temporary config dict to set value
    cfg_level = context.user_data["configuring_scanner"] # Start from scanner temp dict
    for key in temp_config_key[:-1]:
         if key not in cfg_level or not isinstance(cfg_level[key], dict):
              cfg_level[key] = {} # Create nested dict if missing
         cfg_level = cfg_level[key]

    cfg_level[temp_config_key[-1]] = value_to_set
    await safe_reply(update, context, prompt)
    return next_state


async def process_scanner_bb_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_bb_period = DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_period"]
    def_bb_dev = DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_dev"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_BB_PERIOD, ("indicators", "bollinger_period"),
                                             f"✔️ BB Period set. Enter Bollinger Bands deviation (default {def_bb_dev}, type 'default' to use):\nType /cancel", SC_INDICATORS_BB_DEV,
                                             is_int=True, min_val=1, default_val=def_bb_period)

async def process_scanner_bb_dev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_bb_dev = DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_dev"]
    def_stoch_k = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_k"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_BB_DEV, ("indicators", "bollinger_dev"),
                                             f"✔️ BB Deviation set. Enter Stochastic %K period (default {def_stoch_k}, type 'default' to use):\nType /cancel", SC_INDICATORS_STOCH_K,
                                             is_int=False, min_val=0.1, default_val=def_bb_dev)

async def process_scanner_stoch_k(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_stoch_k = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_k"]
    def_stoch_d = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_d"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_STOCH_K, ("indicators", "stoch_k"),
                                             f"✔️ Stoch %K set. Enter Stochastic %D period (default {def_stoch_d}, type 'default' to use):\nType /cancel", SC_INDICATORS_STOCH_D,
                                             is_int=True, min_val=1, default_val=def_stoch_k)

async def process_scanner_stoch_d(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_stoch_d = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_d"]
    def_stoch_slow = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_slowing"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_STOCH_D, ("indicators", "stoch_d"),
                                             f"✔️ Stoch %D set. Enter Stochastic slowing (default {def_stoch_slow}, type 'default' to use):\nType /cancel", SC_INDICATORS_STOCH_SLOW,
                                             is_int=True, min_val=1, default_val=def_stoch_d)

async def process_scanner_stoch_slow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_stoch_slow = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_slowing"]
    def_stoch_os = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_oversold"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_STOCH_SLOW, ("indicators", "stoch_slowing"),
                                             f"✔️ Stoch Slowing set. Enter Stochastic oversold level (default {def_stoch_os}, type 'default' to use):\nType /cancel", SC_INDICATORS_STOCH_OVERSOLD,
                                             is_int=False, min_val=0, max_val=100, default_val=def_stoch_slow) # Changed to False, levels can be float

async def process_scanner_stoch_oversold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_stoch_os = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_oversold"]
    def_stoch_ob = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_overbought"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_STOCH_OVERSOLD, ("indicators", "stoch_oversold"),
                                             f"✔️ Stoch Oversold set. Enter Stochastic overbought level (default {def_stoch_ob}, type 'default' to use):\nType /cancel", SC_INDICATORS_STOCH_OVERBOUGHT,
                                             is_int=False, min_val=0, max_val=100, default_val=def_stoch_os)


async def process_scanner_stoch_overbought(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes Stochastic overbought level input and validates against oversold."""
    if not update.message: return SC_INDICATORS_STOCH_OVERBOUGHT
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    def_stoch_ob = DEFAULT_CONFIG["scanner"]["indicators"]["stoch_overbought"]
    stoch_os = context.user_data["configuring_scanner"]["indicators"]["stoch_oversold"] # Get previously set OS

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_stoch_ob
        logging.debug(f"User chose default '{def_stoch_ob}' for Stoch Overbought.")
    else:
        try:
            value = float(user_input)
            if 0 < value <= 100 and value > stoch_os:
                 value_to_set = value
            else:
                 await safe_reply(update, context, f"⛔️ Invalid value. Overbought level must be > Oversold ({stoch_os}) and <= 100. Enter a valid number or 'default' ({def_stoch_ob}):\nType /cancel to abort.")
                 return SC_INDICATORS_STOCH_OVERBOUGHT
        except ValueError:
            await safe_reply(update, context, f"⛔️ Invalid format. Please enter a valid number or type 'default' ({def_stoch_ob}):\nType /cancel to abort.")
            return SC_INDICATORS_STOCH_OVERBOUGHT
        except Exception as e:
            logging.error(f"Error processing Stoch overbought: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return SC_INDICATORS_STOCH_OVERBOUGHT

    context.user_data["configuring_scanner"]["indicators"]["stoch_overbought"] = value_to_set
    def_rsi_p = DEFAULT_CONFIG["scanner"]["indicators"]["rsi_period"]
    await safe_reply(update, context, f"✔️ Stoch Overbought set. Enter RSI period (default {def_rsi_p}, type 'default' to use):\nType /cancel")
    return SC_INDICATORS_RSI_PERIOD


async def process_scanner_rsi_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_rsi_p = DEFAULT_CONFIG["scanner"]["indicators"]["rsi_period"]
    def_rsi_os = DEFAULT_CONFIG["scanner"]["indicators"]["rsi_oversold"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_RSI_PERIOD, ("indicators", "rsi_period"),
                                             f"✔️ RSI Period set. Enter RSI oversold level (default {def_rsi_os}, type 'default' to use):\nType /cancel", SC_INDICATORS_RSI_OVERSOLD,
                                             is_int=True, min_val=1, default_val=def_rsi_p) # RSI Period usually int


async def process_scanner_rsi_oversold(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_rsi_os = DEFAULT_CONFIG["scanner"]["indicators"]["rsi_oversold"]
    def_rsi_ob = DEFAULT_CONFIG["scanner"]["indicators"]["rsi_overbought"]
    return await _process_numeric_config_conv(update, context, SC_INDICATORS_RSI_OVERSOLD, ("indicators", "rsi_oversold"),
                                             f"✔️ RSI Oversold set. Enter RSI overbought level (default {def_rsi_ob}, type 'default' to use):\nType /cancel", SC_INDICATORS_RSI_OVERBOUGHT,
                                             is_int=False, min_val=0, max_val=100, default_val=def_rsi_os)


async def process_scanner_rsi_overbought(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes RSI overbought level input and validates against oversold, then moves to trend filter config."""
    if not update.message: return SC_INDICATORS_RSI_OVERBOUGHT
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    def_rsi_ob = DEFAULT_CONFIG["scanner"]["indicators"]["rsi_overbought"]
    rsi_os = context.user_data["configuring_scanner"]["indicators"]["rsi_oversold"]

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_rsi_ob
        logging.debug(f"User chose default '{def_rsi_ob}' for RSI Overbought.")
    else:
        try:
            value = float(user_input)
            if 0 < value <= 100 and value > rsi_os:
                value_to_set = value
            else:
                 await safe_reply(update, context, f"⛔️ Invalid value. RSI Overbought must be > Oversold ({rsi_os}) and <= 100. Enter a valid number or 'default' ({def_rsi_ob}):\nType /cancel to abort.")
                 return SC_INDICATORS_RSI_OVERBOUGHT
        except ValueError:
            await safe_reply(update, context, f"⛔️ Invalid format. Please enter a valid number or type 'default' ({def_rsi_ob}):\nType /cancel to abort.")
            return SC_INDICATORS_RSI_OVERBOUGHT
        except Exception as e:
            logging.error(f"Error processing RSI overbought: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return SC_INDICATORS_RSI_OVERBOUGHT


    context.user_data["configuring_scanner"]["indicators"]["rsi_overbought"] = value_to_set
    def_trend_tf = DEFAULT_CONFIG["scanner"]["overall_trend_timeframe"]

    # Only ask for Trend Filter config if the current mode is FULL_AUTO, otherwise skip
    # Or ask anyway and save it, but mention it's only used in FULL_AUTO? Let's ask anyway.
    available_trend_tfs = [tf for tf in AVAILABLE_TIMEFRAMES if tf not in context.user_data["configuring_scanner"]["timeframes"]] # Suggest higher TFs than signal TFs
    await safe_reply(update, context, f"✔️ RSI Overbought set.\n"
                                     f"📈 Step 5 (Trend Filter - used in FULL_AUTO mode): Enter timeframe for overall trend analysis (default {def_trend_tf}, type 'default' to use) from available TFs:\n<code>{', '.join(available_trend_tfs) or ', '.join(AVAILABLE_TIMEFRAMES)}</code>\nType /cancel")
    return SC_TREND_TF


async def process_scanner_trend_tf(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes trend timeframe input."""
    if not update.message:
        return SC_TREND_TF
    user_input = update.message.text.strip().upper()
    if user_input.lower() == "cancel":
        await cfg_cancel(update, context)
        return ConversationHandler.END

    def_trend_tf = DEFAULT_CONFIG["scanner"]["overall_trend_timeframe"].upper()
    value_to_set = None

    if user_input == "DEFAULT":
        value_to_set = def_trend_tf
    elif user_input in AVAILABLE_TIMEFRAMES:
        value_to_set = user_input
        # Optionally check TF hierarchy here
        selected_tfs = context.user_data["configuring_scanner"].get("timeframes", [])
    else:
        available_trend_tfs = [tf for tf in AVAILABLE_TIMEFRAMES if tf not in context.user_data["configuring_scanner"].get("timeframes", [])]
        await safe_reply(
            update,
            context,
            f"⛔️ Invalid timeframe. Please choose from:<code>{', '.join(available_trend_tfs) or ', '.join(AVAILABLE_TIMEFRAMES)}</code> or type 'default' ({def_trend_tf}).\nType /cancel to abort."
        )
        return SC_TREND_TF

    context.user_data["configuring_scanner"]["overall_trend_timeframe"] = value_to_set
    await safe_reply(
        update,
        context,
        "📊 Step 6 (Trend Bias): Use Bollinger Bands for trend bias reference? (yes/no, default no)\nType /cancel",
    )
    return SC_TREND_USE_BB


async def process_scanner_trend_use_bb(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores user's preference for using BB in trend bias (reference only)."""
    if not update.message:
        return SC_TREND_USE_BB
    user_input = update.message.text.strip().lower()
    if user_input == "cancel":
        await cfg_cancel(update, context)
        return ConversationHandler.END

    if user_input in ("yes", "y"):
        use_bb = True
    elif user_input in ("no", "n", "default"):
        use_bb = False
    else:
        await safe_reply(
            update,
            context,
            "⛔️ Invalid choice. Please type 'yes' or 'no'.\nType /cancel",
        )
        return SC_TREND_USE_BB

    cfg_sc = context.user_data.get("configuring_scanner", {})
    cfg_sc["trend_use_bb"] = use_bb
    bb_cfg = cfg_sc.get("indicators", {})
    cfg_sc["trend_bb_period"] = bb_cfg.get(
        "bollinger_period",
        DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_period"],
    )
    cfg_sc["trend_bb_dev"] = bb_cfg.get(
        "bollinger_dev",
        DEFAULT_CONFIG["scanner"]["indicators"]["bollinger_dev"],
    )

    mb, nb = compute_required_bars(cfg_sc)
    cfg_sc["min_bars"] = mb
    cfg_sc["num_bars"] = nb
    cfg_sc["min_bars_trend"] = max(cfg_sc["trend_bb_period"] * 10, 300)

    config["scanner"].update(cfg_sc)
    context.user_data.pop("configuring_scanner", None)
    INDICATORS_CACHE.clear()
    last_bar_times.clear()

    await safe_reply(
        update,
        context,
        f"🎉 Scanner configuration complete! Calculated min bars: {mb}, fetch bars: {nb}. Configuration saved automatically.",
    )
    save_config(config)
    return ConversationHandler.END







# Scanner Configuration Handler
configure_scanner_handler = ConversationHandler(
    entry_points=[CommandHandler("configure_scanner", cmd_configure_scanner)],
    states={
        SC_TIMEFRAMES: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_timeframes)],
        SC_SYMBOLS: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_symbols)],
        SC_INDICATORS_BB_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_bb_period)],
        SC_INDICATORS_BB_DEV: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_bb_dev)],
        SC_INDICATORS_STOCH_K: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_stoch_k)],
        SC_INDICATORS_STOCH_D: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_stoch_d)],
        SC_INDICATORS_STOCH_SLOW: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_stoch_slow)],
        SC_INDICATORS_STOCH_OVERSOLD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_stoch_oversold)],
        SC_INDICATORS_STOCH_OVERBOUGHT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_stoch_overbought)],
        SC_INDICATORS_RSI_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_rsi_period)],
        SC_INDICATORS_RSI_OVERSOLD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_rsi_oversold)],
        SC_INDICATORS_RSI_OVERBOUGHT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_rsi_overbought)],
        SC_TREND_TF: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_trend_tf)],
        SC_TREND_USE_BB: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_scanner_trend_use_bb)],
    },
    fallbacks=[CommandHandler("cancel", cfg_cancel)],
    conversation_timeout=900, # 15 minute timeout
)


# Trade Manager Configuration Handler (Expanded)
async def cmd_configure_trade_manager(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().lower() if update.message else ""
    if text == "default":
        # Apply all default trade manager settings at once
        config["trade_manager"] = DEFAULT_CONFIG["trade_manager"].copy()
        save_config(config)
        await safe_reply(update, context, "✅ Applied default trade-manager settings and auto-saved configuration.")
        return ConversationHandler.END

    """Starts trade manager configuration conversation."""
    context.user_data.clear()
    context.user_data["configuring_trade_manager"] = {"strategy_settings": {}} # Temporary dict for trade manager config

    def_margin = DEFAULT_CONFIG["trade_manager"]["margin_level_stop"]
    await safe_reply(update, context,
                     "🚀 Trade Manager Setup: (or send 'default' to accept all defaults immediately)\n"
                     "🔣 Step 1: Include cooperative scalping? (yes/no):")
    return TM_COOP_INCLUDE

# Helper for numeric config in Trade Manager conv, handling 'default'
async def _process_numeric_config_tm_conv(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                          state_key: int, temp_config_keys: Tuple[str, ...],
                                          prompt: str, next_state: int,
                                          is_int: bool = True, min_val=None, max_val=None, default_val=None, allow_none=False) -> int:
    if not update.message: return state_key
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = default_val
        logging.debug(f"User chose default '{default_val}' for {temp_config_keys[-1]}.")
    elif allow_none and not user_input:
         value_to_set = None
         logging.debug(f"User chose None (empty input) for {temp_config_keys[-1]}.")
    else:
        try:
            value = int(user_input) if is_int else float(user_input)
            valid = True
            if min_val is not None and value < min_val: valid = False
            if max_val is not None and value > max_val: valid = False

            if valid:
                value_to_set = value
            else:
                range_str = ""
                if min_val is not None: range_str += f" >= {min_val}"
                if max_val is not None: range_str += f" <= {max_val}"
                none_option = ", leave empty for no limit" if allow_none else ""
                await safe_reply(update, context, f"⛔️ Invalid value. Please enter a {'integer' if is_int else 'number'}{range_str}{none_option} or type 'default' ({default_val}):\nType /cancel to abort.")
                return state_key # Ask again
        except ValueError:
            none_option = ", leave empty for no limit" if allow_none else ""
            await safe_reply(update, context, f"⛔️ Invalid format. Please enter a valid {'integer' if is_int else 'number'}{none_option} or type 'default' ({default_val}):\nType /cancel to abort.")
            return state_key # Ask again
        except Exception as e:
            logging.error(f"Error processing numeric config for {temp_config_keys[-1]}: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return state_key # Ask again

    # Navigate temporary config dict
    cfg_level = context.user_data["configuring_trade_manager"]
    for key in temp_config_keys[:-1]:
         if key not in cfg_level or not isinstance(cfg_level[key], dict):
              cfg_level[key] = {} # Create nested dict if missing
         cfg_level = cfg_level[key]

    cfg_level[temp_config_keys[-1]] = value_to_set
    await safe_reply(update, context, prompt)
    return next_state

async def process_tm_coop_include(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip().lower()
    if user_input == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    include = user_input in ('yes', 'y')
    context.user_data['configuring_trade_manager']['coop_include'] = include
    if include:
        await safe_reply(update, context, "🔣 Enter scalping lot size (e.g., 0.01):")
        return TM_COOP_LOT
    else:
        def_margin = DEFAULT_CONFIG['trade_manager']['margin_level_stop']
        await safe_reply(update, context, f"📉 Step 2: Enter margin level percentage below which trading is stopped (e.g., {def_margin}, type 'default'):\nType /cancel")
        return TM_MARGIN_LEVEL

async def process_tm_coop_lot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    try:
        lot = float(user_input)
        context.user_data.setdefault('configuring_trade_manager', {}).setdefault('coop_scalp', {})['lot'] = lot
    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid lot size. Enter a number:")
        return TM_COOP_LOT
    await safe_reply(update, context, "🔣 Enter scalping profit target in USD:")
    return TM_COOP_PROFIT

async def process_tm_coop_profit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    try:
        profit = float(user_input)
        context.user_data['configuring_trade_manager']['coop_scalp']['profit'] = profit
    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid profit target. Enter a number in USD:")
        return TM_COOP_PROFIT
    # After profit target, move directly to margin level configuration
    def_margin = DEFAULT_CONFIG['trade_manager']['margin_level_stop']
    await safe_reply(update, context,
                     f"📉 Step 2: Enter margin level percentage below which trading is stopped (e.g., {def_margin}, type 'default'):\nType /cancel")
    return TM_MARGIN_LEVEL

async def process_tm_coop_sl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Set scalping stop loss as a percentage of the lock-in value."""
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END

    try:
        sl_pct = float(user_input)
        if sl_pct <= 0:
            raise ValueError
    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid percentage. Enter a positive number:")
        return TM_COOP_SL

    lock_cfg = context.user_data.get('configuring_trade_manager', {}).get('lock_in_value', {})
    lock_amount = lock_cfg.get('value')
    sl_value = lock_amount * sl_pct / 100 if lock_amount is not None else None
    context.user_data.setdefault('configuring_trade_manager', {}).setdefault('coop_scalp', {})['sl'] = sl_value

    await safe_reply(update, context, "🔀 Step 9: Choose trade opening mode (sequential/concurrent):")
    return TM_OPEN_MODE

async def process_tm_margin_level(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_margin = DEFAULT_CONFIG["trade_manager"]["margin_level_stop"]
    def_lot = DEFAULT_CONFIG["trade_manager"]["default_lot_size"]
    return await _process_numeric_config_tm_conv(update, context, TM_MARGIN_LEVEL, ("margin_level_stop",),
                                               f"✔️ Margin level set. 💰 Step 3: Enter default lot size (used for manual trades & strategy fallback, e.g., {def_lot}, type 'default'):\nType /cancel", TM_DEFAULT_LOT,
                                               is_int=False, min_val=1.0, default_val=def_margin)


async def process_tm_default_lot_size(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_lot = DEFAULT_CONFIG["trade_manager"]["default_lot_size"]
    def_sl = DEFAULT_CONFIG["trade_manager"]["default_stop_loss_pips"]
    return await _process_numeric_config_tm_conv(update, context, TM_DEFAULT_LOT, ("default_lot_size",),
                                               f"✔️ Default lot size set. 🛑 Step 3: Enter default stop loss in pips (used for manual trades & strategy fallback, e.g., {def_sl}, type 'default'):\nType /cancel", TM_DEFAULT_SL,
                                               is_int=False, min_val=0.0, default_val=def_lot)


async def process_tm_default_stop_loss(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_sl = DEFAULT_CONFIG["trade_manager"]["default_stop_loss_pips"]
    def_max_t = DEFAULT_CONFIG["trade_manager"]["max_open_trades"]
    return await _process_numeric_config_tm_conv(update, context, TM_DEFAULT_SL, ("default_stop_loss_pips",),
                                               f"✔️ Default SL pips set. 🗄️ Step 4: Enter maximum total open trades allowed across all pairs (e.g., {def_max_t}, type 'default'):\nType /cancel", TM_MAX_TRADES,
                                               is_int=False, min_val=0.0, default_val=def_sl) # SL can be 0


async def process_tm_max_trades(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_max_t = DEFAULT_CONFIG["trade_manager"]["max_open_trades"]
    def_spread = DEFAULT_CONFIG["trade_manager"]["max_spread_pips"]
    prompt = f"✔️ Max trades set. 📉 Step 5: Enter maximum allowed spread in pips before placing a market order (e.g., 2.0, type 'default' for {def_spread if def_spread is not None else 'no limit'}, or leave empty for no limit):\nType /cancel"
    return await _process_numeric_config_tm_conv(update, context, TM_MAX_TRADES, ("max_open_trades",),
                                               prompt, TM_MAX_SPREAD,
                                               is_int=True, min_val=1, default_val=def_max_t)


async def process_tm_max_spread(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes max spread input, handles 'default' or empty for None."""
    if not update.message: return TM_MAX_SPREAD
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    def_spread = DEFAULT_CONFIG["trade_manager"]["max_spread_pips"]
    value_to_set = None

    if user_input.lower() == 'default':
         value_to_set = def_spread
         logging.debug(f"User chose default '{def_spread}' for max spread.")
    elif not user_input: # Empty input
        value_to_set = None
        logging.debug("User left input empty for max spread, setting to None.")
    else:
        try:
            value = float(user_input)
            if value >= 0: # Spread can be 0 for ECN type brokers
                 value_to_set = value if value > 1e-9 else None # Treat 0 as no limit? Or allow 0 limit? Let's allow 0.
                 if value > 1e-9: value_to_set = value
                 else: value_to_set = None # Treat 0/very small as no limit based on common usage
            else:
                 await safe_reply(update, context, "⛔️ Spread limit must be zero or positive. Enter again, 'default' for current default, or leave empty for no limit:\nType /cancel to abort.")
                 return TM_MAX_SPREAD
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid format. Enter a number, 'default' for current default, or leave empty for no limit:\nType /cancel to abort.")
            return TM_MAX_SPREAD
        except Exception as e:
            logging.error(f"Error processing max spread: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_MAX_SPREAD


    context.user_data["configuring_trade_manager"]["max_spread_pips"] = value_to_set

    def_pip_confirm = DEFAULT_CONFIG["trade_manager"]["pip_confirmation_threshold"]
    await safe_reply(update, context, f"✔️ Max spread set.\n"
                                     f"🚦 Step 6 (FULL_AUTO only): Enter minimum pips price must move *after* signal candle close to confirm entry (0 to disable, default {def_pip_confirm}, type 'default'):\nType /cancel")
    return TM_PIP_CONFIRM_THRESH


async def process_tm_pip_confirm_thresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_pip_confirm = DEFAULT_CONFIG["trade_manager"]["pip_confirmation_threshold"]
    def_timeout = DEFAULT_CONFIG["trade_manager"]["pip_confirmation_timeout_sec"]
    return await _process_numeric_config_tm_conv(update, context, TM_PIP_CONFIRM_THRESH, ("pip_confirmation_threshold",),
                                               f"✔️ Pip confirmation threshold set. ⏱️ Step 7 (FULL_AUTO only): Enter maximum seconds to wait for pip confirmation before timing out (default {def_timeout}, type 'default'):\nType /cancel", TM_PIP_CONFIRM_TIMEOUT,
                                               is_int=True, min_val=0, default_val=def_pip_confirm)


async def process_tm_pip_confirm_timeout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip().lower()
    if user_input == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    if user_input == 'default':
        timeout = DEFAULT_CONFIG['trade_manager']['pip_confirmation_timeout_sec']
    else:
        try:
            timeout = int(user_input)
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid input. Please enter a number or 'default'.")
            return TM_PIP_CONFIRM_TIMEOUT

    context.user_data['configuring_trade_manager']['pip_confirmation_timeout_sec'] = timeout
    await safe_reply(update, context, f"✔️ Pip confirmation timeout set to {timeout} seconds.")

    def_interval = DEFAULT_CONFIG['trade_manager']['pip_confirm_check_interval_sec']
    await safe_reply(update, context,
                     f"⏱️ Step 8: Enter seconds between pip confirmation checks (default {def_interval}, type 'default'):")
    return TM_PIP_CONFIRM_INTERVAL

async def process_tm_pip_confirm_interval(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user_input = update.message.text.strip().lower()
    if user_input == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    if user_input == 'default':
        interval = DEFAULT_CONFIG['trade_manager']['pip_confirm_check_interval_sec']
    else:
        try:
            interval = float(user_input)
            if interval <= 0:
                raise ValueError
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid input. Please enter a positive number or 'default'.")
            return TM_PIP_CONFIRM_INTERVAL

    context.user_data['configuring_trade_manager']['pip_confirm_check_interval_sec'] = interval
    await safe_reply(update, context, f"✔️ Pip confirmation check interval set to {interval} seconds.")

    await safe_reply(update, context, "📰 Step 9: Apply news filter? (yes/no):")
    return TM_USE_NEWS_FILTER

async def process_tm_use_news_filter(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip().lower()
    if choice == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    use_filter = choice in ('yes', 'y')
    context.user_data['configuring_trade_manager']['use_news_filter'] = use_filter
    await safe_reply(update, context, "🤖 Step 10: Enable AI-driven decisions and auto training? (yes/no):")
    return TM_AI_TRAINING

async def process_tm_ai_training(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip().lower()
    if choice == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    enable_ai = choice in ('yes', 'y')
    context.user_data['configuring_trade_manager']['ai_training'] = enable_ai
    await safe_reply(update, context, "🔒 Step 11: Enter profit lock-in method (fixed/trailing, type 'default'):")
    return TM_LOCKIN_METHOD


async def process_tm_lockin_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip().lower()
    if choice == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    if choice not in ('fixed', 'trailing', 'default'):
        await safe_reply(update, context, "⛔️ Invalid. Choose 'fixed', 'trailing' or 'default'.")
        return TM_LOCKIN_METHOD
    context.user_data['configuring_trade_manager']['lock_in_method'] = choice
    await safe_reply(update, context, "🔢 Enter lock-in value as 'Trigger/Amount' (e.g., 500/100):")
    return TM_LOCKIN_VALUE

async def process_tm_lockin_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    txt = update.message.text.strip()
    if txt.lower() == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    try:
        trigger, value = parse_lock_in(txt)
        context.user_data['configuring_trade_manager']['lock_in_value'] = {'trigger': trigger, 'value': value}
    except ValueError as e:
        await safe_reply(update, context, str(e))
        return TM_LOCKIN_VALUE
    await safe_reply(update, context, "🔣 Enter scalping stop loss as percentage of lock-in:")
    return TM_COOP_SL

async def process_tm_open_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text.strip().lower()
    if choice == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    if choice not in ('sequential', 'concurrent'):
        await safe_reply(update, context, "⛔️ Choose 'sequential' or 'concurrent'.")
        return TM_OPEN_MODE
    context.user_data['configuring_trade_manager']['open_mode'] = choice
    await safe_reply(
        update,
        context,
        "Step 10: Select take-profit method for long trades (pips/money/bollingerband):",
    )
    return TM_LONG_TP_METHOD

async def process_long_tp_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    choice = None
    if query:
        await query.answer()
        data = decode_callback_data(query.data)
        choice = data.get("tp_method")
    elif update.message:
        choice = update.message.text.strip().lower()
    if not choice:
        await safe_reply(update, context, "Invalid selection. Choose again:")
        return TM_LONG_TP_METHOD
    if choice == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END

    lm_cfg = context.user_data.setdefault('configuring_trade_manager', {}).setdefault('strategy_settings', {}).setdefault('long_medium', {})
    if choice == 'pips':
        lm_cfg['take_profit_mode'] = 'pips'
        await safe_reply(update, context, "Enter take-profit pips amount for long trades (e.g., 50.0):")
        return TM_LONG_TP_VALUE
    if choice == 'money':
        lm_cfg['take_profit_mode'] = 'money'
        await safe_reply(update, context, "Enter take-profit money (USD) amount for long trades (e.g., 20.0):")
        return TM_LONG_TP_VALUE
    if choice in ('bb', 'bollingerband', 'bollingerbands'):
        lm_cfg['take_profit_mode'] = 'bollingerbands'
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "Middle", callback_data=encode_callback_data({'tp_bb': 'middle'})
                ),
                InlineKeyboardButton(
                    "Top/Bottom", callback_data=encode_callback_data({'tp_bb': 'top'})
                ),
            ]
        ])
        await safe_reply(
            update,
            context,
            "Take profit at Bollinger Band middle or top/bottom?",
            reply_markup=keyboard,
        )
        return TM_LONG_TP_BB_TYPE
    await safe_reply(update, context, "Invalid selection. Choose again:")
    return TM_LONG_TP_METHOD

async def process_long_tp_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message:
        return TM_LONG_TP_VALUE
    txt = update.message.text.strip()
    if txt.lower() == 'cancel':
        await cfg_cancel(update, context)
        return ConversationHandler.END
    try:
        val = float(txt)
        if val <= 0:
            raise ValueError
    except ValueError:
        await safe_reply(update, context, "Enter a positive number:")
        return TM_LONG_TP_VALUE
    lm_cfg = context.user_data.setdefault('configuring_trade_manager', {}).setdefault('strategy_settings', {}).setdefault('long_medium', {})
    lm_cfg['take_profit_value'] = val
    await safe_reply(update, context, "🎉 Trade-manager configuration complete! Saving settings...")
    return await process_tm_final_step(update, context)

async def process_long_tp_bb_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if not query:
        await safe_reply(update, context, "Invalid selection.")
        return TM_LONG_TP_BB_TYPE
    await query.answer()
    data = decode_callback_data(query.data)
    bb_type = data.get('tp_bb')
    if bb_type not in ('middle', 'top'):
        await safe_reply(update, context, "Invalid selection.")
        return TM_LONG_TP_BB_TYPE
    lm_cfg = context.user_data.setdefault('configuring_trade_manager', {}).setdefault('strategy_settings', {}).setdefault('long_medium', {})
    lm_cfg['take_profit_bb_type'] = bb_type
    await safe_reply(update, context, "🎉 Trade-manager configuration complete! Saving settings...")
    return await process_tm_final_step(update, context)





async def process_tm_strategy_settings_intro(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return TM_STRATEGY_SETTINGS
    user_input = update.message.text.strip().lower()
    if user_input == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    if user_input in ['yes', 'y']:
        keyboard = [
            [InlineKeyboardButton("long_medium", callback_data=encode_callback_data({"cfg_strat": "long_medium"}))],
            [InlineKeyboardButton("Scalping_strategy", callback_data=encode_callback_data({"cfg_strat": "scalping_strategy"}))],
            [InlineKeyboardButton("Cooperative", callback_data=encode_callback_data({"cfg_strat": "cooperative"}))],
            [InlineKeyboardButton("Done with Strategies", callback_data=encode_callback_data({"cfg_strat": "done"}))]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_reply(update, context, "Select strategy to configure:", reply_markup=reply_markup)
        return TM_STRAT_SELECT
    elif user_input in ['no', 'n']:
        await safe_reply(update, context, "⏭️ Skipping strategy-specific settings.\nProceeding to final step.")
        await process_tm_final_step(update, context) # Skip directly to finalization
        return ConversationHandler.END # Handled by final_step

    else:
        await safe_reply(update, context, "⛔️ Invalid input. Reply Yes or No:\nType /cancel")
        return TM_STRATEGY_SETTINGS

async def select_strategy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = decode_callback_data(query.data)
    if not data or "cfg_strat" not in data:
         logging.warning("Invalid callback data for strat select.")
         try: await query.edit_message_text("Error processing selection.", reply_markup=None)
         except: pass
         return TM_STRAT_SELECT # Stay in state?

    strat_key = data["cfg_strat"]
    if strat_key == "done":
        await query.edit_message_text("✅ Done configuring strategy settings.", reply_markup=None)
        await process_tm_final_step(update, context)
        return ConversationHandler.END # Handled by final_step

    # Initialize strategy settings in temp dict if not present
    if strat_key not in context.user_data["configuring_trade_manager"]["strategy_settings"]:
         # Use copy of defaults for the selected strategy as base
         base_settings = DEFAULT_CONFIG["trade_manager"]["strategy_settings"].get(strat_key, {})
         context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key] = base_settings.copy()

    temp_strat_settings = context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]

    # --- Handle Configuration based on selected strategy ---
    if strat_key == "long_medium":
        def_lot = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("lot_size") # Default is None
        def_sl_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("stop_loss_pips") # Default is None
        def_tp_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("take_profit_pips") # Default is None

        current_lot = temp_strat_settings.get("lot_size", 'Not Set')
        current_sl_pips = temp_strat_settings.get("stop_loss_pips", 'Not Set')
        current_tp_pips = temp_strat_settings.get("take_profit_pips", 'Not Set')


        await query.edit_message_text(
             f"⚙️ Configuring Long/Medium settings:\n"
             f"Current Lot Size: {current_lot}\n"
             f"Current SL (pips): {current_sl_pips}\n"
             f"Current TP (pips): {current_tp_pips}\n\n"
             f"Enter Lot Size for Long/Medium trades (type '{def_lot}' for default {def_lot} or leave empty for no override):\nType /cancel to abort.", reply_markup=None
        )
        context.user_data["current_strat_cfg"] = strat_key
        return TM_STRAT_LM_LOT

    elif strat_key == "scalping_strategy":
         def_mult = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("lot_size_multiplier")
         def_sl_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("stop_loss_pips")
         def_tp_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("take_profit_pips")

         current_mult = temp_strat_settings.get("lot_size_multiplier", 'Not Set')
         current_sl_pips = temp_strat_settings.get("stop_loss_pips", 'Not Set')
         current_tp_pips = temp_strat_settings.get("take_profit_pips", 'Not Set')

         await query.edit_message_text(
             f"⚙️ Configuring Scalping settings (used in FULL_AUTO Coop mode):\n"
             f"Current Lot Multiplier: {current_mult}\n"
             f"Current SL (pips): {current_sl_pips}\n"
             f"Current TP (pips): {current_tp_pips}\n\n"
             f"Enter Lot Size Multiplier (e.g., 0.5 for half the LM/Default lot, default {def_mult}, type 'default'):\nType /cancel to abort.", reply_markup=None
         )
         context.user_data["current_strat_cfg"] = strat_key
         return TM_STRAT_SC_LOT_MULT

    elif strat_key == "cooperative":
         def_method = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["cooperative"].get("lock_in_method")
         def_value = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["cooperative"].get("lock_in_value")

         current_method = temp_strat_settings.get("lock_in_method", 'Not Set')
         current_value = temp_strat_settings.get("lock_in_value", 'Not Set')

         await query.edit_message_text(
             f"⚙️ Configuring Cooperative settings (used in FULL_AUTO mode to enable scalping):\n"
             f"Current Lock-in Method: {current_method}\n"
             f"Current Lock-in Value: {current_value}\n\n"
             f"Choose lock-in method: 'pips' or 'money' (USD, type 'default' for {def_method}):\nType /cancel to abort.", reply_markup=None
         )
         context.user_data["current_strat_cfg"] = strat_key
         return TM_COOP_METHOD

    # Should not reach here
    await safe_reply(update, context, "⛔️ Unknown strategy selected.")
    return TM_STRAT_SELECT # Stay here

# Long/Medium Strategy Configuration Steps
async def process_tm_strat_lm_lot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return TM_STRAT_LM_LOT
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    if not strat_key or strat_key != "long_medium": # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS # Go back to strat selection

    def_lot = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("lot_size") # Can be None

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_lot
        logging.debug(f"User chose default '{def_lot}' for LM Lot Size.")
    elif not user_input: # Allow clearing override
         value_to_set = None
         logging.debug("User left input empty for LM Lot Size, setting to None.")
    else:
        try:
            value = float(user_input)
            if value > 0:
                 value_to_set = value
            else:
                 await safe_reply(update, context, "⛔️ Lot size must be positive. Enter a number > 0, 'default', or leave empty for no override:\nType /cancel")
                 return TM_STRAT_LM_LOT
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid format. Enter a number, 'default', or leave empty:\nType /cancel")
            return TM_STRAT_LM_LOT
        except Exception as e:
            logging.error(f"Error processing LM lot size: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_STRAT_LM_LOT

    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["lot_size"] = value_to_set

    def_sl_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("stop_loss_pips")
    await safe_reply(update, context, f"✔️ LM Lot Size set. Enter Stop Loss in pips for Long/Medium trades (type '{def_sl_pips}' for default {def_sl_pips}, or leave empty for no override):\nType /cancel")
    return TM_STRAT_LM_SL


async def process_tm_strat_lm_sl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return TM_STRAT_LM_SL
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    if not strat_key or strat_key != "long_medium": # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS

    def_sl_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("stop_loss_pips")

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_sl_pips
        logging.debug(f"User chose default '{def_sl_pips}' for LM SL Pips.")
    elif not user_input: # Allow clearing override
         value_to_set = None
         logging.debug("User left input empty for LM SL Pips, setting to None.")
    else:
        try:
            value = float(user_input)
            if value >= 0:
                 value_to_set = value
            else:
                 await safe_reply(update, context, "⛔️ SL in pips must be zero or positive. Enter a number >= 0, 'default', or leave empty for no override:\nType /cancel")
                 return TM_STRAT_LM_SL
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid format. Enter a number, 'default', or leave empty:\nType /cancel")
            return TM_STRAT_LM_SL
        except Exception as e:
            logging.error(f"Error processing LM SL: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_STRAT_LM_SL


    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["stop_loss_pips"] = value_to_set

    def_tp_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("take_profit_pips")
    await safe_reply(update, context, f"✔️ LM SL Pips set. Enter Take Profit in pips for Long/Medium trades (type '{def_tp_pips}' for default {def_tp_pips}, or leave empty for no override / use BBM):\nType /cancel")
    return TM_STRAT_LM_TP


async def process_tm_strat_lm_tp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return TM_STRAT_LM_TP
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    if not strat_key or strat_key != "long_medium": # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS

    def_tp_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["long_medium"].get("take_profit_pips")

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_tp_pips
        logging.debug(f"User chose default '{def_tp_pips}' for LM TP Pips.")
    elif not user_input: # Allow clearing override / using BBM fallback
         value_to_set = None
         logging.debug("User left input empty for LM TP Pips, setting to None (use BBM fallback).")
    else:
        try:
            value = float(user_input)
            if value > 0:
                 value_to_set = value
            else:
                 await safe_reply(update, context, "⛔️ TP in pips must be positive. Enter a number > 0, 'default', or leave empty for no override / use BBM:\nType /cancel")
                 return TM_STRAT_LM_TP
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid format. Enter a number, 'default', or leave empty:\nType /cancel")
            return TM_STRAT_LM_TP
        except Exception as e:
            logging.error(f"Error processing LM TP: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_STRAT_LM_TP


    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["take_profit_pips"] = value_to_set

    await safe_reply(update, context, "✅ Long/Medium strategy settings updated. Returning to strategy selection.")
    # Return to strategy selection menu
    keyboard = [
        [InlineKeyboardButton("long_medium", callback_data=encode_callback_data({"cfg_strat": "long_medium"}))],
        [InlineKeyboardButton("Scalping_strategy", callback_data=encode_callback_data({"cfg_strat": "scalping_strategy"}))],
        [InlineKeyboardButton("Cooperative", callback_data=encode_callback_data({"cfg_strat": "cooperative"}))],
        [InlineKeyboardButton("Done with Strategies", callback_data=encode_callback_data({"cfg_strat": "done"}))]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_send_message(context, update.effective_chat.id, "Select strategy to configure:", reply_markup=reply_markup)
    context.user_data.pop("current_strat_cfg", None)
    return TM_STRAT_SELECT


# Scalping Strategy Configuration Steps
async def process_tm_strat_sc_lot_mult(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return TM_STRAT_SC_LOT_MULT
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    if not strat_key or strat_key != "scalping_strategy": # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS

    def_mult = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("lot_size_multiplier")

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_mult
        logging.debug(f"User chose default '{def_mult}' for SC Lot Multiplier.")
    else:
        try:
            value = float(user_input)
            if value > 0: # Allow 0 multiplier? Maybe not useful
                 value_to_set = value
            else:
                 await safe_reply(update, context, "⛔️ Lot multiplier must be positive. Enter a number > 0 or 'default':\nType /cancel")
                 return TM_STRAT_SC_LOT_MULT
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid format. Enter a number or 'default':\nType /cancel")
            return TM_STRAT_SC_LOT_MULT
        except Exception as e:
            logging.error(f"Error processing SC lot multiplier: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_STRAT_SC_LOT_MULT

    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["lot_size_multiplier"] = value_to_set

    def_sl_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("stop_loss_pips")
    await safe_reply(update, context, f"✔️ SC Lot Multiplier set. Enter Stop Loss in pips for Scalping trades (default {def_sl_pips}, type 'default'):\nType /cancel")
    return TM_STRAT_SC_SL

async def process_tm_strat_sc_sl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    def_sl_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("stop_loss_pips")
    def_tp_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("take_profit_pips")
    return await _process_numeric_config_tm_conv(update, context, TM_STRAT_SC_SL, ("strategy_settings", "scalping_strategy", "stop_loss_pips"),
                                               f"✔️ SC SL Pips set. Enter Take Profit in pips for Scalping trades (default {def_tp_pips}, type 'default'):\nType /cancel", TM_STRAT_SC_TP,
                                               is_int=False, min_val=0.0, default_val=def_sl_pips)


async def process_tm_strat_sc_tp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes Scalping TP pips and completes Scalping config."""
    if not update.message: return TM_STRAT_SC_TP
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    if not strat_key or strat_key != "scalping_strategy": # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS

    def_tp_pips = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["scalping_strategy"].get("take_profit_pips")

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_tp_pips
        logging.debug(f"User chose default '{def_tp_pips}' for SC TP Pips.")
    else:
        try:
            value = float(user_input)
            if value > 0:
                 value_to_set = value
            else:
                 await safe_reply(update, context, "⛔️ TP in pips must be positive. Enter a number > 0 or 'default':\nType /cancel")
                 return TM_STRAT_SC_TP
        except ValueError:
            await safe_reply(update, context, "⛔️ Invalid format. Enter a number or 'default':\nType /cancel")
            return TM_STRAT_SC_TP
        except Exception as e:
            logging.error(f"Error processing SC TP: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_STRAT_SC_TP


    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["take_profit_pips"] = value_to_set

    await safe_reply(update, context, "✅ Scalping strategy settings updated. Returning to strategy selection.")
    # Return to strategy selection menu
    keyboard = [
        [InlineKeyboardButton("long_medium", callback_data=encode_callback_data({"cfg_strat": "long_medium"}))],
        [InlineKeyboardButton("Scalping_strategy", callback_data=encode_callback_data({"cfg_strat": "scalping_strategy"}))],
        [InlineKeyboardButton("Cooperative", callback_data=encode_callback_data({"cfg_strat": "cooperative"}))],
        [InlineKeyboardButton("Done with Strategies", callback_data=encode_callback_data({"cfg_strat": "done"}))]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_send_message(context, update.effective_chat.id, "Select strategy to configure:", reply_markup=reply_markup)
    context.user_data.pop("current_strat_cfg", None)
    return TM_STRAT_SELECT


# Cooperative Strategy Configuration Steps
async def process_tm_coop_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.message: return TM_COOP_METHOD
    user_input = update.message.text.strip().lower()
    if user_input == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    if not strat_key or strat_key != "cooperative": # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS

    def_method = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["cooperative"].get("lock_in_method")

    value_to_set = None
    if user_input == 'default':
        value_to_set = def_method
        logging.debug(f"User chose default '{def_method}' for Coop Method.")
    elif user_input in ["pips", "money", "bbm"]:
        value_to_set = user_input
    else:
        await safe_reply(update, context, "⛔️ Invalid method. Choose 'pips', 'money', or 'bbm', or type 'default':\nType /cancel")
        return TM_COOP_METHOD

    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["lock_in_method"] = value_to_set
    if value_to_set == 'bbm':
        await safe_reply(update, context, f"✔️ Coop Lock-in Method set to BBM. The bot will use Bollinger Band Middle to lock in profits.\nReturning to strategy selection.")
        keyboard = [
            [InlineKeyboardButton("long_medium", callback_data=encode_callback_data({"cfg_strat": "long_medium"}))],
            [InlineKeyboardButton("Scalping_strategy", callback_data=encode_callback_data({"cfg_strat": "scalping_strategy"}))],
            [InlineKeyboardButton("Cooperative", callback_data=encode_callback_data({"cfg_strat": "cooperative"}))],
            [InlineKeyboardButton("Done with Strategies", callback_data=encode_callback_data({"cfg_strat": "done"}))]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await safe_send_message(context, update.effective_chat.id, "Select strategy to configure:", reply_markup=reply_markup)
        context.user_data.pop("current_strat_cfg", None)
        return TM_STRAT_SELECT
    # existing pips/money flow:
    prompt = "Enter positive pips to lock in:" if value_to_set == "pips" else "Enter positive USD amount to lock in:"
    def_value = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["cooperative"].get("lock_in_value")
    await safe_reply(update, context, f"✔️ Coop Lock-in Method set to {value_to_set}.\n{prompt} (type '{def_value}' for default {def_value}):\nType /cancel")
    context.user_data["coop_lock_method_selected"] = value_to_set
    return TM_COOP_VALUE

    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["lock_in_method"] = value_to_set
    prompt = "Enter positive pips to lock in:" if value_to_set == "pips" else "Enter positive USD amount to lock in:"
    def_value = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["cooperative"].get("lock_in_value")
    await safe_reply(update, context, f"✔️ Coop Lock-in Method set to {value_to_set}.\n{prompt} (type '{def_value}' for default {def_value}):\nType /cancel")
    # Need to differentiate the next step based on the method chosen
    context.user_data["coop_lock_method_selected"] = value_to_set # Store for next step validation
    return TM_COOP_VALUE

async def process_tm_coop_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes Cooperative lock-in value and completes Coop config."""
    if not update.message: return TM_COOP_VALUE
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    strat_key = context.user_data.get("current_strat_cfg")
    coop_method = context.user_data.get("coop_lock_method_selected")
    if not strat_key or strat_key != "cooperative" or not coop_method: # Defensive check
         await safe_reply(update, context, "Internal error: Strategy context missing.")
         return TM_STRATEGY_SETTINGS


    def_value = DEFAULT_CONFIG["trade_manager"]["strategy_settings"]["cooperative"].get("lock_in_value")
    is_int = coop_method == "pips" # Pips are typically integers
    min_val = 0.0 # Value must be positive
    if coop_method == "pips": min_val = 1 # Pips should be at least 1

    value_to_set = None
    if user_input.lower() == 'default':
        value_to_set = def_value
        logging.debug(f"User chose default '{def_value}' for Coop Value.")
    else:
        try:
            value = int(user_input) if is_int else float(user_input)
            if value >= min_val:
                 value_to_set = value
            else:
                 await safe_reply(update, context, f"⛔️ Lock-in value must be >= {min_val}. Enter a number, 'default', or leave empty:\nType /cancel")
                 return TM_COOP_VALUE
        except ValueError:
            await safe_reply(update, context, f"⛔️ Invalid format. Enter a valid {'integer' if is_int else 'number'}, 'default', or leave empty:\nType /cancel")
            return TM_COOP_VALUE
        except Exception as e:
            logging.error(f"Error processing Coop value: {e}", exc_info=True)
            await safe_reply(update, context, "An unexpected error occurred. Please try again.\nType /cancel to abort.")
            return TM_COOP_VALUE


    context.user_data["configuring_trade_manager"]["strategy_settings"][strat_key]["lock_in_value"] = value_to_set

    await safe_reply(update, context, "✅ Cooperative settings updated. Returning to strategy selection.")
    # Return to strategy selection menu
    keyboard = [
        [InlineKeyboardButton("long_medium", callback_data=encode_callback_data({"cfg_strat": "long_medium"}))],
        [InlineKeyboardButton("Scalping_strategy", callback_data=encode_callback_data({"cfg_strat": "scalping_strategy"}))],
        [InlineKeyboardButton("Cooperative", callback_data=encode_callback_data({"cfg_strat": "cooperative"}))],
        [InlineKeyboardButton("Done with Strategies", callback_data=encode_callback_data({"cfg_strat": "done"}))]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await safe_send_message(context, update.effective_chat.id, "Select strategy to configure:", reply_markup=reply_markup)
    context.user_data.pop("current_strat_cfg", None)
    context.user_data.pop("coop_lock_method_selected", None)
    return TM_STRAT_SELECT


# Final step for Trade Manager configuration

async def process_tm_final_step(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Merge temporary settings into config and save."""
    tm_cfg = context.user_data.get("configuring_trade_manager", {})
    if tm_cfg:
        strat_cfg = tm_cfg.pop("strategy_settings", {})
        config["trade_manager"].update(tm_cfg)
        for k, v in strat_cfg.items():
            config["trade_manager"]["strategy_settings"].setdefault(k, {}).update(v)
        save_config(config)
    context.user_data.clear()
    await safe_reply(update, context, "✅ Trade-manager settings saved.")
    return ConversationHandler.END



# Trade Manager Configuration Handler
configure_trade_manager_handler = ConversationHandler(
    entry_points=[CommandHandler("configure_trade_manager", cmd_configure_trade_manager)],
    states={
        TM_COOP_INCLUDE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_coop_include)],
        TM_COOP_LOT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_coop_lot)],
        TM_COOP_PROFIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_coop_profit)],
        TM_MARGIN_LEVEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_margin_level)],
        TM_DEFAULT_LOT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_default_lot_size)],
        TM_DEFAULT_SL: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_default_stop_loss)],
        TM_MAX_TRADES: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_max_trades)],
        TM_MAX_SPREAD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_max_spread)],
        TM_PIP_CONFIRM_THRESH: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_pip_confirm_thresh)],
        TM_PIP_CONFIRM_TIMEOUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_pip_confirm_timeout)],
        TM_PIP_CONFIRM_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_pip_confirm_interval)],
        TM_USE_NEWS_FILTER:   [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_use_news_filter)],
        TM_AI_TRAINING:      [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_ai_training)],
        TM_LOCKIN_METHOD:    [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_lockin_method)],
        TM_LOCKIN_VALUE:     [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_lockin_value)],
        TM_COOP_SL:          [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_coop_sl)],
        TM_OPEN_MODE:        [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_open_mode)],
        TM_LONG_TP_METHOD:   [MessageHandler(filters.TEXT & ~filters.COMMAND, process_long_tp_method)],
        TM_LONG_TP_VALUE:    [MessageHandler(filters.TEXT & ~filters.COMMAND, process_long_tp_value)],
        TM_LONG_TP_BB_TYPE:  [CallbackQueryHandler(process_long_tp_bb_type)],
        TM_STRATEGY_SETTINGS: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strategy_settings_intro)],
        TM_STRAT_SELECT: [CallbackQueryHandler(select_strategy_callback)],
        TM_STRAT_LM_LOT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strat_lm_lot)],
        TM_STRAT_LM_SL: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strat_lm_sl)],
        TM_STRAT_LM_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strat_lm_tp)],
        TM_STRAT_SC_LOT_MULT: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strat_sc_lot_mult)],
        TM_STRAT_SC_SL: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strat_sc_sl)],
        TM_STRAT_SC_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_strat_sc_tp)],
        TM_COOP_METHOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_coop_method)],
        TM_COOP_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_tm_coop_value)],
    },
    fallbacks=[CommandHandler("cancel", cfg_cancel), CallbackQueryHandler(cfg_cancel, pattern='^cancel$')],
    conversation_timeout=900, # 15 minute timeout
)



async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays a list of available commands."""
    commands = [
        "/start - Show this help message",
        "/configure_scanner - Configure scanner settings",
        "/configure_trade_manager - Configure trade-manager settings",
        "/config_option - Show current bot configurations",
        "/start_scanner - Start scanning and trade-manager",
        "/set_mode &lt;semi|full&gt; - Select operational mode",
        "/lock_in - Manually lock in profit for cooperative scalping",
        "/trail_stop - Configure a global trailing stop",
        "/status - Show current bot status",
        "/close &lt;symbol|ticket&gt; - Close a position",
        "/close_all [symbol] - Close all positions (optionally filtered)",
        "/cancel - Abort any ongoing configuration conversation"
    ]
    text = "<b>Available Commands:</b>\n" + "\n".join(commands)
    await safe_reply(update, context, text, parse_mode=ParseMode.HTML)

# =============================================================================
# Mode Setting Command
# =============================================================================













async def cmd_config_option(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current scanner and trade-manager configuration."""
    sc = config.get("scanner", {})
    tm = config.get("trade_manager", {})
    mode = config.get("operational_mode", MODE_SEMI_AUTO)

    # Helper
    def get_nested(d, keys, default=None):
        for k in keys:
            if isinstance(d, dict) and k in d:
                d = d[k]
            else:
                return default
        return d

    default_scalp = get_nested(DEFAULT_CONFIG, ["trade_manager","coop_scalp"], {})
    scalp_cfg = get_nested(tm, ["coop_scalp"], {})

    lm_settings = tm.get('strategy_settings', {}).get('long_medium', {})
    lm_tp_mode = lm_settings.get('take_profit_mode')
    lm_tp_value = lm_settings.get('take_profit_value')
    lm_tp_bb_type = lm_settings.get('take_profit_bb_type')
    if lm_tp_mode == 'pips':
        tp_display = f"{lm_tp_value} pips" if lm_tp_value is not None else '–'
    elif lm_tp_mode == 'money':
        tp_display = f"{lm_tp_value} USD" if lm_tp_value is not None else '–'
    elif lm_tp_mode == 'bollingerbands':
        if lm_tp_bb_type == 'top':
            tp_display = 'Bollinger top/bottom'
        elif lm_tp_bb_type == 'middle':
            tp_display = 'Bollinger middle'
        else:
            tp_display = 'Bollinger'
    else:
        tp_display = '–'

    lines = [
        "<b>Current Configuration:</b>",
        "",
        "🕑 <b>Scanner Settings:</b>",
        f" • Timeframes: {', '.join(sc.get('timeframes', [])) or 'None'}",
        f" • Symbols: {', '.join(sc.get('symbols', [])) or 'None'}",
        f" • Bollinger Bands: period {get_nested(sc, ['indicators','bollinger_period'], '–')}, deviation {get_nested(sc, ['indicators','bollinger_dev'], '–')}",
        f" • Stochastic: %K {get_nested(sc,['indicators','stoch_k'],'–')}, %D {get_nested(sc,['indicators','stoch_d'],'–')}, slowing {get_nested(sc,['indicators','stoch_slowing'],'–')}, oversold {get_nested(sc,['indicators','stoch_oversold'],'–')}, overbought {get_nested(sc,['indicators','stoch_overbought'],'–')}",
        f" • RSI: period {get_nested(sc,['indicators','rsi_period'],'–')}, oversold {get_nested(sc,['indicators','rsi_oversold'],'–')}, overbought {get_nested(sc,['indicators','rsi_overbought'],'–')}",
        f" • Trend TF: {sc.get('overall_trend_timeframe','–')}",
        f" • Trend Bias: {'Bollinger Bands' if sc.get('trend_use_bb') else 'TEMA/DEMA'}",
    ]
    if sc.get('trend_use_bb'):
        lines.extend([
            f" • Trend BB Period: {sc.get('trend_bb_period','–')}",
            f" • Trend BB Deviation: {sc.get('trend_bb_dev','–')}",
        ])
    lines.extend([
        f" • Num Bars: min {sc.get('min_bars','–')}, fetch {sc.get('num_bars','–')}",
        "",
        "💹 <b>Trade Manager Settings:</b>",
        f" • Margin Stop Level: {tm.get('margin_level_stop','–')}%",
        f" • Default Lot Size: {tm.get('default_lot_size','–')}",
        f" • Default SL (pips): {tm.get('default_stop_loss_pips','–')}",
        f" • Default TP: {tp_display}",
        f" • Max Open Trades: {tm.get('max_open_trades','–')}",
        f" • Max Spread (pips): {tm.get('max_spread_pips','–')}",
        f" • Pip Confirmation Threshold: {tm.get('pip_confirmation_threshold','–')}",
        f" • Pip Confirmation Timeout: {tm.get('pip_confirmation_timeout_sec','–')}s",
        f" • Pip Confirmation Check Interval: {tm.get('pip_confirm_check_interval_sec','–')}s",
        f" • Use News Filter: {tm.get('use_news_filter', False)}",
        f" • AI Training Enabled: {tm.get('ai_training', False)}",
        f" • Trend Filter TF: {sc.get('overall_trend_timeframe','–')}",
        f" • Trend Bias: {'Bollinger Bands' if sc.get('trend_use_bb') else 'TEMA/DEMA'}",
    ])
    if sc.get('trend_use_bb'):
        lines.extend([
            f" • Trend BB Period: {sc.get('trend_bb_period','–')}",
            f" • Trend BB Deviation: {sc.get('trend_bb_dev','–')}",
        ])
    lines.extend([
        f" • Profit Lock-in Method: {tm.get('lock_in_method','–')}",
        f" • Lock-in Value: {get_nested(tm, ['lock_in_value','trigger'],'–')}/{get_nested(tm, ['lock_in_value','value'],'–')}",
        f" • Include Coop Scalping: {tm.get('coop_include','–')}",
        f" • Scalping Lot Size: {scalp_cfg.get('lot','–')}",
        f" • Scalping Profit Target: {scalp_cfg.get('profit','–')}",
        f" • Scalping Stop Loss: {scalp_cfg.get('sl','–')}",
        f" • Trade Opening Mode: {tm.get('open_mode','–')}",
        "",
        f"⚙️ <b>Mode:</b> {mode}"
    ])
    text = "\n".join(lines)
    await safe_reply(update, context, text, parse_mode=ParseMode.HTML)



async def  cmd_set_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sets the operational mode (semi-auto or full-auto)."""
    if not context.args or context.args[0].lower() not in ["semi", "full"]:
        await safe_reply(update, context, "❌ Usage: /set_mode <semi|full>\n\n"
                                         f"Current Mode: {config['operational_mode']}\n\n"
                                         "<b>Semi-Auto:</b> Signals sent to Telegram with buttons. You choose which to trade. Monitors manage *all* open trades. Cooperative scalping can be triggered manually via /lock_in if desired.\n\n"
                                         "<b>Full-Auto:</b> Signals are processed automatically. Bot decides based on filters (Trend, Pip Confirmation) whether to trade. Cooperative scalping activates automatically upon parent trade profit lock-in.")
        return

    new_mode = MODE_SEMI_AUTO if context.args[0].lower() == "semi" else MODE_FULL_AUTO

    if config["operational_mode"] == new_mode:
        await safe_reply(update, context, f"ℹ️ Bot is already in <b>{new_mode}</b> mode.", parse_mode=ParseMode.HTML)
        return

    # --- Mode Switching Logic ---
    config["operational_mode"] = new_mode
    save_config(config) # Persist the mode change

    message = f"✅ Operational mode set to <b>{new_mode}</b>."
    if new_mode == MODE_FULL_AUTO:
        message += "\nBot will now automatically process signals after applying filters."
        # Note: Full-Auto requires Scanner and Trade Manager to be running.
        # If they are not, the user should be informed to start them.
        with STATE_LOCK:
             if not runtime_state["scanner_running"] or not runtime_state["trade_manager_running"]:
                  message += "\n⚠️ Scanner and Trade Manager are not running. Use /start_scanner."
             else:
                  # If switching TO Full-Auto while running, clear any pending manual buttons
                  await safe_send_message(context, config["telegram"]["chat_id"], "Clearing pending manual signal buttons...")
                  # Logic to clear buttons... this is tricky as buttons don't persist state directly.
                  # A simpler approach is to inform the user pending buttons are now inactive.
                  message += "\nExisting signal buttons may no longer function as intended."
    elif new_mode == MODE_SEMI_AUTO:
        message += "\nBot will now send signals with action buttons for manual trade placement."
        # In Semi-Auto, cooperative_auto_trading_active is effectively False.
        # Scalping is only enabled for a pair if the user triggers lock-in on a Long/Medium trade for that pair.
        # Need to check cooperative_states - if any are 'achieved', their scalping should still be active until closed.
        # If switching FROM Full-Auto to Semi-Auto, existing auto trades/coop states persist but new ones aren't created automatically.

    await safe_reply(update, context, message, parse_mode=ParseMode.HTML)
    logging.info(f"Operational mode changed to {new_mode}")

# =============================================================================
# Advanced Trade Commands & Cooperative Mode (Manual Trigger)
# =============================================================================

# Note: /full_auto_trading command is effectively replaced by /set_mode full
# And its config is integrated into /configure_trade_manager (Cooperative settings)

async def cmd_lock_in(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Initiates manual profit lock-in conversation, can trigger cooperative scalping in SEMI-AUTO."""
    # In SEMI-AUTO mode, this command is the way a user manually triggers the coop scalping for a specific pair
    # In FULL-AUTO mode, locking happens automatically via the cooperative_monitor
    # This command will primarily be used in SEMI-AUTO mode.

    await safe_reply(update, context, "💰 Manual Profit Lock-in:\nEnter the <b>Ticket ID</b> of the position you want to lock profit on.\nType /cancel to abort.")
    return LOCK_IN_PIPS_MANUAL # Using the pips step for simplicity, but input is ticket ID first


async def process_lock_in_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
     """Processes ticket ID input for manual profit lock-in."""
     if not update.message: return LOCK_IN_PIPS_MANUAL
     user_input = update.message.text.strip()
     if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

     try:
         ticket_id = int(user_input)
         context.user_data["lock_in_ticket"] = ticket_id

         # Verify ticket exists and is an open position
         pos = None
         with mt5_sync_lock:
              pos_list = mt5.positions_get(ticket=ticket_id)
              if pos_list:
                   pos = pos_list[0]

         if not pos:
              await safe_reply(update, context, f"⛔️ No open position found with ticket ID {ticket_id}. Please enter a valid open ticket ID:\nType /cancel to abort.")
              # Could optionally list open tickets here
              return LOCK_IN_PIPS_MANUAL # Ask again

         # Store position details temporarily
         context.user_data["lock_in_position_info"] = {
             "ticket": pos.ticket,
             "symbol": pos.symbol,
             "type": pos.type,
             "price_open": pos.price_open,
             "volume": pos.volume,
             "magic": pos.magic,
             "sl": pos.sl,
             "tp": pos.tp,
             "profit": pos.profit # Current floating profit
         }

         # Check if this is a bot-managed LM trade in SEMI-AUTO mode for Coop triggering
         is_semi_auto_coop_candidate = (
             config["operational_mode"] == MODE_SEMI_AUTO and
             pos.magic == MAGIC_DEFAULT # Check for LM magic
         )

         if is_semi_auto_coop_candidate:
              # For SEMI-AUTO coop, lock-in uses the configured Cooperative settings (money/pips and value)
              coop_settings = config["trade_manager"]["strategy_settings"].get("cooperative", {})
              lock_in_method = coop_settings.get("lock_in_method")
              lock_in_value = coop_settings.get("lock_in_value")

              if not lock_in_method or lock_in_value is None or lock_in_value <= 0:
                  await safe_reply(update, context, "⚠️ Cooperative lock-in settings are not configured for SEMI-AUTO mode. Please configure them via /configure_trade_manager (Cooperative section).\nOr enter pips to lock in manually for this specific trade:")
                  # Fallback to asking for pips manually if coop settings are missing
                  await safe_reply(update, context, f"Enter minimum pips in profit to lock for ticket {ticket_id} (e.g., 10):")
                  context.user_data["manual_lock_in_method"] = "pips_manual" # Flag manual pips method
                  return LOCK_IN_PIPS_MANUAL # Stay in this state, asking for pips

              # Attempt to apply the configured cooperative lock-in
              logging.info(f"Attempting Semi-Auto Cooperative lock-in for ticket {ticket_id} ({pos.symbol}). Method: {lock_in_method}, Value: {lock_in_value}")
              lock_success = await apply_cooperative_lock_in_manual(update, context, pos, lock_in_method, lock_in_value)

              if lock_success:
                  await safe_reply(update, context, f"✅ Semi-Auto Cooperative Lock-in applied for ticket {ticket_id}. Scalping enabled for {pos.symbol}. Use /status to verify.")
                  return ConversationHandler.END
              else:
                   await safe_reply(update, context, f"❌ Failed to apply Semi-Auto Cooperative Lock-in for ticket {ticket_id}. See logs for details.\nOr enter pips to lock in manually for this specific trade:")
                   # Fallback to asking for pips manually if auto lock-in fails
                   await safe_reply(update, context, f"Enter minimum pips in profit to lock for ticket {ticket_id} (e.g., 10):")
                   context.user_data["manual_lock_in_method"] = "pips_manual"
                   return LOCK_IN_PIPS_MANUAL

         else:
              # This is either not a LM trade, not bot managed, or mode is FULL-AUTO (where it's automatic)
              # In this case, only offer manual SL setting based on pips
              if config["operational_mode"] == MODE_FULL_AUTO:
                   await safe_reply(update, context, "ℹ️ Bot is in FULL-AUTO mode. Cooperative lock-in is automatic for relevant trades.\nUsing this command will only allow setting a fixed SL based on pips for ticket {ticket_id}.")
              elif pos.magic != MAGIC_DEFAULT:
                    await safe_reply(update, context, "ℹ️ This command is designed for Long/Medium trades (Magic {MAGIC_DEFAULT}). You entered a trade with Magic {pos.magic}.\nUsing this command will only allow setting a fixed SL based on pips for ticket {ticket_id}.")

              await safe_reply(update, context, f"Enter minimum pips in profit to lock for ticket {ticket_id} (e.g., 10):")
              context.user_data["manual_lock_in_method"] = "pips_manual"
              return LOCK_IN_PIPS_MANUAL # Ask for pips

     except ValueError:
         await safe_reply(update, context, "⛔️ Invalid number. Please enter a valid Ticket ID:")
         return LOCK_IN_PIPS_MANUAL # Ask again
     except Exception as e:
          logging.error(f"Error in process_lock_in_ticket: {e}", exc_info=True)
          await safe_reply(update, context, "An unexpected error occurred. Cancelling lock-in.")
          context.user_data.clear()
          return ConversationHandler.END


async def process_lock_in_pips_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes pips input for manual fixed SL adjustment (not cooperative lock-in logic)."""
    if not update.message or not context.user_data.get("lock_in_ticket"):
        logging.warning("process_lock_in_pips_value called without message or ticket info.")
        await safe_reply(update, context, "Session expired or data missing. Please try again.")
        context.user_data.clear()
        return ConversationHandler.END

    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    ticket_id = context.user_data["lock_in_ticket"]
    pos_info = context.user_data.get("lock_in_position_info") # Get position info

    if not pos_info:
         await safe_reply(update, context, "Internal error: Position info missing.")
         context.user_data.clear()
         return ConversationHandler.END

    try:
        lock_in_pips = float(user_input)
        if lock_in_pips < 0:
             await safe_reply(update, context, "⛔️ Pips to lock must be zero or positive. Enter again:")
             return LOCK_IN_PIPS_MANUAL # Ask again

        modified_count = 0
        error_count = 0

        with mt5_sync_lock:
            # Re-fetch position to get latest details (especially current price/profit)
            pos_list = mt5.positions_get(ticket=ticket_id)
            pos = pos_list[0] if pos_list else None

            if not pos:
                 await safe_reply(update, context, f"⚠️ Position {ticket_id} not found or closed.")
                 context.user_data.clear()
                 return ConversationHandler.END

            symbol = pos.symbol
            symbol_info = mt5.symbol_info(symbol)
            tick = mt5.symbol_info_tick(symbol)
            if not tick or not symbol_info or symbol_info.point <=0:
                logging.warning(f"Cannot get valid info/tick for {symbol} (ticket {ticket_id}) during manual pips lock-in.")
                await safe_reply(update, context, f"❌ Cannot get required market data for {symbol}. Cannot set SL.")
                context.user_data.clear()
                return ConversationHandler.END

            point = symbol_info.point
            digits = symbol_info.digits
            current_sl = pos.sl
            new_sl = 0.0
            profit_pips = 0.0

            # Calculate current profit in pips
            if pos.type == mt5.ORDER_TYPE_BUY:
                current_price = tick.bid # Use bid for buy profit calc
                profit_points = current_price - pos.price_open
                profit_pips = profit_points / point
                # Calculate new SL price based on *current price* trailing by lock_in_pips
                new_sl = round(current_price - lock_in_pips * point, digits)

            elif pos.type == mt5.ORDER_TYPE_SELL:
                current_price = tick.ask # Use ask for sell profit calc
                profit_points = pos.price_open - current_price
                profit_pips = profit_points / point
                # Calculate new SL price based on *current price* trailing by lock_in_pips
                new_sl = round(current_price + lock_in_pips * point, digits)

            # Check if SL needs update (only update if new SL is better/higher for BUY or lower for SELL, and not too close to market)
            # Also ensure new_sl is not too close to the current price (stops level)
            min_distance_points = symbol_info.trade_stops_level * point
            needs_update = False
            is_zero_sl = (current_sl == 0.0 or abs(current_sl) < 1e-9)

            if pos.type == mt5.ORDER_TYPE_BUY:
                 if new_sl > 0.0 and (is_zero_sl or new_sl > current_sl): # New SL is positive and better than current
                      if current_price - new_sl >= min_distance_points: # Check distance from current price
                           needs_update = True
                      else:
                          logging.warning(f"Calculated new SL {new_sl:.{digits}f} for {ticket_id} is too close to current price {current_price:.{digits}f}. Min distance needed: {min_distance_points/point:.1f} pips.")
                          await safe_reply(update, context, f"⚠️ Cannot set SL {new_sl:.{digits}f} for ticket {ticket_id}. It's too close to the market price. Minimum distance is {min_distance_points/point:.1f} pips.")

            elif pos.type == mt5.ORDER_TYPE_SELL:
                  if new_sl > 0.0 and (is_zero_sl or new_sl < current_sl): # New SL is positive and better than current
                      if new_sl - current_price >= min_distance_points: # Check distance from current price
                           needs_update = True
                      else:
                          logging.warning(f"Calculated new SL {new_sl:.{digits}f} for {ticket_id} is too close to current price {current_price:.{digits}f}. Min distance needed: {min_distance_points/point:.1f} pips.")
                          await safe_reply(update, context, f"⚠️ Cannot set SL {new_sl:.{digits}f} for ticket {ticket_id}. It's too close to the market price. Minimum distance is {min_distance_points/point:.1f} pips.")


            if needs_update:
                logging.info(f"Manually setting SL for ticket {ticket_id}. Current Profit: {profit_pips:.1f} pips. Requested trail: {lock_in_pips} pips. New SL: {new_sl:.{digits}f}")
                request = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "position": pos.ticket,
                    "symbol": symbol,
                    "sl": new_sl,
                    "tp": pos.tp # Keep existing TP
                }
                result = mt5.order_send(request)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    modified_count += 1
                    logging.info(f"Successfully updated SL for ticket {ticket_id}")
                else:
                    error_count += 1
                    err_comment = getattr(result, 'comment', 'Send Failed') if result else 'Send Failed'
                    logging.error(f"Failed to update SL for ticket {ticket_id}. Reason: {err_comment}")

            # --- Report Outcome ---
            msg = f"✅ Manual SL adjustment attempted for ticket {ticket_id} (trailing by {lock_in_pips} pips).\n"
            if modified_count > 0:
                 msg += f"Successfully set SL to {new_sl:.{digits}f}."
            else:
                 msg += "SL was not updated (either not in profit enough, new SL not better than existing, or error)."

            if error_count > 0:
                msg += f"\n⚠️ Failed to modify SL for ticket {ticket_id} (check logs)."

            await safe_reply(update, context, msg)

        context.user_data.clear()
        return ConversationHandler.END

    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid number. Enter zero or positive pips:")
        return LOCK_IN_PIPS_MANUAL # Ask again
    except Exception as e:
         logging.error(f"Error in process_lock_in_pips_value: {e}", exc_info=True)
         await safe_reply(update, context, "An unexpected error occurred during SL adjustment.")
         context.user_data.clear()
         return ConversationHandler.END


async def apply_cooperative_lock_in_manual(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                           position, method: str, value: float) -> bool:
    """Applies cooperative lock-in criteria to a specific position (used by manual command)."""
    ticket_id = position.ticket
    symbol = position.symbol
    current_profit_usd = position.profit

    lock_in_target_usd = 0
    if method == "pips":
        # Calculate required USD profit for lock-in pips
        with mt5_sync_lock:
            symbol_info = mt5.symbol_info(symbol)
            point = symbol_info.point if symbol_info else 0
            if point <= 1e-9:
                logging.error(f"Coop Manual Lock-in Failed: Invalid symbol info for {symbol} ticket {ticket_id}")
                return False
            # Calculate profit at the target price (entry + pips or entry - pips)
            price_target = position.price_open + value * point * PIP_MULTIPLIER if position.type == mt5.ORDER_TYPE_BUY else position.price_open - value * point * PIP_MULTIPLIER
            profit_at_target = mt5.order_calc_profit(position.type, symbol, position.volume, position.price_open, price_target)
            lock_in_target_usd = profit_at_target if profit_at_target is not None else 0
    elif method == "money":
        lock_in_target_usd = value

    if lock_in_target_usd <= 1e-9:
         logging.warning(f"Coop Manual Lock-in Failed: Calculated target profit is zero or negative ({lock_in_target_usd:.2f}) for ticket {ticket_id}.")
         return False

    # Check if current profit meets or exceeds the target
    if current_profit_usd >= lock_in_target_usd:
         logging.info(f"Coop Manual Lock-in: Condition met for {symbol} ticket {ticket_id}. Profit {current_profit_usd:.2f} >= Target {lock_in_target_usd:.2f} ({method})")
         # Calculate SL to lock in the target USD amount
         # Use a small buffer (e.g., 95%) to ensure SL is beyond entry price and valid
         lock_sl_price = calculate_lock_in_sl(position, lock_in_target_usd * 0.95)

         if lock_sl_price is not None and abs(lock_sl_price) > 1e-9: # Ensure calculated SL is valid and not zero
              with mt5_sync_lock:
                   request = {
                      "action": mt5.TRADE_ACTION_SLTP,
                      "position": position.ticket,
                      "symbol": symbol,
                      "sl": lock_sl_price,
                      "tp": position.tp # Keep original TP
                   }
                   result = mt5.order_send(request)

              if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                   # Update cooperative state - Manually trigger "achieved" status
                   cooperative_states[symbol] = cooperative_states.get(symbol, {}) # Ensure symbol exists
                   cooperative_states[symbol].update({
                        "long_medium_ticket": ticket_id, # Assume this ticket is the parent LM trade
                        "direction": get_trade_direction(position.type),
                        "lock_in_status": "achieved",
                        "locked_in_profit_usd": lock_in_target_usd,
                        "scalping_status": "active", # Enable scalping manually
                        "current_scalping_pnl_usd": 0, # Reset cumulative scalp PnL for this new lock-in phase
                        "open_scalp_tickets": [],
                        # parent_tf, bbm_tp, signal_id might not be known for manual trades, handle defaults or N/A
                        "parent_tf": "Manual",
                        "bbm_tp": None,
                        "signal_id": None,
                   })
                   open_trades[ticket_id] = open_trades.get(ticket_id, {}) # Ensure ticket exists
                   open_trades[ticket_id].update({"symbol": symbol, "strategy": "long_medium"}) # Ensure minimal tracking data if missing
                   logging.info(f"Coop Manual Lock-in: Locked profit for {symbol} ticket {ticket_id}. New SL: {lock_sl_price}. Status -> achieved, Scalping -> active.")
                   # No need to notify telegram again here, the calling function will report success
                   return True
              else:
                   err_comment = getattr(result, 'comment', 'Send Failed') if result else 'Send Failed'
                   logging.error(f"Coop Manual Lock-in: Failed to apply lock-in SL for {symbol} ticket {ticket_id}. Reason: {err_comment}")
                   return False
         else:
              logging.warning(f"Coop Manual Lock-in: Could not calculate valid lock-in SL for {symbol} ticket {ticket_id}.")
              return False
    else:
         logging.info(f"Coop Manual Lock-in: Condition NOT met for {symbol} ticket {ticket_id}. Profit {current_profit_usd:.2f} < Target {lock_in_target_usd:.2f}")
         await safe_reply(update, context, f"ℹ️ Ticket {ticket_id} ({symbol}) does not yet meet the lock-in criteria (Need {lock_in_target_usd:.2f} USD/pips, current profit {current_profit_usd:.2f} USD).")
         return False



































# Re-define states based on new numbers
# 0-13: Scanner
# 14-43: Trade Manager (TM_START=14)
# 44-45: Limit Order (LIMIT_ORDER_ENTRY=44)
LOCK_IN_TICKET = 46
LOCK_IN_MANUAL_PIPS = 47
TRAIL_STOP_METHOD = 48
TRAIL_STOP_VALUE = 49

# Update states dict for lock_in_conv_handler
lock_in_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("lock_in", cmd_lock_in)],
    states={
        LOCK_IN_TICKET: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_lock_in_ticket)],
        LOCK_IN_MANUAL_PIPS: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_lock_in_pips_value)],
    },
    fallbacks=[CommandHandler("cancel", cfg_cancel)],
     conversation_timeout=300
)


# Trailing Stop (Basic Implementation - Needs Background Task)
# Re-implementing with updated state values
async def cmd_trail_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Initiates trailing stop configuration. (Note: Actual trailing needs a monitor)"""
    # Clear any previous trailing settings
    trailing_settings.pop('global', None)
    # Could add per-symbol trailing later

    await safe_reply(update, context, "🚧 Basic Trailing Stop Setup 🚧\n"
                                     "Note: This configures a *global* trailing stop applied by a monitor task to *any* profitable trade.\n"
                                     "Choose trailing method: 'pips' or 'money' (USD):\nType /cancel to abort.")
    return TRAIL_STOP_METHOD

async def process_trail_stop_method(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes trailing stop method input."""
    if not update.message: return TRAIL_STOP_METHOD
    user_input = update.message.text.strip().lower()
    if user_input == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    method = user_input
    if method not in ["pips", "money"]: # Candle trailing not implemented yet
        await safe_reply(update, context, "❌ Invalid method. Choose 'pips' or 'money':\nType /cancel")
        return TRAIL_STOP_METHOD
    context.user_data["trail_stop_method"] = method
    prompt = {"pips": "distance in pips", "money": "distance in account currency (USD)"}
    await safe_reply(update, context, f"📌 Enter trailing {prompt[method]} (must be positive):\nType /cancel")
    return TRAIL_STOP_VALUE

async def process_trail_stop_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Processes trailing stop value and sets it globally."""
    if not update.message or not context.user_data: return TRAIL_STOP_VALUE
    user_input = update.message.text.strip()
    if user_input.lower() == 'cancel': await cfg_cancel(update, context); return ConversationHandler.END

    try:
        value = float(user_input)
        if value <= 0:
             await safe_reply(update, context, "⛔️ Trailing value must be positive. Enter again:\nType /cancel")
             return TRAIL_STOP_VALUE

        method = context.user_data["trail_stop_method"]
        # Store globally - the background monitor would read this
        trailing_settings['global'] = {"method": method, "value": value, "active": True}
        await safe_reply(update, context, f"✔️ Global trailing stop parameters set: Trail by {value} {method.upper()}.\n"
                                         "The background monitor will now attempt to trail profitable trades based on these settings.")
        context.user_data.clear()
        # Consider saving this to config if persistence is desired (requires adding to config structure)
        # save_config(config)
        return ConversationHandler.END
    except ValueError:
        await safe_reply(update, context, "⛔️ Enter a valid positive number:\nType /cancel")
        return TRAIL_STOP_VALUE
    except Exception as e:
        logging.error(f"Error setting trailing stop value: {e}", exc_info=True)
        await safe_reply(update, context, "An unexpected error occurred.")
        context.user_data.clear()
        return ConversationHandler.END


trail_stop_conv_handler = ConversationHandler(
    entry_points=[CommandHandler("trail_stop", cmd_trail_stop)],
    states={
        TRAIL_STOP_METHOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_trail_stop_method)],
        TRAIL_STOP_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_trail_stop_value)]
    },
    fallbacks=[CommandHandler("cancel", cfg_cancel)],
    conversation_timeout=300
)

async def cmd_stop_trail_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Stops the global trailing stop monitor."""
    if trailing_settings.get("global", {}).get("active") is not True:
        await safe_reply(update, context, "ℹ️ Global trailing stop is not currently active.")
        return

    # Deactivate trailing stop globally
    if "global" in trailing_settings:
         trailing_settings["global"]["active"] = False

    await safe_reply(update, context, "⏹ Global trailing stop has been stopped.")
    logging.info("Global trailing stop stopped by user.")
    # Consider saving config if trailing settings are added there

# Basic Trailing Stop Monitor
async def trailing_stop_monitor(context: ContextTypes.DEFAULT_TYPE) -> None:
     """ Periodically checks profitable trades and trails SL if global setting is active. """
     global_settings = trailing_settings.get("global")
     if not global_settings or not global_settings.get("active"):
         return # Trailing not enabled globally

     method = global_settings["method"]
     value = global_settings["value"]
     modified_count = 0
     error_count = 0
     update_messages = [] # Collect messages to send

     try:
         with mt5_sync_lock:
             positions = mt5.positions_get()
             if not positions: return

             for pos in positions:
                 # Only trail trades opened by bot or manual trades detected (based on magic number check or internal tracking?)
                 # Let's trail ALL trades in profit with Magic DEFAULT or SCALPING, or those added by manual_trade_detector to open_trades
                 # This ensures manual trades the bot becomes aware of also get trailed if desired.
                 # Check if the position is in profit
                 if pos.profit <= 1e-9: continue # Skip trades not in profit

                 symbol_info = mt5.symbol_info(pos.symbol)
                 tick = mt5.symbol_info_tick(pos.symbol)
                 if not tick or not symbol_info or symbol_info.point <= 1e-9:
                     logging.warning(f"Cannot get valid info/tick for {pos.symbol} (ticket {pos.ticket}) during trailing stop.")
                     error_count += 1
                     continue

                 point = symbol_info.point
                 digits = symbol_info.digits
                 current_sl = pos.sl
                 potential_new_sl = 0.0
                 should_trail = False
                 trail_distance_points = value * point # For pips method

                 if method == "pips":
                     if pos.type == mt5.ORDER_TYPE_BUY:
                         # Current price is Bid for BUY profit/SL calculation
                         current_price = tick.bid
                         # Calculate new SL based on trailing distance from current price
                         potential_new_sl = round(current_price - trail_distance_points, digits)
                         # Check if new SL is better than current SL and not too close to market
                         min_distance_points = symbol_info.trade_stops_level * point
                         if potential_new_sl > 0.0 and (current_sl == 0.0 or potential_new_sl > current_sl):
                             if current_price - potential_new_sl >= min_distance_points:
                                 should_trail = True
                             else:
                                 logging.debug(f"Trailing stop for {pos.ticket}: New SL {potential_new_sl:.{digits}f} is too close to Bid {current_price:.{digits}f}. Min distance: {min_distance_points/point:.1f} pips.")


                     elif pos.type == mt5.ORDER_TYPE_SELL:
                         # Current price is Ask for SELL profit/SL calculation
                         current_price = tick.ask
                         # Calculate new SL based on trailing distance from current price
                         potential_new_sl = round(current_price + trail_distance_points, digits)
                         # Check if new SL is better than current SL and not too close to market
                         min_distance_points = symbol_info.trade_stops_level * point
                         if potential_new_sl > 0.0 and (current_sl == 0.0 or potential_new_sl < current_sl):
                             if potential_new_sl - current_price >= min_distance_points:
                                 should_trail = True
                             else:
                                 logging.debug(f"Trailing stop for {pos.ticket}: New SL {potential_new_sl:.{digits}f} is too close to Ask {current_price:.{digits}f}. Min distance: {min_distance_points/point:.1f} pips.")

                 elif method == "money":
                     # Calculate SL price that trails behind current profit by the value (USD)
                     # This is different from locking in a fixed amount. It means SL moves up $1 for every $1 the trade moves up.
                     # If current profit is P, new profit if hit SL is P - value. Calculate SL for that.
                     current_profit_usd = pos.profit
                     # Only trail if profit exceeds the trail amount.
                     if current_profit_usd > value:
                         target_profit_usd = current_profit_usd - value # Target profit if SL hit
                         # Calculate the SL price that would result in target_profit_usd from OPEN price
                         # This calculation is complex and might not map directly to simple trailing logic.
                         # A true "money trail" often means trailing based on equity *changes*.
                         # Let's stick to a simpler interpretation: SL is set such that the *current* profit is protected minus the trailing value.
                         # This is closer to calculate_lock_in_sl logic, but based on current profit, not a fixed target.
                         # Alternative: Calculate distance in pips/points equivalent to the $value difference and trail by that.
                         # Let's use the "distance in money" interpretation for now, trailing the *peak* profit. (Requires storing peak profit - complex)
                         # Simpler approach for monitor: Trail by the dollar amount from *current* profit.
                         # The target profit if SL is hit should be related to current profit.
                         # e.g., if profit is $50, trail by $10. New SL ensures profit is at least $40.
                         # If profit moves to $60, new SL ensures profit is at least $50.
                         # This requires recalculating the *target profit level* based on the *current price*.

                         # Recalculate target profit relative to *current price* trailing
                         # Need to find the price X such that profit at X relative to entry = (Profit at current price) - value
                         # (CurrentPrice - Entry) * USD_per_point = CurrentProfit
                         # (SL_Price - Entry) * USD_per_point = TargetProfit
                         # TargetProfit = CurrentProfit - Value
                         # SL_Price = Entry + (TargetProfit / USD_per_point)

                         # Calculate USD per point
                         profit_at_entry_plus_point = mt5.order_calc_profit(pos.type, symbol, pos.volume, pos.price_open, pos.price_open + point)
                         profit_at_entry = mt5.order_calc_profit(pos.type, symbol, pos.volume, pos.price_open, pos.price_open)
                         usd_per_point = abs(profit_at_entry_plus_point - profit_at_entry) if profit_at_entry_plus_point is not None else 0

                         if usd_per_point <= 1e-9:
                              logging.warning(f"Cannot calculate USD per point for {pos.symbol} ticket {pos.ticket} during money trailing.")
                              error_count += 1
                              continue

                         # Calculate target profit if SL is hit
                         target_profit_usd_level = current_profit_usd - value

                         # Calculate required SL price based on the *entry price* and the *target profit level*
                         # This formula assumes a linear profit calculation with price change
                         # It's SL_Price = Entry + (TargetProfit / (USD_per_point * Direction_Sign))
                         direction_sign = 1 if pos.type == mt5.ORDER_TYPE_BUY else -1
                         potential_new_sl = round(pos.price_open + (target_profit_usd_level / usd_per_point) * direction_sign, digits)

                         # Check if new SL is better than current SL and not too close to market
                         min_distance_points = symbol_info.trade_stops_level * point
                         current_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask # Use relevant current price for distance check

                         if potential_new_sl > 0.0 and (current_sl == 0.0 or (pos.type == mt5.ORDER_TYPE_BUY and potential_new_sl > current_sl) or (pos.type == mt5.ORDER_TYPE_SELL and potential_new_sl < current_sl)):
                             if pos.type == mt5.ORDER_TYPE_BUY and current_price - potential_new_sl >= min_distance_points:
                                 should_trail = True
                             elif pos.type == mt5.ORDER_TYPE_SELL and potential_new_sl - current_price >= min_distance_points:
                                 should_trail = True
                             else:
                                 logging.debug(f"Trailing stop for {pos.ticket}: New SL {potential_new_sl:.{digits}f} is too close to current price {current_price:.{digits}f}. Min distance: {min_distance_points/point:.1f} pips.")
                         else:
                             logging.debug(f"Trailing stop for {pos.ticket}: New SL {potential_new_sl:.{digits}f} not better than current SL {current_sl:.{digits}f} or not positive.")

                     else:
                         logging.debug(f"Trailing stop for {pos.ticket} ({symbol}): Current profit {pos.profit:.2f} < trailing value {value:.2f}. Not trailing yet.")


                 # --- Update SL if needed ---
                 if should_trail and potential_new_sl > 1e-9: # Ensure positive SL
                     logging.info(f"Trailing SL for {pos.symbol} ticket {pos.ticket}. Method: {method.upper()}, Value: {value}. New SL: {potential_new_sl:.{digits}f}")
                     request = {
                         "action": mt5.TRADE_ACTION_SLTP,
                         "position": pos.ticket,
                         "symbol": pos.symbol,
                         "sl": potential_new_sl,
                         "tp": pos.tp # Keep existing TP
                     }
                     result = mt5.order_send(request)
                     if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                         modified_count += 1
                         update_messages.append(f"➡️ Trailing SL updated for {pos.symbol} (Ticket {pos.ticket}) to {potential_new_sl:.{digits}f}")
                     else:
                          error_count += 1
                          err_comment = getattr(result, 'comment', 'Send Failed') if result else 'Send Failed'
                          logging.error(f"Failed trailing SL update for ticket {pos.ticket}. Reason: {err_comment}")
                          update_messages.append(f"❌ Failed to update SL for {pos.symbol} (Ticket {pos.ticket}). Reason: {err_comment}")
                     time.sleep(0.1) # Small delay

         if update_messages:
              # Concatenate messages to reduce Telegram calls
              full_message = "📢 Trailing Stop Updates:\n\n" + "\n".join(update_messages)
              await safe_send_message(context, text=full_message)

     except Exception as e:
          logging.error(f"Error in trailing_stop_monitor: {e}", exc_info=True)
          # Avoid sending repetitive error messages unless configured


# =============================================================================
# Standard Trade Management Commands
# =============================================================================

async def _modify_sltp(update: Update, context: ContextTypes.DEFAULT_TYPE, field_to_modify: str):
    """Helper to modify SL or TP for positions by price or pips."""
    if not update.message or not context.args or len(context.args) < 2:
        await safe_reply(update, context, f"❌ Usage: /{field_to_modify} <price> [@SYMBOL] or /{field_to_modify} <+/-pips> [@SYMBOL]\n"
                                         f"Examples:\n<code>/{field_to_modify} 1.23456 @EURUSD</code> (Set price)\n"
                                         f"<code>/{field_to_modify} +50 @GBPUSD</code> (Move 50 pips in profit direction)\n"
                                         f"<code>/{field_to_modify} -20 @AUDCAD</code> (Move 20 pips against profit direction)\n"
                                         f"<code>/{field_to_modify} 0 @NZDUSD</code> (Remove SL/TP)\n"
                                         f"(Symbol is optional, defaults to all matching bot magic)")
        return

    value_str = context.args[0].strip()
    symbol_arg = next((arg[1:].upper() for arg in context.args[1:] if arg.startswith("@")), None)

    modified_count = 0
    error_count = 0
    processed_tickets_batch = set() # Track tickets modified in this run

    try:
        # Determine if input is price or pips adjustment
        is_pips_adjustment = value_str.startswith('+') or value_str.startswith('-')
        value = float(value_str)

        with mt5_sync_lock:
            if not runtime_state["mt5_initialized"] or not mt5.terminal_info():
                await safe_reply(update, context, "❌ MT5 not connected. Cannot modify trades.")
                return

            positions = mt5.positions_get(symbol=symbol_arg) if symbol_arg else mt5.positions_get()

            # Filter positions: Only modify trades managed by the bot (MAGIC_DEFAULT or MAGIC_SCALPING)
            # Or allow modifying all trades if no symbol specified?
            # Let's stick to bot-managed trades by default for safety, unless a specific ticket is targeted (not implemented here)
            # Or, modify *all* positions if a symbol or no argument is given, but only if they don't have bot magic? No, modify all.
            # The risk is modifying trades from *other* EAs.
            # Safer: Only modify trades that are *tracked* in `open_trades`.
            positions_to_modify = [pos for pos in positions if pos and pos.ticket in open_trades] # Filter by tracked trades

            if symbol_arg:
                 positions_to_modify = [pos for pos in positions_to_modify if pos.symbol == symbol_arg]


            if not positions_to_modify:
                await safe_reply(update, context, f"ℹ️ No open trades tracked by the bot found{f' for {symbol_arg}' if symbol_arg else ''}.")
                # If no *tracked* trades match, perhaps list all open positions?
                all_positions = mt5.positions_get()
                if all_positions and len(all_positions) > 0:
                     await safe_reply(update, context, f"Found {len(all_positions)} total open positions. Use ticket ID with /close, /stop_loss, /new_take_profit to target specific manual trades not tracked by the bot.")
                return


            for pos in positions_to_modify:
                # Avoid processing the same ticket twice if the list somehow contains duplicates
                if pos.ticket in processed_tickets_batch: continue
                processed_tickets_batch.add(pos.ticket)

                symbol = pos.symbol
                symbol_info = mt5.symbol_info(symbol)
                tick = mt5.symbol_info_tick(symbol)
                if not tick or not symbol_info or symbol_info.point <= 1e-9:
                    logging.warning(f"Cannot get valid info/tick for {symbol} (ticket {pos.ticket}) during SL/TP modification.")
                    error_count += 1
                    continue

                point = symbol_info.point
                digits = symbol_info.digits
                current_sl = pos.sl
                current_tp = pos.tp

                request_sl = current_sl
                request_tp = current_tp
                new_value_price = 0.0 # The calculated target price

                try: # Wrap price/pips calculation and request sending for each trade
                    if is_pips_adjustment:
                        # Value is pips adjustment
                        pips_adjust = value # Can be positive or negative

                        # Calculate new price based on pips movement from CURRENT price
                        # This is effectively manually "trailing" or "pushing" the stop/take
                        current_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask # Use appropriate price
                        pips_move = pips_adjust * point * PIP_MULTIPLIER

                        if field_to_modify == "stop_loss":
                            # Positive pips: move SL towards profit (trail)
                            # Negative pips: move SL away from profit (give room)
                            direction_sign = 1 if pos.type == mt5.ORDER_TYPE_BUY else -1
                            # For SL, positive pips make SL "better", negative make it "worse"
                            # new_value_price = current_price - (pips_adjust * point * direction_sign) # Wrong way

                            if pos.type == mt5.ORDER_TYPE_BUY:
                                # BUY: SL is below current price. +Pips moves SL up, -Pips moves SL down.
                                new_value_price = current_price - (pips_adjust * point) # +10 pips: SL is 10*point below current BID
                            elif pos.type == mt5.ORDER_TYPE_SELL:
                                # SELL: SL is above current price. +Pips moves SL down, -Pips moves SL up.
                                new_value_price = current_price + (pips_adjust * point) # +10 pips: SL is 10*point above current ASK

                            # Validate new SL price
                            min_distance_points = symbol_info.trade_stops_level * point
                            if abs(current_price - new_value_price) < min_distance_points:
                                logging.warning(f"Calculated new SL {new_value_price:.{digits}f} for {pos.ticket} too close to current price. Skipping.")
                                await safe_send_message(context, text=f"⚠️ Cannot set SL for ticket {pos.ticket}. Calculated price too close to market.")
                                continue # Skip this trade

                            request_sl = round(new_value_price, digits)
                            logging.info(f"Calculated new SL for {pos.ticket} ({symbol}): {request_sl:.{digits}f} (+/-{pips_adjust} pips from current price)")

                        elif field_to_modify == "take_profit":
                            # Positive pips: move TP towards profit (closer)
                            # Negative pips: move TP away from profit (further)
                            # Direction sign applies to price change relative to TP
                            # new_value_price = current_price + (pips_adjust * point * direction_sign) # Wrong way

                            if pos.type == mt5.ORDER_TYPE_BUY:
                                # BUY: TP is above current price. +Pips moves TP up, -Pips moves TP down.
                                new_value_price = current_price + (pips_adjust * point) # +10 pips: TP is 10*point above current BID
                            elif pos.type == mt5.ORDER_TYPE_SELL:
                                # SELL: TP is below current price. +Pips moves TP down, -Pips moves TP up.
                                new_value_price = current_price - (pips_adjust * point) # +10 pips: TP is 10*point below current ASK

                             # Validate new TP price
                            min_distance_points = symbol_info.trade_stops_level * point
                            if abs(current_price - new_value_price) < min_distance_points:
                                logging.warning(f"Calculated new TP {new_value_price:.{digits}f} for {pos.ticket} too close to current price. Skipping.")
                                await safe_send_message(context, text=f"⚠️ Cannot set TP for ticket {pos.ticket}. Calculated price too close to market.")
                                continue # Skip this trade

                            request_tp = round(new_value_price, digits)
                            logging.info(f"Calculated new TP for {pos.ticket} ({symbol}): {request_tp:.{digits}f} (+/-{pips_adjust} pips from current price)")

                    else:
                        # Value is a fixed price
                        new_value_price = value
                        # Check if price is valid (positive, and not ridiculously far)
                        if new_value_price < 0: # Already checked at start, but double check
                            logging.warning(f"Attempted to set {field_to_modify} to negative price {new_value_price} for {pos.ticket}. Skipping.")
                            await safe_send_message(context, text=f"⚠️ Cannot set {field_to_modify} for ticket {pos.ticket} to negative price.")
                            continue

                        if field_to_modify == "stop_loss":
                             request_sl = round(new_value_price, digits)
                             # Validate new SL price (not too close to current price, must be valid relative to current price)
                             min_distance_points = symbol_info.trade_stops_level * point
                             current_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
                             if (pos.type == mt5.ORDER_TYPE_BUY and current_price - request_sl < min_distance_points) or \
                                (pos.type == mt5.ORDER_TYPE_SELL and request_sl - current_price < min_distance_points):
                                  if request_sl != 0.0: # Allow setting to 0.0 to remove SL
                                       logging.warning(f"Attempted to set SL {request_sl:.{digits}f} for {pos.ticket} too close to current price {current_price:.{digits}f}. Skipping.")
                                       await safe_send_message(context, text=f"⚠️ Cannot set SL for ticket {pos.ticket} to {request_sl:.{digits}f}. It's too close to market price.")
                                       continue
                             logging.info(f"Setting SL for {pos.ticket} ({symbol}) to fixed price: {request_sl:.{digits}f}")


                        elif field_to_modify == "take_profit":
                             request_tp = round(new_value_price, digits)
                             # Validate new TP price (not too close to current price, must be valid relative to current price)
                             min_distance_points = symbol_info.trade_stops_level * point
                             current_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
                             if (pos.type == mt5.ORDER_TYPE_BUY and request_tp - current_price < min_distance_points) or \
                                (pos.type == mt5.ORDER_TYPE_SELL and current_price - request_tp < min_distance_points):
                                   if request_tp != 0.0: # Allow setting to 0.0 to remove TP
                                        logging.warning(f"Attempted to set TP {request_tp:.{digits}f} for {pos.ticket} too close to current price {current_price:.{digits}f}. Skipping.")
                                        await safe_send_message(context, text=f"⚠️ Cannot set TP for ticket {pos.ticket} to {request_tp:.{digits}f}. It's too close to market price.")
                                        continue

                             logging.info(f"Setting TP for {pos.ticket} ({symbol}) to fixed price: {request_tp:.{digits}f}")

                    # Send modification request
                    request = {
                        "action": mt5.TRADE_ACTION_SLTP,
                        "position": pos.ticket,
                        "symbol": symbol,
                        "sl": request_sl if request_sl is not None else pos.sl, # Use the calculated/set SL or original if not modifying
                        "tp": request_tp if request_tp is not None else pos.tp, # Use the calculated/set TP or original if not modifying
                    }

                    result = mt5.order_send(request)

                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        modified_count += 1
                        logging.info(f"Successfully updated {field_to_modify} for ticket {pos.ticket}")
                    else:
                        error_count += 1
                        err_comment = getattr(result, 'comment', 'Send Failed') if result else 'Send Failed'
                        logging.error(f"Failed to set {field_to_modify} for ticket {pos.ticket}. Reason: {err_comment}")
                        await safe_send_message(context, text=f"❌ Failed to update {field_to_modify} for ticket {pos.ticket}. Reason: {err_comment}") # Notify user on failure
                except Exception as e:
                     logging.error(f"Error processing trade {pos.ticket} during SL/TP modification: {e}", exc_info=True)
                     error_count += 1
                     await safe_send_message(context, text=f"❌ Error processing trade {pos.ticket} during modification.")

                time.sleep(0.05) # Small delay

    except ValueError:
        await safe_reply(update, context, "⛔️ Invalid value format. Please enter a number (e.g., 1.23456) or pips adjustment (e.g., +50).")
        return # End command processing

    except Exception as e:
         logging.error(f"Unhandled exception in _modify_sltp: {e}", exc_info=True)
         await safe_reply(update, context, "An unexpected error occurred during modification.")
         return # End command processing


    # --- Report Outcome ---
    target = f" for {symbol_arg}" if symbol_arg else " for tracked positions"
    value_desc = f"to price {value_str}" if not is_pips_adjustment else f"by {value_str} pips"
    if value == 0 and not is_pips_adjustment: value_desc = "to None (removed)"

    msg = f"✅ Set {field_to_modify.replace('_', ' ')} {value_desc}{target}.\n"
    msg += f"Successfully modified {modified_count} positions."
    if error_count > 0:
        msg += f"\n⚠️ Failed for {error_count} positions (check logs & messages above)."
    await safe_reply(update, context, msg)


async def cmd_stop_loss(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adjusts stop loss for open positions."""
    await _modify_sltp(update, context, "stop_loss")

async def cmd_new_take_profit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adjusts take profit for open positions."""
    await _modify_sltp(update, context, "take_profit")


async def _close_positions(update: Update, context: ContextTypes.DEFAULT_TYPE, symbol_filter: Optional[str] = None, ticket_filter: Optional[int] = None):
    """Helper function to close positions."""
    closed_count = 0
    error_count = 0
    target_desc = ""

    with mt5_sync_lock:
        if not runtime_state["mt5_initialized"] or not mt5.terminal_info():
            await safe_reply(update, context, "❌ MT5 not connected. Cannot close trades.")
            return

        if ticket_filter:
             positions_to_close = mt5.positions_get(ticket=ticket_filter)
             target_desc = f" ticket {ticket_filter}"
        elif symbol_filter:
             positions_to_close = mt5.positions_get(symbol=symbol_filter)
             target_desc = f" for symbol {symbol_filter}"
        else:
             positions_to_close = mt5.positions_get()
             target_desc = " all open positions"

        if not positions_to_close:
            await safe_reply(update, context, f"ℹ️ No open positions found matching the criteria ({target_desc}).")
            return

        logging.info(f"Attempting to close {len(positions_to_close)} positions: {target_desc}")

        for pos in positions_to_close:
            symbol = pos.symbol
            ticket = pos.ticket
            volume = pos.volume

            # Determine close order type (opposite of position type)
            order_type_close = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            tick = mt5.symbol_info_tick(symbol)
            symbol_info = mt5.symbol_info(symbol)

            if not tick or not symbol_info:
                logging.error(f"Cannot get info/tick to close position {ticket} for {symbol}. Skipping.")
                error_count += 1
                await safe_send_message(context, text=f"❌ Failed to close position {ticket}: Could not get market data.")
                continue

            # Get current price for closing
            price = tick.bid if order_type_close == mt5.ORDER_TYPE_SELL else tick.ask

            # Determine filling mode (IOC is typical for closure)
            filling_mode = mt5.ORDER_FILLING_IOC
            if symbol_info.filling_mode & mt5.ORDER_FILLING_FOK: # Prefer FOK if available
                 filling_mode = mt5.ORDER_FILLING_FOK
            # Ignore RETURN filling mode for manual closures


            request = {
                "action": mt5.TRADE_ACTION_DEAL, # Use DEAl action for closure
                "position": ticket, # Specify the position to close
                "symbol": symbol,
                "volume": volume, # Close the full volume
                "type": order_type_close,
                "price": price, # Use current market price
                "deviation": 20, # Allow some deviation for market close
                "magic": pos.magic, # Include the original magic number
                "comment": f"CloseManual_{symbol_filter or ticket_filter or 'All'}"[:31], # Truncate comment
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": filling_mode,
            }

            logging.info(f"Sending close request for ticket {ticket} ({symbol}): {request}")
            result = mt5.order_send(request)

            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                closed_count += 1
                logging.info(f"Successfully sent close order for ticket {ticket}")
                # Removal from open_trades tracking and DB update happens in trade_close_monitor
            else:
                error_count += 1
                err_comment = getattr(result, 'comment', 'Send Failed') if result else 'Send Failed'
                logging.error(f"Failed to close position ticket {ticket}. Reason: {err_comment}")
                await safe_send_message(context, text=f"❌ Failed to close position {ticket}. Reason: {err_comment}")

            time.sleep(0.1) # Delay between close orders

    # --- Report Outcome ---
    msg = f"✅ Close command executed for{target_desc}.\n"
    msg += f"Successfully sent close requests for {closed_count} positions."
    if error_count > 0:
        msg += f"\n⚠️ Failed for {error_count} positions (check logs & messages above)."
    await safe_reply(update, context, msg)

    # No need to clear internal states/caches immediately here, monitors handle that based on MT5 reality

async def cmd_close(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Closes a specific trade by symbol or ticket ID."""
    if not context.args or len(context.args) != 1:
        await safe_reply(update, context, "❌ Usage: /close <@SYMBOL|TICKET_ID>\nExamples:\n<code>/close @EURUSD</code>\n<code>/close 123456789</code>")
        return

    arg = context.args[0].strip()
    symbol_filter = None
    ticket_filter = None

    if arg.startswith("@"):
        symbol_filter = arg[1:].upper()
    else:
        try:
            ticket_filter = int(arg)
        except ValueError:
            await safe_reply(update, context, "❌ Invalid argument. Use @SYMBOL or TICKET_ID.")
            return

    if symbol_filter:
        await _close_positions(update, context, symbol_filter=symbol_filter)
    elif ticket_filter:
         await _close_positions(update, context, ticket_filter=ticket_filter)
         # If closing by ticket, specifically remove from pending confirmations if it was waiting
         with CONFIRMATION_PENDING_LOCK:
              keys_to_remove = [key for key, data in confirmation_pending_trades.items() if data.get("parent_trade_data", {}).get("ticket") == ticket_filter]
              for key in keys_to_remove:
                   logging.info(f"Removing confirmation pending entry {key} due to manual close of ticket {ticket_filter}")
                   confirmation_pending_trades.pop(key, None)
         maybe_stop_pip_monitor()


async def cmd_close_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Closes all open trades, optionally filtered by symbol."""
    symbol_filter = None
    if context.args:
        symbol_arg = context.args[0].strip()
        if symbol_arg.startswith("@"):
            symbol_filter = symbol_arg[1:].upper()
        else:
            await safe_reply(update, context, "❌ Usage: /close_all [@SYMBOL]\nExample:\n<code>/close_all @EURUSD</code> (Close all for EURUSD)\n<code>/close_all</code> (Close all positions)")
            return

    await _close_positions(update, context, symbol_filter=symbol_filter)

    # Clear pending confirmations if related to the symbol filter or all
    with CONFIRMATION_PENDING_LOCK:
         keys_to_remove = []
         for key, data in confirmation_pending_trades.items():
              if symbol_filter is None or data.get("symbol") == symbol_filter:
                   keys_to_remove.append(key)
         for key in keys_to_remove:
              logging.info(f"Removing confirmation pending entry {key} due to /close_all{f' @{symbol_filter}' if symbol_filter else ''}")
              confirmation_pending_trades.pop(key, None)
    maybe_stop_pip_monitor()

    # Clear all cooperative states if all trades were closed globally
    if symbol_filter is None:
         cooperative_states.clear()
         logging.info("All cooperative states cleared due to /close_all.")



async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Displays current bot status."""
    await sync_open_trades()
    # MT5 account info
    pending_orders = mt5.orders_get() or []
    pending_orders_count = len(pending_orders)
    account_info = mt5.account_info()
    if account_info:
        balance = account_info.balance
        equity = account_info.equity
        margin_level = account_info.margin_level
        profit = account_info.profit
        account_currency = account_info.currency
        mt5_status = 'Connected ✅' if mt5.initialize() else 'Disconnected ❌'
    else:
        balance = equity = profit = margin_level = 0.0
        account_currency = ""
        mt5_status = 'Connected ✅' if mt5.initialize() else 'Disconnected ❌'

    # Runtime statuses
    scanner_status = "Running ✅" if runtime_state.get('scanner_running') else "Stopped ⏹"
    tm_status = "Running ✅" if runtime_state.get('trade_manager_running') else "Stopped ⏹"

    # Trades tracking
    open_trades_count = len(open_trades)
    tracked_trades_count = open_trades_count
    tracked_manual = sum(1 for t in open_trades.values() if t.get("opened_by") == "manual_detected")
    tracked_bot = tracked_trades_count - tracked_manual

    # Compose message
    
    status_lines = [
        f"<b>Bot Status:</b>",
        f"MT5: {mt5_status}",
        f"Scanner: {scanner_status}",
        f"Trade Manager: {tm_status}",
        f"Open Positions: {open_trades_count}",
        f"Pending Orders: {pending_orders_count}",
        f"Balance: {balance:.2f} {account_currency}",
        f"Equity: {equity:.2f} {account_currency}",
        f"Margin Level: {margin_level:.2f}%",
        f"Profit: {profit:.2f} {account_currency}",
        f"Tracked Trades: {tracked_trades_count} (Bot: {tracked_bot}, Manual: {tracked_manual})",
    ]
    status_text = "\n".join(status_lines)
    await safe_reply(update, context, status_text, parse_mode=ParseMode.HTML)

async def scanner_loop(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic scanner that generates simple Bollinger-based signals."""
    try:
        runtime_state["last_scanner_run"] = datetime.now(timezone.utc)
        if not is_market_open():
            if runtime_state.get("market_state") != BotState.IDLE:
                with STATE_LOCK:
                    runtime_state["market_state"] = BotState.IDLE
                await safe_send_message(context, "🔴 Market closed. Scanner sleeping.")
            await asyncio.sleep(60)
            run_health_check()
            return
        else:
            if runtime_state.get("market_state") != BotState.ACTIVE:
                with STATE_LOCK:
                    runtime_state["market_state"] = BotState.RECOVERING
                await safe_send_message(context, "🟡 Market reopened. Recovering...")
                run_health_check()
                with STATE_LOCK:
                    runtime_state["market_state"] = BotState.ACTIVE
                await safe_send_message(context, "🟢 Resuming scanner.")
        if not MT5_UTILS_INITIALIZED:
            return
        if not runtime_state.get("mt5_initialized") or not mt5.terminal_info():
            if not init_mt5():
                return
        max_trades = config["trade_manager"].get("max_open_trades")
        if max_trades:
            with mt5_sync_lock:
                current_trades = len(mt5.positions_get() or [])
            if runtime_state.get("scanner_auto_paused"):
                if current_trades < max_trades:
                    with STATE_LOCK:
                        runtime_state["scanner_running"] = True
                        runtime_state["scanner_auto_paused"] = False
                    await safe_send_message(context, "▶️ Scanner resumed: open trades below limit.")
                else:
                    return
            elif current_trades >= max_trades:
                with STATE_LOCK:
                    runtime_state["scanner_running"] = False
                    runtime_state["scanner_auto_paused"] = True
                await safe_send_message(context, "⏸ Scanner paused: maximum open trades reached.")
                return

        if not runtime_state.get("scanner_running"):
            return
        for symbol in config["scanner"]["symbols"]:
            for tf in config["scanner"]["timeframes"]:
                try:
                    df = await asyncio.wait_for(
                        asyncio.to_thread(
                            fetch_history_data,
                            symbol,
                            tf,
                            config["scanner"]["num_bars"],
                        ),
                        timeout=30,
                    )
                except asyncio.TimeoutError:
                    logging.warning(f"Timeout fetching data for {symbol} {tf}")
                    continue
                if df is None or len(df) < config["scanner"]["min_bars"]:
                    continue
                # Limit to one signal per bar
                last_bar = df.index[-1]
                prev_last = last_bar_times.setdefault(symbol, {}).get(tf)
                if prev_last == last_bar:
                    continue
                last_bar_times[symbol][tf] = last_bar
                await asyncio.sleep(0)

                bb = ta.bbands(
                    df['close'],
                    length=config["scanner"]["indicators"]["bollinger_period"],
                    std=config["scanner"]["indicators"]["bollinger_dev"],
                )
                upper = bb[
                    f'BBU_{config["scanner"]["indicators"]["bollinger_period"]}_{config["scanner"]["indicators"]["bollinger_dev"]}'
                ]
                lower = bb[
                    f'BBL_{config["scanner"]["indicators"]["bollinger_period"]}_{config["scanner"]["indicators"]["bollinger_dev"]}'
                ]

                rsi = ta.rsi(
                    df['close'],
                    length=config["scanner"]["indicators"]["rsi_period"],
                )
                stoch = ta.stoch(
                    df['high'],
                    df['low'],
                    df['close'],
                    k=config["scanner"]["indicators"]["stoch_k"],
                    d=config["scanner"]["indicators"]["stoch_d"],
                    smooth_k=config["scanner"]["indicators"]["stoch_slowing"],
                )
                sk_col = (
                    f'STOCHk_{config["scanner"]["indicators"]["stoch_k"]}_'
                    f'{config["scanner"]["indicators"]["stoch_d"]}_'
                    f'{config["scanner"]["indicators"]["stoch_slowing"]}'
                )
                sd_col = (
                    f'STOCHd_{config["scanner"]["indicators"]["stoch_k"]}_'
                    f'{config["scanner"]["indicators"]["stoch_d"]}_'
                    f'{config["scanner"]["indicators"]["stoch_slowing"]}'
                )

                last_rsi = rsi.iloc[-1]
                last_sk = stoch[sk_col].iloc[-1]
                last_sd = stoch[sd_col].iloc[-1]

                last, prev = df['close'].iloc[-1], df['close'].iloc[-2]
                last_upper, last_lower = upper.iloc[-1], lower.iloc[-1]

                rsi_os = config["scanner"]["indicators"]["rsi_oversold"]
                rsi_ob = config["scanner"]["indicators"]["rsi_overbought"]
                stoch_os = config["scanner"]["indicators"]["stoch_oversold"]
                stoch_ob = config["scanner"]["indicators"]["stoch_overbought"]

                # Generate BUY signal when price crosses above lower band
                # and both RSI and Stochastic are in oversold territory
                if (
                    prev < last_lower
                    and last >= last_lower
                    and last_rsi <= rsi_os
                    and last_sk <= stoch_os
                    and last_sd <= stoch_os
                ):
                    decision, prob = await ai_decision("BUY", last_upper, last_lower, symbol, tf)
                    if decision:
                        await send_signal_to_telegram(
                            context,
                            None,
                            symbol,
                            tf,
                            "BUY",
                            "Bollingerband_Kachu_Strategy",
                            bb_upper=last_upper,
                            bb_lower=last_lower,
                            low=None,
                            high=None,
                            bbm=None,
                            signal_candle_close=last,
                            prob=prob,
                        )
                # Generate SELL signal when price crosses below upper band
                # and both RSI and Stochastic are in overbought territory
                elif (
                    prev > last_upper
                    and last <= last_upper
                    and last_rsi >= rsi_ob
                    and last_sk >= stoch_ob
                    and last_sd >= stoch_ob
                ):
                    decision, prob = await ai_decision("SELL", last_upper, last_lower, symbol, tf)
                    if decision:
                        await send_signal_to_telegram(
                            context,
                            None,
                            symbol,
                            tf,
                            "SELL",
                            "Bollingerband_Kachu_Strategy",
                            bb_upper=last_upper,
                            bb_lower=last_lower,
                            low=None,
                            high=None,
                            bbm=None,
                            signal_candle_close=last,
                            prob=prob,
                        )
    except Exception as e:
        logging.error(f"Error in scanner_loop: {e}", exc_info=True)

async def pip_confirmation_monitor(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Checks pending signals and fires trades when price moves far enough."""
    now = datetime.now(pytz.utc)
    to_fire: List[Tuple[str, Dict[str, Any]]] = []
    to_remove: List[str] = []
    with CONFIRMATION_PENDING_LOCK:
        for key, data in list(confirmation_pending_trades.items()):
            sid      = data["signal_id"]
            symbol   = data["symbol"]
            action   = data["action"]
            required = data["required_price"]
            start    = data["start_time"]
            timeout  = config["trade_manager"]["pip_confirmation_timeout_sec"]
            if (now - start).total_seconds() > timeout:
                await update_signal_status(sid, -1)
                await safe_send_message(
                    context,
                    f"⚠️ AUTO Signal Ignored ({symbol} {data['timeframe']} {action}): confirmation timed out.",
                )
                to_remove.append(key)
                continue
            with mt5_sync_lock:
                tick = mt5.symbol_info_tick(symbol)
            if not tick:
                continue
            price = tick.ask if action == "BUY" else tick.bid
            if (action == "BUY" and price >= required) or (
                action == "SELL" and price <= required
            ):
                to_fire.append((key, data))
        for key in to_remove:
            confirmation_pending_trades.pop(key, None)
    for key, data in to_fire:
        with CONFIRMATION_PENDING_LOCK:
            confirmation_pending_trades.pop(key, None)
        asyncio.create_task(
            safe_send_message(
                context,
                (
                    f"✅ AUTO Signal {data['signal_id']} ({data['symbol']} {data['timeframe']} {data['action']}): pip confirmation reached. Placing trade."
                    + (f"\nConfidence: {data['prob']*100:.2f}%" if data.get('prob') is not None else "")
                ),
            )
        )
        asyncio.create_task(
            place_auto_trade(
                context,
                data['signal_id'],
                data['symbol'],
                data['timeframe'],
                data['action'],
                data['strategy'],
                low      = data['parent_trade_data'].get('low'),
                high     = data['parent_trade_data'].get('high'),
                bbm      = data['parent_trade_data'].get('bbm'),
                bb_upper = data['parent_trade_data'].get('bb_upper'),
                bb_lower = data['parent_trade_data'].get('bb_lower'),
                prob     = data.get('prob'),
            )
        )
    maybe_stop_pip_monitor()

async def breakeven_monitor(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Move stop loss to breakeven when position profit exceeds target."""
    try:
        target = config["trade_manager"].get("breakeven_profit")
        if not target or target <= 0:
            return
        with mt5_sync_lock:
            positions = mt5.positions_get() or []
        if not positions:
            return
        for pos in positions:
            if pos.profit < target:
                continue
            symbol_info = mt5.symbol_info(pos.symbol)
            tick = mt5.symbol_info_tick(pos.symbol)
            if not symbol_info or not tick or symbol_info.point <= 1e-9:
                continue
            point = symbol_info.point
            digits = symbol_info.digits
            min_distance = symbol_info.trade_stops_level * point
            entry = pos.price_open
            current_sl = pos.sl
            current_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
            new_sl = None
            if pos.type == mt5.ORDER_TYPE_BUY:
                if (current_sl == 0.0 or current_sl < entry) and current_price - entry >= min_distance:
                    new_sl = round(entry, digits)
            elif pos.type == mt5.ORDER_TYPE_SELL:
                if (current_sl == 0.0 or current_sl > entry) and entry - current_price >= min_distance:
                    new_sl = round(entry, digits)
            if new_sl is not None:
                request = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "position": pos.ticket,
                    "symbol": pos.symbol,
                    "sl": new_sl,
                    "tp": pos.tp,
                }
                result = mt5.order_send(request)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    logging.info(f"Breakeven SL set for {pos.symbol} ticket {pos.ticket} at {new_sl:.{digits}f}")
                else:
                    err_comment = getattr(result, 'comment', 'Send Failed') if result else 'Send Failed'
                    logging.error(f"Failed breakeven SL for ticket {pos.ticket}: {err_comment}")
                time.sleep(0.1)
    except Exception as e:
        logging.error(f"Error in breakeven_monitor: {e}", exc_info=True)

async def trade_close_monitor(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Automatically close trades when profit target in money is reached."""
    try:
        with mt5_sync_lock:
            positions = mt5.positions_get() or []
            account_info = mt5.account_info()
        if not positions or not account_info:
            return

        equity_target = config["trade_manager"].get("equity_profit_target")
        if equity_target and account_info.profit >= equity_target:
            dummy = type('Dummy', (), {'effective_chat': type('C', (), {'id': config['telegram']['chat_id']})(), 'message': None, 'callback_query': None})()
            await _close_positions(dummy, context)
            logging.info('trade_close_monitor: equity profit target reached; issued /close_all')
            return

        for pos in positions:
            ticket = pos.ticket
            strategy = open_trades.get(ticket, {}).get("strategy", "long_medium")
            strat_cfg = config["trade_manager"]["strategy_settings"].get(strategy, {})
            tp_mode = strat_cfg.get("take_profit_mode")
            tp_value = strat_cfg.get("take_profit_value")

            if tp_mode == "money" and tp_value and pos.profit >= tp_value:
                dummy = type('Dummy', (), {'effective_chat': type('C', (), {'id': config['telegram']['chat_id']})(), 'message': None, 'callback_query': None})()
                await _close_positions(dummy, context)
                pnl_pips = calculate_pnl_pips(pos.price_open, pos.price_current, pos.type, pos.symbol)
                await update_trade_close(ticket, datetime.now(pytz.utc), pos.price_current, pos.profit, pnl_pips)
                open_trades.pop(ticket, None)
                logging.info(f'trade_close_monitor: take-profit reached for ticket {ticket}; issued /close_all')
                return
    except Exception as e:
        logging.error(f"Error in trade_close_monitor: {e}", exc_info=True)

async def trade_manager_loop(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic trade manager tasks: trailing stops, manual detection, position & close monitoring."""
    try:
        runtime_state["last_trade_manager_run"] = datetime.now(timezone.utc)
        if not is_market_open():
            if runtime_state.get("market_state") != BotState.IDLE:
                with STATE_LOCK:
                    runtime_state["market_state"] = BotState.IDLE
                await safe_send_message(context, "🔴 Market closed. Trade manager idle.")
            await asyncio.sleep(60)
            run_health_check()
            return
        else:
            if runtime_state.get("market_state") != BotState.ACTIVE:
                with STATE_LOCK:
                    runtime_state["market_state"] = BotState.RECOVERING
                await safe_send_message(context, "🟡 Market reopened. Recovering...")
                run_health_check()
                with STATE_LOCK:
                    runtime_state["market_state"] = BotState.ACTIVE
                await safe_send_message(context, "🟢 Trade manager resumed.")
        if not runtime_state.get("trade_manager_running"):
            return
        if not MT5_UTILS_INITIALIZED:
            return
        if not runtime_state.get("mt5_initialized") or not mt5.terminal_info():
            if not init_mt5():
                return
        if config["trade_manager"].get("use_news_filter"):
            with mt5_sync_lock:
                positions = mt5.positions_get() or []
            symbols_to_close = {p.symbol for p in positions if should_close_for_news(p.symbol)}
            for sym in {p.symbol for p in positions}:
                await update_news_state(context, sym)
            if symbols_to_close:
                dummy = type("Dummy", (), {"effective_chat": type("C", (), {"id": config["telegram"]["chat_id"]})(), "message": None, "callback_query": None})()
                for sym in symbols_to_close:
                    await _close_positions(dummy, context, symbol_filter=sym)
                    await safe_send_message(context, text=f"📰 Closing {sym} due to upcoming news event.")
        max_trades = config["trade_manager"].get("max_open_trades")
        if runtime_state.get("scanner_auto_paused") and max_trades:
            with mt5_sync_lock:
                current_trades = len(mt5.positions_get() or [])
            if current_trades < max_trades:
                with STATE_LOCK:
                    runtime_state["scanner_running"] = True
                    runtime_state["scanner_auto_paused"] = False
                await safe_send_message(context, "▶️ Scanner resumed: open trades below limit.")
        await sync_open_trades()
        await breakeven_monitor(context)
        await trailing_stop_monitor(context)
        await asyncio.sleep(0)  # Yield control
        # Additional monitors if defined
        try:
            await asyncio.gather(
                manual_trade_detector(context),
                position_monitor(context),
                trade_close_monitor(context)
            )
        except NameError:
            pass
    except Exception as e:
        logging.error(f"Error in trade_manager_loop: {e}", exc_info=True)


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Initialize the database using the global loop
    GLOBAL_LOOP.run_until_complete(init_db())
    if config["trade_manager"].get("ai_training"):
        try:
            seeded_rows = GLOBAL_LOOP.run_until_complete(seed_training_data())
        except Exception as e:
            logging.error(f"Seeding AI training data failed: {e}")
            seeded_rows = 0
        if seeded_rows >= MIN_TRAINING_ROWS:
            GLOBAL_LOOP.run_until_complete(train_ai_model())
        else:
            logging.warning(
                "Skipping AI model training: only %d rows available after seeding (minimum %d required).",
                seeded_rows,
                MIN_TRAINING_ROWS,
            )

    # Build the Application
    application = ApplicationBuilder().token(config["telegram"]["bot_token"]).build()

    # Register command handlers
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("start_scanner", cmd_start_scanner))
    application.add_handler(CommandHandler("stop_scanner", cmd_stop_scanner))
    application.add_handler(CommandHandler("set_mode",     cmd_set_mode))
    application.add_handler(configure_scanner_handler)
    application.add_handler(configure_trade_manager_handler)
    application.add_handler(CommandHandler("status", cmd_status))
    application.add_handler(CommandHandler("config_option", cmd_config_option))
    application.add_handler(CommandHandler("close", cmd_close))
    application.add_handler(CommandHandler("close_all", cmd_close_all))
    application.add_handler(CommandHandler("cancel", cfg_cancel))
    application.add_handler(CallbackQueryHandler(inline_button_callback))  # ← fixes inline button callbacks

    # Schedule background jobs
    async def _trigger_timeout(context: ContextTypes.DEFAULT_TYPE) -> None:
        return

    async def news_update_job(context: ContextTypes.DEFAULT_TYPE) -> None:
        if config["trade_manager"].get("use_news_filter"):
            await update_news_events()

    job_queue = application.job_queue

    def schedule_job(callback, first=0, interval=None):
        """Schedule jobs with backwards compatible arguments."""
        interval = interval if interval is not None else config["background_tasks_interval"]
        interval = max(interval, 1)
        params = inspect.signature(job_queue.run_repeating).parameters
        if "job_kwargs" in params:
            job_queue.run_repeating(
                callback,
                interval=interval,
                first=first,
                job_kwargs={"coalesce": True, "max_instances": 1},
            )
        else:
            # Fallback for very old PTB versions
            job_queue.run_repeating(
                callback,
                interval=interval,
                first=first,
                coalesce=True,
                max_instances=1,
            )

    schedule_job(_trigger_timeout, first=5)
    schedule_job(news_update_job, first=0)
    schedule_job(scanner_loop, first=0)
    schedule_job(trade_manager_loop, first=0)

    def watchdog() -> None:
        while True:
            interval = max(config["background_tasks_interval"], 1)
            time.sleep(interval * 5)
            now = datetime.now(timezone.utc)
            last_scan = runtime_state.get("last_scanner_run", now)
            if runtime_state.get("scanner_running") and (now - last_scan).total_seconds() > interval * 5:
                logging.info("Scanner loop stalled; forcing restart")
                application.job_queue.run_once(scanner_loop, 0)
                runtime_state["last_scanner_run"] = now
            last_tm = runtime_state.get("last_trade_manager_run", now)
            if runtime_state.get("trade_manager_running") and (now - last_tm).total_seconds() > interval * 5:
                logging.info("Trade manager loop stalled; forcing restart")
                application.job_queue.run_once(trade_manager_loop, 0)
                runtime_state["last_trade_manager_run"] = now

    threading.Thread(target=watchdog, daemon=True).start()

    # Periodically retrain the AI model if enabled
    if config["trade_manager"].get("ai_training"):
        interval_hours = config["trade_manager"].get("ai_training_interval_hours", 0)
        if interval_hours and interval_hours > 0:
            interval_seconds = interval_hours * 3600
            params = inspect.signature(job_queue.run_repeating).parameters
            if "job_kwargs" in params:
                job_queue.run_repeating(
                    train_ai_model,
                    interval=interval_seconds,
                    first=interval_seconds,
                    job_kwargs={"coalesce": True, "max_instances": 1},
                )
            else:
                job_queue.run_repeating(
                    train_ai_model,
                    interval=interval_seconds,
                    first=interval_seconds,
                    coalesce=True,
                    max_instances=1,
                )

    # Start the bot (this will create and manage its own event loop)
    application.run_polling()
