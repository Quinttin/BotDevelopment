# ProfitPulse AI

This project integrates MetaTrader5 with a Telegram bot to scan markets,
place trades and manage positions. The repository ships with utilities and
an extensive bot script capable of semi or fully automatic trading.

## Requirements

* Python 3.10+
* MT5 terminal installed locally

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Configuration

1. Copy `config.example.json` to `config.json` and fill in your own
   MetaTrader5 credentials and Telegram bot information.
2. Optionally, sensitive values may be provided via environment variables:
   `MT5_TERMINAL_PATH`, `MT5_SERVER`, `MT5_ACCOUNT`, `MT5_PASSWORD`,
   `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `TELEGRAM_CHANNEL_ID`.
3. The bot reads `config.json` on startup and will create it during runtime
   when settings are saved. `config.json` is ignored by Git to avoid
   committing secrets.

### Equity-based protections

The `trade_manager` section of the configuration now supports two optional
fields:

* `breakeven_profit` – amount of profit (USD) required on a position before its
  stop-loss is moved to break even.
* `equity_profit_target` – total open profit (USD) at which all open positions
  are closed automatically.

## Models and data

* Historical sample data is provided in `historical_seed.csv`.
* Generate the machine-learning models by running:

```bash
python train_models.py
```

This produces `keras_trading_model.keras` and `sklearn_trading_model.pkl`
which the bot expects at runtime.

## Running

After installing dependencies and configuring credentials, start the bot
with:

```bash
python ProfitPulse_Ai.py
```

The bot will connect to MetaTrader5, initialise the Telegram interface and
await commands.
