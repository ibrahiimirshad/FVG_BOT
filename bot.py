import pandas as pd
import numpy as np
from binance.spot import Spot  # ✅ Correct import for binance-connector
import time
import json
import os
from datetime import datetime, timedelta
from telegram import Bot
from telegram.error import TelegramError
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Tuple, List, Optional

# Configure logging (add terminal output for debugging)
logging.basicConfig(
    filename='fvg_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
# Add console handler for terminal output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger('').addHandler(console_handler)

# Load configuration from config.json
def load_config(file_path='config.json'):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error("config.json not found. Exiting.")
        print("Error: config.json not found. Please create or check the file.")
        exit(1)
    except json.JSONDecodeError:
        logging.error("config.json is invalid. Exiting.")
        print("Error: config.json is invalid. Please check the JSON format.")
        exit(1)

config = load_config()

# Binance API setup with early validation
API_KEY = os.getenv('BINANCE_API_KEY', config['binance']['api_key'])
API_SECRET = os.getenv('BINANCE_API_SECRET', config['binance']['api_secret'])
if not API_KEY or not API_SECRET:
    logging.error("Binance API key or secret missing in config.json or environment. Exiting.")
    print("Error: Binance API key or secret missing. Please update config.json.")
    exit(1)
try:
    client = Spot(API_KEY, API_SECRET)
    # Test API connection
    client.ping()
    logging.info("Binance API connection established successfully.")
    print("Binance API connection established successfully.")
except Exception as e:
    logging.error(f"Failed to connect to Binance API: {e}. Exiting.")
    print(f"Error: Failed to connect to Binance API: {e}. Please check API keys.")
    exit(1)

# Telegram setup with early validation
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', config['telegram']['bot_token'])
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', config['telegram']['chat_id'])
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logging.error("Telegram bot token or chat ID missing in config.json or environment. Exiting.")
    print("Error: Telegram bot token or chat ID missing. Please update config.json.")
    exit(1)
try:
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    async def send_startup_message():
         await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚀 Starting FVG bot monitoring...")
    asyncio.run(send_startup_message())  # Ensure it's run in an event loop
    logging.info("Telegram connection established successfully.")
    print("Telegram connection established successfully.")
except TelegramError as e:
    logging.error(f"Failed to connect to Telegram: {e}. Exiting.")
    print(f"Error: Failed to connect to Telegram: {e}. Please check bot token and chat ID.")
    exit(1)

async def send_telegram_alert(message):
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        logging.info(f"Telegram alert sent: {message}")
        print(f"Telegram alert sent: {message}")
    except TelegramError as e:
        logging.error(f"Telegram error: {e}")
        print(f"Telegram error: {e}")

def send_telegram_sync(message):
    asyncio.run(send_telegram_alert(message))

# Track API calls for rate limiting (optimized with retries)
class RateLimiter:
    def __init__(self, max_requests=1200, window_seconds=60, max_retries=3):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_count = 0
        self.last_reset = time.time()
        self.max_retries = max_retries

    def check(self, retries=0):
        current_time = time.time()
        if current_time - self.last_reset >= self.window_seconds:
            self.request_count = 0
            self.last_reset = current_time
        if self.request_count >= self.max_requests:
            if retries < self.max_retries:
                logging.warning(f"Rate limit exceeded. Retrying in 60 seconds (attempt {retries + 1}/{self.max_retries})")
                print(f"Rate limit exceeded. Retrying in 60 seconds (attempt {retries + 1}/{self.max_retries})")
                time.sleep(60)
                return self.check(retries + 1)
            else:
                logging.error(f"Max retries reached for rate limit. Pausing for 300 seconds.")
                print(f"Max retries reached for rate limit. Pausing for 300 seconds.")
                time.sleep(300)
                self.request_count = 0
                self.last_reset = time.time()
        self.request_count += 1
        return True

rate_limiter = RateLimiter(max_requests=1200, window_seconds=60, max_retries=3)

# Fetch Binance data with retries and error handling
def fetch_binance_data(symbol: str, interval: str, limit: int) -> pd.DataFrame:
    retries = 0
    max_retries = 3
    while retries < max_retries:
        try:
            rate_limiter.check(retries)
            klines = client.get_klines(symbol=symbol, interval=interval, limit=limit)
            if not klines:
                raise ValueError("No data returned from Binance API")
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df = df.astype(float)
            # Add VWAP for liquidity analysis
            df['vwap'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
            logging.info(f"Fetched {len(df)} {interval} candles for {symbol}")
            print(f"Fetched {len(df)} {interval} candles for {symbol}")
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap']]
        except Exception as e:
            retries += 1
            if retries < max_retries:
                logging.warning(f"Error fetching data for {symbol} ({interval}): {e}. Retrying (attempt {retries}/{max_retries})")
                print(f"Error fetching data for {symbol} ({interval}): {e}. Retrying (attempt {retries}/{max_retries})")
                time.sleep(30)  # Wait before retry
            else:
                logging.error(f"Max retries reached for {symbol} ({interval}): {e}. Skipping.")
                print(f"Max retries reached for {symbol} ({interval}): {e}. Skipping.")
                return pd.DataFrame()  # Return empty DataFrame to continue loop

# Fetch order book with retries and error handling
def fetch_order_book(symbol: str, limit: int = 20) -> dict:
    retries = 0
    max_retries = 3
    while retries < max_retries:
        try:
            rate_limiter.check(retries)
            order_book = client.get_orderbook(symbol=symbol, limit=limit)
            if not order_book or 'bids' not in order_book or 'asks' not in order_book:
                raise ValueError("Invalid order book data from Binance API")
            logging.info(f"Fetched order book for {symbol}")
            print(f"Fetched order book for {symbol}")
            return order_book
        except Exception as e:
            retries += 1
            if retries < max_retries:
                logging.warning(f"Error fetching order book for {symbol}: {e}. Retrying (attempt {retries}/{max_retries})")
                print(f"Error fetching order book for {symbol}: {e}. Retrying (attempt {retries}/{max_retries})")
                time.sleep(30)
            else:
                logging.error(f"Max retries reached for order book {symbol}: {e}. Skipping.")
                print(f"Max retries reached for order book {symbol}: {e}. Skipping.")
                return {'bids': [], 'asks': []}

# Detect Fair Value Gaps (FVGs) with volume and VWAP confirmation
def detect_fvg(df: pd.DataFrame, lookback: int = 3, min_size: float = 0.005, interval: str = '2h') -> List[Tuple[str, float, float, datetime]]:
    if df.empty:
        return []
    fvgs = []
    for i in range(len(df) - lookback):
        candle1, candle2, candle3 = df.iloc[i:i+3]
        # Bullish FVG: High of Candle 1 < Low of Candle 2, Low of Candle 3 > High of Candle 2
        if (candle1['high'] < candle2['low'] and candle3['low'] > candle2['high']):
            fvg_size = (candle2['low'] - candle1['high']) / candle1['high']
            volume_threshold = df['volume'].mean() * 1.5
            if (fvg_size >= min_size and candle2['volume'] > volume_threshold and 
                candle2['close'] > candle2['vwap']):
                fvgs.append(('bullish', candle1['high'], candle2['low'], candle2['timestamp']))
        # Bearish FVG (for completeness, though strategy focuses on bullish)
        elif (candle1['low'] > candle2['high'] and candle3['high'] < candle2['low']):
            fvg_size = (candle1['low'] - candle2['high']) / candle2['high']
            if (fvg_size >= min_size and candle2['volume'] > volume_threshold and 
                candle2['close'] < candle2['vwap']):
                fvgs.append(('bearish', candle2['high'], candle1['low'], candle2['timestamp']))
    logging.info(f"Detected {len(fvgs)} FVGs for {interval}")
    print(f"Detected {len(fvgs)} FVGs for {interval}")
    return fvgs

# Find swing highs with volume confirmation
def find_swing_highs(df: pd.DataFrame, lookback: int = 5) -> List[Tuple[float, datetime]]:
    if df.empty:
        return []
    swing_highs = []
    for i in range(lookback, len(df) - lookback):
        if i + lookback >= len(df):
            break
        is_high = all(df.iloc[i]['high'] >= df.iloc[i + j]['high'] for j in range(-lookback, lookback + 1) if j != 0)
        if is_high and df.iloc[i]['volume'] > df['volume'].mean():
            swing_highs.append((df.iloc[i]['high'], df.iloc[i]['timestamp']))
    logging.info(f"Found {len(swing_highs)} swing highs")
    print(f"Found {len(swing_highs)} swing highs")
    return swing_highs

# Calculate ATR for volatility
def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    if df.empty:
        return pd.Series()
    df = df.copy()
    df['high_low'] = df['high'] - df['low']
    df['high_close'] = abs(df['high'] - df['close'].shift(1))
    df['low_close'] = abs(df['low'] - df['close'].shift(1))
    df['tr'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
    return df['tr'].rolling(window=period, min_periods=1).mean()

# Calculate Fibonacci retracement levels with market maker precision
def calculate_fibonacci(low: float, high: float) -> dict:
    if pd.isna(low) or pd.isna(high) or low >= high:
        return {0.0: 0.0, 0.5: 0.0, 0.618: 0.0, 0.786: 0.0, 1.0: 0.0}
    diff = high - low
    levels = {
        0.0: low,
        0.236: high - (diff * 0.236),
        0.382: high - (diff * 0.382),
        0.5: high - (diff * 0.5),
        0.618: high - (diff * 0.618),
        0.786: high - (diff * 0.786),
        1.0: high
    }
    return levels

# Check higher timeframe (HTF) trend (daily) for alignment
def check_htf_trend(symbol: str, interval: str = '1d', limit: int = 30) -> str:
    df_daily = fetch_binance_data(symbol, interval, limit)
    if df_daily.empty:
        return 'neutral'
    latest_daily = df_daily.iloc[-1]
    ema20_daily = df_daily['close'].ewm(span=20, adjust=False).mean().iloc[-1]
    logging.info(f"HTF trend for {symbol}: {'bullish' if latest_daily['close'] > ema20_daily else 'bearish'}")
    print(f"HTF trend for {symbol}: {'bullish' if latest_daily['close'] > ema20_daily else 'bearish'}")
    return 'bullish' if latest_daily['close'] > ema20_daily else 'bearish'

# Long trade strategy with market maker optimizations (2h and 5m FVGs)
def long_trade_strategy(symbol: str = "BTCUSDT"):
    TRADING_PAIRS = [symbol]
    INTERVAL_2H = '2h'
    INTERVAL_5M = '5m'
    LIMIT_2H = 150
    LIMIT_5M = 1500
    ACCOUNT_RISK = 0.01  # 1% of account per trade
    MIN_RR = 2.0  # Minimum 2:1 R:R

    # Track state for the pair
    state = {
        'active_2h_fvg': None,  # (type, start, end, timestamp)
        'last_5m_swing_high': None,  # (high, timestamp)
        'active_5m_fvg': None,  # (type, start, end, timestamp)
        'fib_levels': None,
        'trade_active': False,
        'entry_price': None,
        'stop_loss': None,
        'take_profit': None,
        'position_size': None
    }

    logging.info(f"Starting monitoring for {symbol} at {datetime.now()}")
    print(f"Starting monitoring for {symbol} at {datetime.now()}")

    while True:
        try:
            # Load trading hours from config.json
            START_HOUR = config["trading"].get("start_hour", "07:00")
            END_HOUR = config["trading"].get("end_hour", "20:00")
            TRADE_WEEKENDS = config["trading"].get("trade_weekends", False)

            current_time = datetime.utcnow()  # Ensure UTC time
            start_time = datetime.strptime(START_HOUR, "%H:%M").time()
            end_time = datetime.strptime(END_HOUR, "%H:%M").time()

            if (not TRADE_WEEKENDS and current_time.weekday() >= 5) or not (start_time <= current_time.time() <= end_time):
              logging.info(f"Skipping {symbol} - Outside trading hours {current_time}")
              print(f"Skipping {symbol} - Outside trading hours {current_time}")
              time.sleep(300)
            continue

            # Fetch data for 2h, 5m, and daily for HTF trend
            logging.info(f"Fetching data for {symbol}...")
            print(f"Fetching data for {symbol}...")
            df_2h = fetch_binance_data(symbol, INTERVAL_2H, LIMIT_2H)
            df_5m = fetch_binance_data(symbol, INTERVAL_5M, LIMIT_5M)
            df_daily = fetch_binance_data(symbol, '1d', 30)
            if df_2h.empty or df_5m.empty or df_daily.empty:
                logging.warning(f"No data for {symbol} on 2h, 5m, or daily intervals. Retrying in 300 seconds.")
                print(f"No data for {symbol} on 2h, 5m, or daily intervals. Retrying in 300 seconds.")
                time.sleep(300)
                continue

            # Add ATR for volatility
            df_2h['ATR'] = calculate_atr(df_2h)
            df_5m['ATR'] = calculate_atr(df_5m)

            latest_2h = df_2h.iloc[-1]
            latest_5m = df_5m.iloc[-1]
            current_time = latest_5m['timestamp'].to_pydatetime()

            # Check HTF trend for alignment
            htf_trend = check_htf_trend(symbol)
            if htf_trend != 'bullish':
                logging.info(f"Skipping {symbol} - Daily trend is {htf_trend}, waiting for bullish alignment.")
                print(f"Skipping {symbol} - Daily trend is {htf_trend}, waiting for bullish alignment.")
                time.sleep(300)
                continue

            # Step 1: Detect 2h FVG (bullish only for long) with volume and VWAP
            logging.info(f"Detecting 2h FVGs for {symbol}...")
            print(f"Detecting 2h FVGs for {symbol}...")
            recent_2h = df_2h.iloc[max(0, len(df_2h) - 20):]
            fvgs_2h = detect_fvg(recent_2h, lookback=3, min_size=0.005)
            for fvg_type, fvg_start, fvg_end, fvg_time in fvgs_2h:
                if (fvg_type == 'bullish' and fvg_time <= latest_2h['timestamp'] and not state['active_2h_fvg']):
                    # Confirm with order book depth for liquidity
                    logging.info(f"Checking liquidity for 2h FVG in {symbol}...")
                    print(f"Checking liquidity for 2h FVG in {symbol}...")
                    order_book = fetch_order_book(symbol)
                    bid_volume = sum([float(level['quantity']) for level in order_book['bids']])
                    ask_volume = sum([float(level['quantity']) for level in order_book['asks']])
                    liquidity_threshold = df_2h['volume'].mean() * 2
                    if bid_volume + ask_volume < liquidity_threshold:
                        logging.info(f"Skipping {symbol} 2h FVG - Insufficient liquidity: {bid_volume + ask_volume:.2f}")
                        print(f"Skipping {symbol} 2h FVG - Insufficient liquidity: {bid_volume + ask_volume:.2f}")
                        continue

                    state['active_2h_fvg'] = (fvg_type, fvg_start, fvg_end, fvg_time)
                    message = f"📈 {symbol} 2h Bullish FVG Formed at {fvg_time} - Start: {fvg_start:.2f}, End: {fvg_end:.2f}, Volume: {df_2h[df_2h['timestamp'] == fvg_time]['volume'].iloc[0]:.2f}"
                    send_telegram_sync(message)
                    logging.info(message)
                    print(message)
                    break

            if not state['active_2h_fvg']:
                logging.info(f"No 2h FVG detected for {symbol}. Retrying in 60 seconds.")
                print(f"No 2h FVG detected for {symbol}. Retrying in 60 seconds.")
                time.sleep(60)
                continue

            fvg_type_2h, fvg_start_2h, fvg_end_2h, fvg_time_2h = state['active_2h_fvg']

            # Step 2: Price trades back into the 2h FVG with ATR confirmation
            logging.info(f"Checking 2h FVG retest for {symbol}...")
            print(f"Checking 2h FVG retest for {symbol}...")
            if (fvg_end_2h <= latest_2h['close'] <= fvg_start_2h and not state['last_5m_swing_high']):
                atr_2h = df_2h['ATR'].iloc[-1]
                if (latest_2h['close'] - fvg_start_2h) / atr_2h < 1.5:
                    message = f"🔄 {symbol} Price Traded Back into 2h FVG at {latest_2h['close']:.2f} - Start: {fvg_start_2h:.2f}, End: {fvg_end_2h:.2f}, ATR: {atr_2h:.2f}"
                    send_telegram_sync(message)
                    logging.info(message)
                    print(message)
                else:
                    logging.info(f"Skipping {symbol} - Insufficient volatility for 2h FVG retest: {atr_2h:.2f}")
                    print(f"Skipping {symbol} - Insufficient volatility for 2h FVG retest: {atr_2h:.2f}")
                    continue

                # Step 3 (Modified): Mark the last 5m swing high strictly before price traded into the 2h FVG
                logging.info(f"Finding 5m swing high for {symbol}...")
                print(f"Finding 5m swing high for {symbol}...")
                swing_highs_5m = find_swing_highs(df_5m, lookback=5)
                prior_swing_high_found = False
                for high, ts in reversed(swing_highs_5m):
                    if ts < fvg_time_2h:
                        state['last_5m_swing_high'] = (high, ts)
                        message = f"📊 {symbol} Last 5m Swing High Before 2h FVG: {high:.2f} at {ts}, Volume: {df_5m[df_5m['timestamp'] == ts]['volume'].iloc[0]:.2f}"
                        send_telegram_sync(message)
                        logging.info(message)
                        print(message)
                        prior_swing_high_found = True
                        break
                if not prior_swing_high_found:
                    message = f"⚠️ {symbol} No valid 5m swing high before 2h FVG at {fvg_time_2h}. Skipping trade setup."
                    send_telegram_sync(message)
                    logging.info(message)
                    print(message)
                    state = {  # Reset state
                        'active_2h_fvg': None,
                        'last_5m_swing_high': None,
                        'active_5m_fvg': None,
                        'fib_levels': None,
                        'trade_active': False,
                        'entry_price': None,
                        'stop_loss': None,
                        'take_profit': None,
                        'position_size': None
                    }
                    continue

            swing_high_5m, swing_time_5m = state['last_5m_swing_high']

            # Step 4: Price trades through the 5m swing high, and 5m candle body closes above it with ATR
            logging.info(f"Checking 5m swing high breakout for {symbol}...")
            print(f"Checking 5m swing high breakout for {symbol}...")
            recent_5m = df_5m.iloc[max(0, len(df_5m) - 20):]
            atr_5m = df_5m['ATR'].iloc[-1]
            if (latest_5m['close'] > swing_high_5m and not state['active_5m_fvg'] and 
                (latest_5m['close'] - swing_high_5m) / atr_5m >= 1.0):
                message = f"⬆️ {symbol} Price Traded Through 5m Swing High {swing_high_5m:.2f} at {latest_5m['close']:.2f}, ATR: {atr_5m:.2f}"
                send_telegram_sync(message)
                logging.info(message)
                print(message)

                # Step 5: The candle trades through the 5m swing high and leaves or creates a 5m FVG
                logging.info(f"Detecting 5m FVG for {symbol}...")
                print(f"Detecting 5m FVG for {symbol}...")
                fvgs_5m = detect_fvg(recent_5m, lookback=3, min_size=0.003)
                for fvg_type, fvg_start, fvg_end, fvg_time in fvgs_5m:
                    if (fvg_type == 'bullish' and fvg_time == latest_5m['timestamp'] and latest_5m['close'] > swing_high_5m):
                        state['active_5m_fvg'] = (fvg_type, fvg_start, fvg_end, fvg_time)
                        message = f"📉 {symbol} 5m Bullish FVG Created at {fvg_time} - Start: {fvg_start:.2f}, End: {fvg_end:.2f}, Volume: {df_5m[df_5m['timestamp'] == fvg_time]['volume'].iloc[0]:.2f}"
                        send_telegram_sync(message)
                        logging.info(message)
                        print(message)
                        break

            if not state['active_5m_fvg']:
                logging.info(f"No 5m FVG detected for {symbol}. Retrying in 60 seconds.")
                print(f"No 5m FVG detected for {symbol}. Retrying in 60 seconds.")
                time.sleep(60)
                continue

            fvg_type_5m, fvg_start_5m, fvg_end_5m, fvg_time_5m = state['active_5m_fvg']

            # Step 6: Price trades back into the 5m FVG with order book confirmation
            logging.info(f"Checking 5m FVG retest for {symbol}...")
            print(f"Checking 5m FVG retest for {symbol}...")
            if (fvg_end_5m <= latest_5m['close'] <= fvg_start_5m):
                order_book = fetch_order_book(symbol)
                bid_volume = sum([float(level['quantity']) for level in order_book['bids']])
                liquidity_threshold_5m = df_5m['volume'].mean() * 1.5
                if bid_volume < liquidity_threshold_5m:
                    logging.info(f"Skipping {symbol} 5m FVG retest - Low liquidity: {bid_volume:.2f}")
                    print(f"Skipping {symbol} 5m FVG retest - Low liquidity: {bid_volume:.2f}")
                    continue
                message = f"🔄 {symbol} Price Traded Back into 5m FVG at {latest_5m['close']:.2f} - Start: {fvg_start_5m:.2f}, End: {fvg_end_5m:.2f}, Liquidity: {bid_volume:.2f}"
                send_telegram_sync(message)
                logging.info(message)
                print(message)

            # Steps 7 & 8: Price retraces to at least the 0.5 Fib level, enter trade if in 5m FVG and below 0.5 Fib
            logging.info(f"Checking Fibonacci and entry for {symbol}...")
            print(f"Checking Fibonacci and entry for {symbol}...")
            if state['active_2h_fvg'] and state['active_5m_fvg']:
                after_2h_fvg = df_5m[df_5m['timestamp'] >= fvg_time_2h]
                if len(after_2h_fvg) >= 2:
                    low_after_2h = after_2h_fvg['low'].min()
                    high_before_5m = after_2h_fvg[after_2h_fvg['timestamp'] < fvg_time_5m]['high'].max()
                    if not pd.isna(high_before_5m) and not pd.isna(low_after_2h) and low_after_2h < high_before_5m:
                        fib_levels = calculate_fibonacci(low_after_2h, high_before_5m)
                        fib_0_5 = fib_levels[0.5]
                        fib_0_618 = fib_levels[0.618]
                        fib_0_786 = fib_levels[0.786]

                        if (fvg_end_5m <= latest_5m['close'] <= fvg_start_5m and 
                            latest_5m['close'] <= fib_0_5):
                            sweet_spot = (fvg_start_5m >= fib_0_618 and fvg_end_5m <= fib_0_786)
                            if not state['trade_active']:
                                entry_price = latest_5m['close']
                                position_size = calculate_position_size(symbol, ACCOUNT_RISK, entry_price, fib_0_5)
                                state['entry_price'] = entry_price
                                state['position_size'] = position_size

                                stop_loss = fvg_start_2h - (fvg_end_2h - fvg_start_2h) * 0.1
                                trailing_stop = stop_loss
                                state['stop_loss'] = trailing_stop

                                swing_highs_2h = find_swing_highs(df_2h, lookback=5)
                                take_profit_1 = max(high for high, _ in swing_highs_2h if _.to_pydatetime() > fvg_time_2h) if swing_highs_2h else fvg_end_2h * 1.05
                                take_profit_2 = take_profit_1 * 1.02
                                state['take_profit'] = [take_profit_1, take_profit_2]
                                state['trade_active'] = True

                                if sweet_spot:
                                    message = f"🌟 {symbol} Sweet Spot Entry (0.618-0.786 Fib) - "
                                else:
                                    message = f"⚠️ {symbol} Entry Below 0.786 Fib - Caution, sweet spot is 0.618-0.786 - "
                                
                                message += (f"Long Trade Entered at {entry_price:.2f} (Size: {position_size:.4f}) - "
                                           f"Stop Loss: {trailing_stop:.2f}, Take Profits: {take_profit_1:.2f}, {take_profit_2:.2f}, "
                                           f"5m FVG: {fvg_start_5m:.2f}-{fvg_end_5m:.2f}, 0.5 Fib: {fib_0_5:.2f}")
                                send_telegram_sync(message)
                                logging.info(message)
                                print(message)

                                risk = entry_price - trailing_stop
                                reward_1 = take_profit_1 - entry_price
                                reward_2 = take_profit_2 - entry_price
                                rr_ratio_1 = reward_1 / risk if risk > 0 else float('inf')
                                rr_ratio_2 = reward_2 / risk if risk > 0 else float('inf')
                                message = f"📊 {symbol} R:R Ratios: {rr_ratio_1:.2f} (TP1), {rr_ratio_2:.2f} (TP2)"
                                send_telegram_sync(message)
                                logging.info(message)
                                print(message)

            # Manage active trade: Trailing stop and partial exits
            if state['trade_active']:
                logging.info(f"Managing active trade for {symbol} at {latest_5m['close']:.2f}")
                print(f"Managing active trade for {symbol} at {latest_5m['close']:.2f}")
                current_price = latest_5m['close']
                trailing_stop = max(state['stop_loss'], current_price - df_5m['ATR'].iloc[-1] * 2)
                if current_price >= state['take_profit'][0] and state['position_size'] > 0:
                    partial_profit = state['position_size'] * 0.5
                    message = f"💰 {symbol} Partial Profit at {state['take_profit'][0]:.2f} - Remaining Size: {state['position_size'] * 0.5:.4f}"
                    send_telegram_sync(message)
                    logging.info(message)
                    print(message)
                    state['position_size'] *= 0.5
                if current_price >= state['take_profit'][1] and state['position_size'] > 0:
                    message = f"🏆 {symbol} Full Profit at {state['take_profit'][1]:.2f}"
                    send_telegram_sync(message)
                    logging.info(message)
                    print(message)
                    state['trade_active'] = False
                    state['position_size'] = 0
                state['stop_loss'] = trailing_stop

            # Invalidation Check: 2h candle closes above 2h FVG
            if state['active_2h_fvg'] and latest_2h['close'] > fvg_start_2h:
                message = f"⚠️ {symbol} Trade Invalidated - 2h Candle Closed Above 2h FVG at {latest_2h['close']:.2f}"
                send_telegram_sync(message)
                logging.info(message)
                print(message)
                state = {
                    'active_2h_fvg': None,
                    'last_5m_swing_high': None,
                    'active_5m_fvg': None,
                    'fib_levels': None,
                    'trade_active': False,
                    'entry_price': None,
                    'stop_loss': None,
                    'take_profit': None,
                    'position_size': None
                }

            # Invalidation Check: Price reaches take profit without entry
            if (state['trade_active'] and latest_2h['close'] >= state['take_profit'][1] and not state['entry_price']):
                message = f"⚠️ {symbol} Trade Invalidated - Price Reached Take Profit {state['take_profit'][1]:.2f} Without Entry"
                send_telegram_sync(message)
                logging.info(message)
                print(message)
                state['trade_active'] = False

            logging.info(f"Completed cycle for {symbol} at {datetime.now()}. Waiting 60 seconds...")
            print(f"Completed cycle for {symbol} at {datetime.now()}. Waiting 60 seconds...")
            time.sleep(60)  # Check every minute, adjusted for Heroku free tier

        except Exception as e:
            logging.error(f"Error in {symbol} strategy: {e}")
            print(f"Error in {symbol} strategy: {e}")
            send_telegram_sync(f"⚠️ Error in {symbol} strategy: {e}")
            time.sleep(300)  # Pause for recovery

def calculate_position_size(symbol: str, risk_percent: float, entry_price: float, stop_loss: float) -> float:
    # Simplified: Assume $10,000 account, 1% risk per trade
    account_balance = 10000.0  # Configurable in config.json
    risk_amount = account_balance * risk_percent
    risk_per_unit = entry_price - stop_loss
    if risk_per_unit <= 0:
        logging.warning("Invalid position size calculation: risk_per_unit <= 0")
        print("Invalid position size calculation: risk_per_unit <= 0")
        return 0.0
    position_size = risk_amount / risk_per_unit
    # Cap based on order book liquidity (simplified)
    order_book = fetch_order_book(symbol)
    max_liquidity = sum([float(level['quantity']) for level in order_book['bids']]) * 0.1  # 10% of bid liquidity
    calculated_size = min(position_size, max_liquidity)
    logging.info(f"Calculated position size for {symbol}: {calculated_size:.4f}")
    print(f"Calculated position size for {symbol}: {calculated_size:.4f}")
    return calculated_size

if __name__ == "__main__":
    # Monitor multiple pairs (configurable in config.json)
    pairs = config['trading']['pairs']  # e.g., ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
    with ThreadPoolExecutor(max_workers=len(pairs)) as executor:
        executor.map(long_trade_strategy, pairs)