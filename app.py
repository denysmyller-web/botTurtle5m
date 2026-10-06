# trade_rico
# Trade para ficar rico - Estratégia Turtle (Sistema 1)
import os
import sys
import time
import threading
import math
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from pybit.exceptions import InvalidRequestError
from pybit.unified_trading import HTTP
from ta.volatility import AverageTrueRange
from decimal import Decimal, ROUND_DOWN
from flask import Flask, render_template
import plotly.graph_objs as go
import plotly.io as pio
from security import configure_security, login_required
from api import api

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')


def _enable_terminal_colors():
    if not sys.stdout.isatty():
        return False
    if os.name != 'nt':
        return True

    try:
        import ctypes

        stdout_handle = ctypes.windll.kernel32.GetStdHandle(-11)
        console_mode = ctypes.c_uint()
        if not ctypes.windll.kernel32.GetConsoleMode(
            stdout_handle,
            ctypes.byref(console_mode),
        ):
            return False
        return bool(
            ctypes.windll.kernel32.SetConsoleMode(
                stdout_handle,
                console_mode.value | 0x0004,
            )
        )
    except (AttributeError, OSError):
        return False


TERMINAL_COLORS_ENABLED = _enable_terminal_colors()
TERMINAL_OUTPUT_LOCK = threading.Lock()


def print_terminal_status(symbol, status, detail=''):
    color = {
        'COMPRA': '\033[32m',
        'OBSERVANDO': '\033[31m',
        'ERRO': '\033[31m',
        'VENDA': '\033[33m',
        'VENDIDO': '\033[33m',
    }.get(status, '\033[31m')
    timestamp = datetime.now().astimezone().strftime('%d/%m/%Y %H:%M:%S')
    message = f'[{timestamp}] {symbol} | {status}'
    if detail:
        message = f'{message} | {detail}'
    if TERMINAL_COLORS_ENABLED:
        message = f'{color}{message}\033[0m'
    with TERMINAL_OUTPUT_LOCK:
        print(message, flush=True)


app = Flask(__name__, template_folder=BASE_DIR)

# ========================
# CONFIGURAÇÃO
# ========================
load_dotenv(os.path.join(BASE_DIR, 'env.txt'))
app.secret_key = os.getenv('FLASK_SECRET_KEY')
if not app.secret_key or len(app.secret_key) < 32:
    raise RuntimeError('FLASK_SECRET_KEY deve existir no env.txt e ter pelo menos 32 caracteres.')
configure_security(app)
app.register_blueprint(api)
API_KEY = os.getenv("BYBIT_API_KEY")
API_SECRET = os.getenv("BYBIT_API_SECRET")
MODE = os.getenv("MODE", "TEST").upper()
if MODE not in {'TEST', 'LIVE'}:
    raise RuntimeError("MODE deve ser TEST ou LIVE.")

# CAPITAL E GESTÃO DE RISCO PROFISSIONAL
INITIAL_CAPITAL = 100.0
ENTRY_SIZE_PCT = 1.0
POSITION_SIZE_PCT = ENTRY_SIZE_PCT
MIN_ENTRY_MARGIN_USDT = 30.0
BASE_ASSET = 'USDT'
BYBIT_CATEGORY = os.getenv("BYBIT_CATEGORY", "linear")
TICKER_INTERVAL = "5"  
CANDLE_LIMIT = 100  # Suficiente para o cálculo de 55/20
MAX_OPEN_POSITIONS = 1
SCAN_INTERVAL_SECONDS = 1  
RADAR_MAX_COINS = 250
MAX_SCAN_WORKERS = 3
SCAN_UNIVERSE_REFRESH_SECONDS = 300
KLINE_REQUEST_INTERVAL_SECONDS = 0.5
KLINE_CACHE_TTL_SECONDS = 20.0
TRADE_LOG_CSV = os.path.join(BASE_DIR, 'professional_trade_history.csv')
ANALYSIS_LOG_CSV = os.path.join(BASE_DIR, 'coin_analysis_log.csv')
ANALYSIS_LOG_FIELDS = [
    'checked_at_utc', 'symbol', 'scan_mode', 'status', 'price',
    'volume_latest_5m', 'volume_5m_vs_avg20', 'volume_recent_15m',
    'volume_previous_15m', 'volume_ratio_15m', 'variation_last_5m_pct',
    'variation_15m_pct', 'turnover_15m_usdt', 'trend_up', 'rsi',
    'rsi_confirmed', 'volume_confirmed', 'candle_confirmed', 'buy_ready',
    'risk_status', 'position_size_pct', 'leverage',
]

# PARÂMETROS ESTRATÉGIA TURTLE (SISTEMA 1)
ENTRY_BREAKOUT = 20 # Entra na máxima dos últimos 20
EXIT_BREAKDOWN = 10 # Sai na mínima dos últimos 10
ATR_WINDOW = 20 # Janela do ATR original dos Turtles
ATR_MULTIPLIER = 2.0 # Stop Loss em 2x o ATR

ALLOW_SHORTS = False
LEVERAGE = 3

# Inicializar cliente Bybit
client = HTTP(
    testnet=MODE == 'TEST',
    api_key=API_KEY,
    api_secret=API_SECRET,
    timeout=20,
)
print('🔧 Modo: TESTNET (simulação)' if MODE == 'TEST' else '⚠️  Modo: LIVE (negociação real)')

bot_thread = None
bot_stop_event = threading.Event()
read_only_scan_thread = None
read_only_scan_stop_event = threading.Event()
bot_state_lock = threading.Lock()
analysis_log_lock = threading.Lock()
scan_status = {}
last_bot_error = None
scan_symbols_cache = []
scan_symbols_updated_at = 0.0
wallet_balance_cache = None
wallet_balance_cached_at = 0.0
wallet_balance_cache_lock = threading.Lock()
wallet_balance_refresh_lock = threading.Lock()
wallet_balance_cache_ttl = 20.0
wallet_balance_last_error = None
kline_request_lock = threading.Lock()
kline_request_semaphore = threading.BoundedSemaphore(MAX_SCAN_WORKERS)
kline_cache_lock = threading.Lock()
kline_cache = {}
last_kline_request_at = 0.0


def get_available_wallet_balance(force_refresh=False):
    if MODE == 'TEST':
        return float(portfolio.get(BASE_ASSET, 0)), 'SIMULADO'

    global wallet_balance_cache, wallet_balance_cached_at, wallet_balance_last_error
    if not force_refresh:
        with wallet_balance_cache_lock:
            if wallet_balance_cache and time.monotonic() - wallet_balance_cached_at < wallet_balance_cache_ttl:
                return wallet_balance_cache

    with wallet_balance_refresh_lock:
        if not force_refresh:
            with wallet_balance_cache_lock:
                if wallet_balance_cache and time.monotonic() - wallet_balance_cached_at < wallet_balance_cache_ttl:
                    return wallet_balance_cache

        try:
            balance_source = 'BYBIT UNIFIED PRINCIPAL'
            response = client.get_wallet_balance(accountType='UNIFIED', coin=BASE_ASSET)
            if response.get('retCode') != 0:
                raise RuntimeError(response.get('retMsg', 'resposta inválida'))
            accounts = response.get('result', {}).get('list', [])
            coins = accounts[0].get('coin', []) if accounts else []
            usdt = next((coin for coin in coins if coin.get('coin') == BASE_ASSET), None)
            if not usdt:
                return 0.0, f'USDT não encontrado ({balance_source})'

            available = usdt.get('availableToWithdraw')
            if available in (None, ''):
                available = usdt.get('walletBalance', 0)
            result = (float(available), balance_source)
            with wallet_balance_cache_lock:
                wallet_balance_cache = result
                wallet_balance_cached_at = time.monotonic()
                wallet_balance_last_error = None
            return result
        except Exception as exc:
            print(f'❌ Erro ao consultar saldo USDT: {exc}', flush=True)
            wallet_balance_last_error = str(exc)
            if not force_refresh:
                with wallet_balance_cache_lock:
                    if wallet_balance_cache:
                        cached_balance, cached_source = wallet_balance_cache
                        return cached_balance, f'{cached_source} (último valor em cache)'
            return None, str(exc)


def get_cached_dashboard_wallet_balance():
    if MODE == 'TEST':
        return float(portfolio.get(BASE_ASSET, 0)), 'SIMULADO'
    with wallet_balance_cache_lock:
        if wallet_balance_cache:
            return wallet_balance_cache
    return None, 'Consultando saldo na Bybit...'


def get_dashboard_wallet_balance(force_refresh=False):
    global wallet_balance_cache, wallet_balance_cached_at, wallet_balance_last_error
    balance, source = get_available_wallet_balance(force_refresh=force_refresh)
    if MODE == 'TEST' or balance is None or balance > 0:
        return balance, source

    try:
        response = client.get_coin_balance(accountType='FUND', coin=BASE_ASSET)
        if response.get('retCode') != 0:
            raise RuntimeError(response.get('retMsg', 'resposta inválida'))
        funding = response.get('result', {}).get('balance', {})
        if funding.get('coin') != BASE_ASSET:
            return balance, source

        funding_balance = funding.get('transferBalance')
        if funding_balance in (None, ''):
            funding_balance = funding.get('walletBalance', 0)
        if float(funding_balance) > 0:
            result = (float(funding_balance), 'BYBIT FUNDING (fora da carteira de trading)')
            with wallet_balance_cache_lock:
                wallet_balance_cache = result
                wallet_balance_cached_at = time.monotonic()
                wallet_balance_last_error = None
            return result
    except Exception as exc:
        print(f'❌ Erro ao consultar saldo Funding: {exc}', flush=True)
    return balance, source


_symbol_filters_cache = {}

def get_symbol_filters(symbol):
    global _symbol_filters_cache
    if symbol in _symbol_filters_cache:
        return _symbol_filters_cache[symbol]
    try:
        response = client.get_instruments_info(category=BYBIT_CATEGORY, symbol=symbol)
        instruments = response.get('result', {}).get('list', [])
        if not instruments:
            raise Exception("Instrumento não encontrado")
        filters = instruments[0].get('lotSizeFilter', {})
        _symbol_filters_cache[symbol] = filters
        return filters
    except Exception as e:
        print(f'❌ Erro ao obter filtros para {symbol}: {e}')
        return {}

def _decimals_from_step(step_str):
    if not isinstance(step_str, str):
        step_str = str(step_str)
    if '.' in step_str:
        frac = step_str.split('.')[1].rstrip('0')
        return len(frac)
    return 0

def adjust_quantity_to_lot(symbol, raw_qty, price=None):
    try:
        filters = get_symbol_filters(symbol)
        lot = filters

        minQty = Decimal(lot.get('minOrderQty', '0'))
        maxQty = Decimal(lot.get('maxOrderQty', '1e12'))
        stepSize = Decimal(lot.get('qtyStep', '1'))

        minNotional = Decimal(str(10.0))

        raw = Decimal(str(raw_qty))
        decimals = _decimals_from_step(str(stepSize))

        if stepSize == 0:
            stepSize = Decimal('1')

        multiplier = (raw // stepSize)
        adjusted = (multiplier * stepSize).quantize(stepSize, rounding=ROUND_DOWN)

        if adjusted < minQty:
            mult_min = ( (minQty + stepSize - Decimal('1e-18')) // stepSize )
            adjusted_candidate = (mult_min * stepSize).quantize(stepSize, rounding=ROUND_DOWN)
            if adjusted_candidate < minQty:
                adjusted_candidate += stepSize
            adjusted = adjusted_candidate

        if adjusted > maxQty:
            adjusted = maxQty.quantize(stepSize, rounding=ROUND_DOWN)

        if price is not None and minNotional is not None and minNotional > 0:
            price_d = Decimal(str(price))
            notional = (adjusted * price_d).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN)
            if notional < minNotional:
                if price_d == 0:
                    return None
                needed_qty = (minNotional / price_d)
                multiplier_needed = ( (needed_qty + stepSize - Decimal('1e-18')) // stepSize )
                adjusted_candidate = (multiplier_needed * stepSize).quantize(stepSize, rounding=ROUND_DOWN)
                if adjusted_candidate < minQty:
                    adjusted_candidate = minQty
                if adjusted_candidate > maxQty:
                    return None
                adjusted = adjusted_candidate

        if adjusted < minQty or adjusted > maxQty:
            return None

        return float(adjusted.quantize(Decimal(1).scaleb(-decimals), rounding=ROUND_DOWN))
    except Exception as e:
        print(f'❌ Erro em adjust_quantity_to_lot({symbol}): {e}')
        return None

# Estado global
portfolio = {BASE_ASSET: INITIAL_CAPITAL}
current_positions = {}
trade_history = []

if not os.path.exists(TRADE_LOG_CSV):
    with open(TRADE_LOG_CSV, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'time', 'symbol', 'type', 'price', 'quantity', 'usdt_value',
            'stop_loss', 'take_profit', 'pattern', 'profit', 'status'
        ])
        writer.writeheader()


def get_all_usdt_pairs(limit_list=False):
    fallback_main_list = [
        '0GUSDT', '1000CATUSDT', '1000CHEEMSUSDT', '1000SATSUSDT', '1INCHUSDT', '1MBABYDOGEUSDT', '2ZUSDT', '4USDT', 
        'A2ZUSDT', 'AAVEUSDT', 'ABTUSDT', 'ABUSDT', 'ACMUSDT', 'ACTUSDT', 'ACXUSDT', 'ADAUSDT', 'ADSUSDT', 'AEVOUSDT', 
        'AERGOUSDT', 'AEROUSDT', 'AEURUSDT', 'AGIUSDT', 'AGLDUSDT', 'AIAUSDT', 'AIATUSDT', 'AIOZUSDT', 'AITECHUSDT', 
        'AIOTUSDT', 'AIUSDT', 'AIXBTUSDT', 'AI16ZUSDT', 'AKEUSDT', 'AKTUSDT', 'ALCHUSDT', 'ALEUSDT', 'ALGOUSDT', 
        'ALIUSDT', 'ALTUSDT', 'AMPUSDT', 'AMPLUSDT', 'ANDYUSDT', 'ANIMEUSDT', 'ANYONEUSDT', 'APEPEUSDT', 'APEUSDT', 
        'API3USDT', 'APTUSDT', 'APTMUSDT', 'APUUSDT', 'APRUSDT', 'ARBUSDT', 'ARDRUSDT', 'ARIAUSDT', 'ARKMUSDT', 
        'ARKUSDT', 'ARUSDT', 'ASTERUSDT', 'ASTRUSDT', 'ATHUSDT', 'ATOMUSDT', 'ATUSDT', 'AUCTIONUSDT', 'AUDIOUSDT', 
        'AURAUSDT', 'AURORAUSDT', 'AUSDT', 'AVAXUSDT', 'AVNTUSDT', 'AXLUSDT', 'AXSUSDT', 'B2USDT', 'B3USDT', 'BABYDOGEUSDT', 
        'BABYUSDT', 'BALUSDT', 'BANANAS31USDT', 'BANANAUSDT', 'BANDUSDT', 'BANKUSDT', 'BANUSDT', 'BASUSDT', 'BATUSDT', 
        'BAYUSDT', 'BBUSDT', 'BCHUSDT', 'BDCAUSDT', 'BDXUSDT', 'BEAMUSDT', 'BERAUSDT', 'BERTUSDT', 'BFCUSDT', 'BGBUSDT', 
        'BGSCUSDT', 'BICOUSDT', 'BIGTIMEUSDT', 'BIMUSDT', 'BITCOINUSDT', 'BLASTUSDT', 'BLESSUSDT', 'BMXUSDT', 'BNBUSDT', 
        'BNTUSDT', 'BOBAUSDT', 'BOLDUSDT', 'BOMEUSDT', 'BONKUSDT', 'BORAUSDT', 'BORGUSDT', 'BRETTUSDT', 'BSUUSDT', 
        'BSVUSDT', 'BTCUSDT', 'BTTUSDT', 'BURNUSDT', 'BUSDUSDT', 'BUSDT', 'C98USDT', 'CAKEUSDT', 'CARVUSDT', 'CATIUSDT', 
        'CATUSDT', 'CBKUSDT', 'CCDUSDT', 'CELRUSDT', 'CETUSDT', 'CETUSUSDT', 'CFXUSDT', 'CGPTUSDT', 'CHEELUSDT', 'CHEEMSUSDT', 
        'CHEXUSDT', 'CHRUSDT', 'CHZUSDT', 'CKBUSDT', 'CLASHUSDT', 'CLOUSDT', 'COAIUSDT', 'COMPUSDT', 'COOKIEUSDT', 'COREUMUSDT', 
        'COREUSDT', 'CORNUSDT', 'COTIUSDT', 'COWUSDT', 'CPOOLUSDT', 'CROUSDT', 'CROSSUSDT', 'CRVUSDT', 'CTCUSDT', 'CTKUSDT', 
        'CTMUSDT', 'CUSDUSDT', 'CVCUSDT', 'CVXUSDT', 'CYBERUSDT', 'DAGUSDT', 'DAIUSDT', 'DASHUSDT', 'DBRUSDT', 'DCRUSDT', 
        'DEEPUSDT', 'DEGENUSDT', 'DENTUSDT', 'DEPUSDT', 'DESOUSDT', 'DEVVEUSDT', 'DEXEUSDT', 'DHNUSDT', 'DIAUSDT', 'DKAUSDT', 
        'DOGEUSDT', 'DOGUSDT', 'DOLOUSDT', 'DOODUSDT', 'DORAUSDT', 'DOTUSDT', 'DPIUSDT', 'DRIFTUSDT', 'DSYNCUSDT', 'DUSDUSDT', 
        'DYDXUSDT', 'DYMUSDT', 'EDGEUSDT', 'EGL1USDT', 'EGLDUSDT', 'EIGENUSDT', 'ELAUSDT', 'ELGUSDT', 'ELONUSDT', 'ENAUSDT', 
        'ENJUSDT', 'ENSUSDT', 'ERAUSDT', 'ERGUSDT', 'ETCUSDT', 'ETHFIUSDT', 'ETHUSDT', 'ETNUSDT', 'EULUSDT', 'EURCUSDT', 
        'EURCVUSDT', 'EURIUSDT', 'EURTUSDT', 'EVAAUSDT', 'FAIUSDT', 'FARTCOINUSDT', 'FBUSDT', 'FDUSDUSDT', 'FETUSDT', 'FFUSDT', 
        'FIDAUSDT', 'FILUSDT', 'FIROUSDT', 'FLOCKUSDT', 'FLOKIUSDT', 'FLOWUSDT', 'FLRUSDT', 'FLUIDUSDT', 'FORMUSDT', 'FORTHUSDT', 
        'FRAXUSDT', 'FTNUSDT', 'FTTUSDT', 'FUNUSDT', 'FUSDT', 'GALAUSDT', 'GAMEUSDT', 'GASUSDT', 'GCBUSDT', 'GEARUSDT', 
        'GEODUSDT', 'GFIUSDT', 'GHOUSDT', 'GIGAUSDT', 'GIGGLEUSDT', 'GLMRUSDT', 'GLMUSDT', 'GMTUSDT', 'GNOUSDT', 'GOATUSDT', 
        'GODSUSDT', 'GOHOMEUSDT', 'GOMININGUSDT', 'GRASSUSDT', 'GRTUSDT', 'GTUSDT', 'GUSDUSDT', 'GUSDT', 'HBARUSDT', 'HEMIUSDT', 
        'HIVEUSDT', 'HNTUSDT', 'HOMEUSDT', 'HONEYUSDT', 'HPUSDT', 'HUMAUSDT', 'HUSDT', 'HYPEUSDT', 'HYPERUSDT', 'IAGUSDT', 
        'ICPUSDT', 'IDIAUSDT', 'IDOLUSDT', 'IKAUSDT', 'ILVUSDT', 'IMXUSDT', 'INJUSDT', 'INUSDT', 'IOSTUSDT', 'IOUSDT', 
        'IOTAUSDT', 'IPUSDT', 'IQUSDT', 'ISLMUSDT', 'IXTUSDT', 'JASMYUSDT', 'JELLYJELLYUSDT', 'JLPUSDT', 'JOEUSDT', 'JSTUSDT', 
        'JTOUSDT', 'JUPUSDT', 'KAIAUSDT', 'KAITOUSDT', 'KASUSDT', 'KCSUSDT', 'KERNELUSDT', 'KGENUSDT', 'KLKUSDT', 'KMNOUSDT', 
        'KNCUSDT', 'KOGEUSDT', 'KSMUSDT', 'KTAUSDT', 'LABUSDT', 'LAUSDT', 'LAYERUSDT', 'LBTUSDT', 'LDOUSDT', 'LEOUSDT', 
        'LGCTUSDT', 'LIBERTYUSDT', 'LIGHTUSDT', 'LINEAUSDT', 'LINKUSDT', 'LIONUSDT', 'LISUSDUSDT', 'LISTAUSDT', 'LITKEYUSDT', 
        'LONUSDT', 'LOOKUSDT', 'LPTUSDT', 'LQTYUSDT', 'LQUSDT', 'LSKUSDT', 'LTCUSDT', 'LUNAUSDT', 'LUNCUSDT', 'LURUSDT', 
        'LUSDUSDT', 'MAGICUSDT', 'MANAUSDT', 'MANTAUSDT', 'MASKUSDT', 'MEDUSDT', 'METISUSDT', 'METUSDT', 'MEUSDT', 'MEWUSDT', 
        'MEYUSDT', 'MGOUSDT', 'MIRAUSDT', 'MLKUSDT', 'MMUSDT', 'MNEEUSDT', 'MOBUSDT', 'MOCAUSDT', 'MOGUSDT', 'MORPHOUSDT', 
        'MORRAUSDT', 'MOVEUSDT', 'MOVRUSDT', 'MTLUSDT', 'MUSDUSDT', 'MUSDT', 'MVLUSDT', 'MXUSDT', 'MYTHUSDT', 'MYXUSDT', 
        'NAORISUSDT', 'NCTUSDT', 'NEARUSDT', 'NEIROUSDT', 'NEOUSDT', 'NEWTUSDT', 'NEXOUSDT', 'NFTUSDT', 'NILAUSDT', 'NILUSDT', 
        'NOMUSDT', 'NOTUSDT', 'NOWUSDT', 'NXPCUSDT', 'NYMUSDT', 'OBTUSDT', 'OGNUSDT', 'OGUSDT', 'OHOUSDT', 'OKBUSDT', 
        'OLASUSDT', 'OMIUSDT', 'ONDOUSDT', 'ONEUSDT', 'ONGUSDT', 'ONTUSDT', 'OPENUSDT', 'OPUSDT', 'ORBRUSDT', 'ORBSUSDT', 
        'ORDERUSDT', 'OSAKUSDT', 'OSMOUSDT', 'OXTUSDT', 'PAALUSDT', 'PARTIUSDT', 'PAXGUSDT', 'PCIUSDT', 'PENDLEUSDT', 'PENGUUSDT', 
        'PEPECOINUSDT', 'PEPEUSDT', 'PEOPLEUSDT', 'PHAUSDT', 'PIPPINUSDT', 'PIUSDT', 'PIXELUSDT', 'PLUMEUSDT', 'POKTUSDT', 
        'POLUSDT', 'POLYXUSDT', 'PONDUSDT', 'PONKEUSDT', 'POWRUSDT', 'PRIMEUSDT', 'PROMPTUSDT', 'PROMUSDT', 'PROUSDT', 
        'PTBUSDT', 'PUMPUSDT', 'PUNDIXUSDT', 'PYTHIAUSDT', 'PYTHUSDT', 'PYUSDUSDT', 'QAIUSDT', 'QANXUSDT', 'QIUSDT', 
        'QKCUSDT', 'QNTUSDT', 'QTUMUSDT', 'QUSDT', 'RAINUSDT', 'RAYUSDT', 'RECALLUSDT', 'RENDERUSDT', 'REZUSDT', 'RIFUSDT', 
        'RIOUSDT', 'RLCUSDT', 'RLUSDUSDT', 'RONUSDT', 'RPLUSDT', 'RSCUSDT', 'RSRUSDT', 'RUNEUSDT', 'SAGAUSDT', 'SAHARAUSDT', 
        'SANDUSDT', 'SAPIENUSDT', 'SAROSUSDT', 'SATSUSDT', 'SAUCEUSDT', 'SCRTUSDT', 'SDEXUSDT', 'SEIUSDT', 'SERVUSDT', 
        'SFPUSDT', 'SHELLUSDT', 'SHIBUSDT', 'SHXUSDT', 'SIGNUSDT', 'SIRENUSDT', 'SKIUSDT', 'SKYUSDT', 'SLPUSDT', 'SMARTUSDT', 
        'SNEKUSDT', 'SNTUSDT', 'SNUSDT', 'SNXUSDT', 'SOLUSDT', 'SOLVUSDT', 'SOMIUSDT', 'SONICUSDT', 'SOONUSDT', 'SOPHUSDT', 
        'SOSOUSDT', 'SPELLUSDT', 'SPKUSDT', 'SPXUSDT', 'SQDUSDT', 'SRXUSDT', 'SSVUSDT', 'STEEMUSDT', 'STORJUSDT', 'STOUSDT', 
        'STRAXUSDT', 'STRKUSDT', 'STRXUSDT', 'STXUSDT', 'SUIUSDT', 'SUNUSDT', 'SUPERUSDT', 'SUSDUSDT', 'SUSDT', 'SWFTCUSDT', 
        'SXPUSDT', 'SXTUSDT', 'SYRUPUSDT', 'TAGUSDT', 'TAIKOUSDT', 'TAIUSDT', 'TAOUSDT', 'TELUSDT', 'TFUELUSDT', 'THETAUSDT', 
        'TIAUSDT', 'TIBBIRUSDT', 'TOKAMAKUSDT', 'TONUSDT', 'TORNUSDT', 'TOSHIUSDT', 'TPTUSDT', 'TRACUSDT', 'TRADOORUSDT', 
        'TRBUSDT', 'TROLLUSDT', 'TRUMPUSDT', 'TRXUSDT', 'TSLAXUSDT', 'TUSDUSDT', 'TWTUSDT', 'UCNUSDT', 'UDSUSDT', 'UNIUSDT', 
        'UNPUSDT', 'UPCUSDT', 'UQCUSDT', 'URANUSUSDT', 'USD0USDT', 'USD1USDT', 'USDAIUSDT', 'USDBUSDT', 'USDCVUSDT', 
        'USDDUSDT', 'USDEUSDT', 'USDFUSDT', 'USDGUSDT', 'USDLUSDT', 'USDPUSDT', 'USDYUSDT', 'USELESSUSDT', 'USUALUSDT', 
        'VANRYUSDT', 'VBUSDUSDT', 'VELOUSDT', 'VENOMUSDT', 'VETUSDT', 'VIRTUALUSDT', 'VNDCUSDT', 'VRAUSDT', 'VRUSDT', 
        'VSNUSDT', 'VVVUSDT', 'WALUSDT', 'WAXPUSDT', 'WEMIXUSDT', 'WIFUSDT', 'WINUSDT', 'WKCUSDT', 'WLDUSDT', 'WLFIUSDT', 
        'WOOUSDT', 'WRXUSDT', 'WUSDT', 'XAIUSDT', 'XANUSDT', 'XAUMUSDT', 'XAUTUSDT', 'XCNUSDT', 'XDCUSDT', 'XECUSDT', 
        'XLMUSDT', 'XMRUSDT', 'XNOUSDT', 'XPLUSDT', 'XRPUSDT', 'XRDUSDT', 'XTZUSDT', 'XUSDUSDT', 'XVSUSDT', 'YBUSDT', 
        'YFIUSDT', 'YGGUSDT', 'YUSDUSDT', 'ZANOUSDT', 'ZBCNUSDT', 'ZBTUSDT', 'ZBUUSDT', 'ZECUSDT', 'ZENUSDT', 'ZENTUSDT', 
        'ZEREBROUSDT', 'ZKUSDT', 'ZKCUSDT', 'ZORAUSDT', 'ZRCUSDT', 'ZROUSDT', 'ZRXUSDT'
    ]

    try:
        instruments = []
        cursor = None
        while True:
            params = {
                'category': BYBIT_CATEGORY,
                'settleCoin': BASE_ASSET,
                'limit': 1000,
            }
            if cursor:
                params['cursor'] = cursor
            exchange_info = client.get_instruments_info(**params)
            if exchange_info.get('retCode') != 0:
                raise RuntimeError(exchange_info.get('retMsg', 'resposta inválida'))
            result = exchange_info.get('result', {})
            instruments.extend(result.get('list', []))
            next_cursor = result.get('nextPageCursor')
            if not next_cursor:
                break
            if next_cursor == cursor:
                raise RuntimeError('Cursor de paginação repetido pela Bybit.')
            cursor = next_cursor

        symbols = sorted({
            s['symbol'] for s in instruments
            if s.get('status') == 'Trading' 
            and s.get('settleCoin') == BASE_ASSET 
            and s['symbol'].endswith(BASE_ASSET)
        })

        if limit_list:
            return [s for s in symbols if s in fallback_main_list]
        
        return symbols

    except Exception as e:
        print(f'❌ Erro ao obter pares dinâmicos. Carregando modo de segurança: {e}')
        return fallback_main_list


def get_scan_symbols():
    global scan_symbols_cache, scan_symbols_updated_at

    now = time.monotonic()
    with bot_state_lock:
        if scan_symbols_cache and now - scan_symbols_updated_at < SCAN_UNIVERSE_REFRESH_SECONDS:
            return list(scan_symbols_cache)

    symbols = set(get_all_usdt_pairs(limit_list=False))
    if not symbols:
        raise RuntimeError('A Bybit não retornou contratos USDT para análise.')

    try:
        response = client.get_tickers(category=BYBIT_CATEGORY)
        if response.get('retCode') != 0:
            raise RuntimeError(response.get('retMsg', 'resposta inválida'))
        ranked = []
        for ticker in response.get('result', {}).get('list', []):
            symbol = ticker.get('symbol')
            if symbol not in symbols:
                continue
            try:
                turnover = float(ticker.get('turnover24h') or 0)
            except (TypeError, ValueError):
                continue
            if turnover > 0:
                ranked.append((turnover, symbol))
        ranked.sort(reverse=True)
        selected = [symbol for _, symbol in ranked[:RADAR_MAX_COINS]]
        if not selected:
            raise RuntimeError('A Bybit não retornou volume para os contratos USDT.')
    except Exception as exc:
        print(f'⚠️ Falha ao ordenar contratos por liquidez; usando fallback de símbolos: {exc}', flush=True)
        selected = sorted(symbols)[:RADAR_MAX_COINS]

    with bot_state_lock:
        scan_symbols_cache = selected
        scan_symbols_updated_at = time.monotonic()
    print(f'📡 Radar configurado para {len(selected)} contratos USDT de maior liquidez.', flush=True)
    return list(selected)


def get_historical_data(symbol, interval, limit):
    global last_kline_request_at
    cache_key = (symbol, interval, limit)
    now = time.monotonic()
    with kline_cache_lock:
        cached = kline_cache.get(cache_key)
        if cached and now - cached[0] < KLINE_CACHE_TTL_SECONDS:
            return cached[1].copy()

    with kline_request_semaphore:
        now = time.monotonic()
        with kline_cache_lock:
            cached = kline_cache.get(cache_key)
            if cached and now - cached[0] < KLINE_CACHE_TTL_SECONDS:
                return cached[1].copy()

        with kline_request_lock:
            now = time.monotonic()
            wait_seconds = KLINE_REQUEST_INTERVAL_SECONDS - (now - last_kline_request_at)
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            last_kline_request_at = time.monotonic()

        try:
            response = client.get_kline(
                category=BYBIT_CATEGORY, symbol=symbol, interval=interval, limit=limit
            )
            if response.get('retCode') != 0:
                raise RuntimeError(response.get('retMsg', 'resposta inválida'))
            klines = list(reversed(response.get('result', {}).get('list', [])))
            if not klines:
                return pd.DataFrame()

            df = pd.DataFrame(klines, columns=[
                'Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Turnover'
            ])
            for col in ['Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']:
                df[col] = pd.to_numeric(df[col])
            df['Time'] = pd.to_datetime(pd.to_numeric(df['Open time']), unit='ms')
            df.set_index('Time', inplace=True)
            with kline_cache_lock:
                expired_keys = [
                    key
                    for key, (cached_at, _) in kline_cache.items()
                    if time.monotonic() - cached_at >= KLINE_CACHE_TTL_SECONDS
                ]
                for key in expired_keys:
                    del kline_cache[key]
                kline_cache[cache_key] = (time.monotonic(), df.copy())
            return df
        except Exception as exc:
            print(f'❌ Erro ao obter candles de {symbol} ({interval}m): {exc}', flush=True)
            return pd.DataFrame()


def analyze_symbols_concurrently(symbols, stop_event):
    with ThreadPoolExecutor(max_workers=MAX_SCAN_WORKERS) as executor:
        futures = {
            executor.submit(analyze_market_symbol, symbol): symbol
            for symbol in symbols
        }
        try:
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                symbol = futures[future]
                try:
                    yield symbol, future.result()
                except Exception as exc:
                    print(f'❌ Erro ao analisar {symbol}: {exc}', flush=True)
        finally:
            for future in futures:
                if not future.done():
                    future.cancel()


def apply_indicators(df):
    if df.empty:
        return df
    df = df.copy()

    # Cálculo Turtle 1: Highest High dos ultimos 20 candles (para entrada)
    df['Highest_High'] = df['High'].rolling(window=ENTRY_BREAKOUT).max().shift(1)
    
    # Cálculo Turtle 2: Lowest Low dos ultimos 10 candles (para saída de comprado)
    df['Lowest_Low'] = df['Low'].rolling(window=EXIT_BREAKDOWN).min().shift(1)
    
    # Volume MA para filtro
    df['Volume_MA'] = df['Volume'].rolling(window=20, min_periods=1).mean()

    # ATR para o Stop Loss Turtle
    try:
        df['ATR'] = AverageTrueRange(high=df['High'], low=df['Low'], close=df['Close'], window=ATR_WINDOW).average_true_range()
    except Exception:
        df['ATR'] = np.nan

    return df


def generate_signal(df):
    """Gera sinais filtrados pela estratégia Turtle de Richard Dennis."""
    
    df = apply_indicators(df)
    
    if df.empty or pd.isna(df['Highest_High'].iloc[-1]):
        return None, None

    last_candle = df.iloc[-1]
    
    # Condição Turtle de Compra: Preço de fechamento supera a máxima dos últimos 20 candles
    is_turtle_buy = last_candle['Close'] > last_candle['Highest_High']
    
    # Filtro de qualidade: Apenas entrar se houver volume no rompimento
    volume_ma = last_candle['Volume_MA']
    volume_ratio = last_candle['Volume'] / volume_ma if volume_ma and not pd.isna(volume_ma) else 0
    is_volume_confirmed = volume_ratio >= 1.2 # Exige um pouco de volume
    
    if is_turtle_buy and is_volume_confirmed:
        pattern_info = {
            'type': 'TURTLE_BREAKOUT',
            'signal': 'COMPRA',
            'quality': 1.0,
            'highest_20': last_candle['Highest_High'],
            'lowest_10': last_candle['Lowest_Low'],
            'atr': last_candle['ATR'],
            'excellent_buy': True
        }
        return 'COMPRA', pattern_info

    # (Opcional) Implementação Short para Turtles
    if ALLOW_SHORTS:
        is_turtle_sell = last_candle['Close'] < last_candle['Lowest_Low']
        if is_turtle_sell and is_volume_confirmed:
            pattern_info = {
                'type': 'TURTLE_BREAKDOWN',
                'signal': 'VENDA',
                'quality': 1.0,
                'lowest_10': last_candle['Lowest_Low'],
                'highest_20': last_candle['Highest_High'],
                'atr': last_candle['ATR'],
                'excellent_buy': True
            }
            return 'VENDA', pattern_info
            
    return None, None


def analyze_market_symbol(symbol):
    timestamp = datetime.now(timezone.utc).isoformat()
    with bot_state_lock:
        scan_status[symbol] = {
            'symbol': symbol,
            'status': 'ANALISANDO',
            'updated_at': timestamp,
            'buy_analysis': {'risk_status': 'Buscando candles de 5 minutos.'},
        }

    try:
        df = get_historical_data(symbol, TICKER_INTERVAL, CANDLE_LIMIT)
        if df.empty:
            raise RuntimeError('A Bybit não retornou candles para este contrato.')

        df = apply_indicators(df)
        signal, pattern_info = generate_signal(df)
        candle = df.iloc[-1]
        close = float(candle['Close'])
        open_price = float(candle['Open'])
        volume = float(candle['Volume'])
        volume_average = float(candle['Volume_MA']) if pd.notna(candle['Volume_MA']) else 0.0
        volume_ratio = volume / volume_average if volume_average > 0 else 0.0
        price_change_pct = ((close - open_price) / open_price * 100) if open_price else 0.0

        changes = df['Close'].diff()
        average_gain = changes.clip(lower=0).ewm(span=14, min_periods=14, adjust=False).mean().iloc[-1]
        average_loss = -changes.clip(upper=0).ewm(span=14, min_periods=14, adjust=False).mean().iloc[-1]
        if pd.isna(average_gain) or pd.isna(average_loss):
            rsi = None
        elif average_loss == 0:
            rsi = 100.0 if average_gain > 0 else 50.0
        else:
            rsi = float(100 - (100 / (1 + average_gain / average_loss)))

        turnover = float(candle['Turnover']) if pd.notna(candle['Turnover']) else 0.0
        if signal == 'COMPRA' and pattern_info:
            atr = float(candle['ATR']) if pd.notna(candle['ATR']) else 0.0
            breakout_distance = max(close - float(pattern_info['highest_20']), 0.0)
            pattern_info.update({
                'volume_ratio': volume_ratio,
                'turnover_5m_usdt': turnover,
                'breakout_atr': breakout_distance / atr if atr > 0 else None,
            })
        status = signal or 'OBSERVANDO'
        risk_status = (
            'Rompimento Turtle confirmado; aguardando gestão de risco.'
            if signal == 'COMPRA'
            else 'Aguardando rompimento da máxima Turtle com volume.'
        )
        entry = {
            'symbol': symbol,
            'price': close,
            'status': status,
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'buy_analysis': {
                'score': 100 if signal == 'COMPRA' else None,
                'price_change_pct': price_change_pct,
                'volume_5m_vs_avg20': volume_ratio,
                'volume_latest_5m': volume,
                'turnover_5m_usdt': turnover,
                'rsi': rsi,
                'risk_status': risk_status,
            },
        }
        with bot_state_lock:
            scan_status[symbol] = entry
        print_terminal_status(symbol, status, f'${close:.8f}')
        return signal, pattern_info, df, close
    except Exception as exc:
        print_terminal_status(symbol, 'ERRO', str(exc))
        with bot_state_lock:
            scan_status[symbol] = {
                'symbol': symbol,
                'status': 'ERRO',
                'updated_at': datetime.now(timezone.utc).isoformat(),
                'buy_analysis': {
                    'risk_status': f'Falha na leitura: {exc}',
                },
            }
        return None


def rank_buy_candidates(candidates):
    if not candidates:
        return []

    valid_candidates = [
        candidate for candidate in candidates
        if candidate[1][1].get('breakout_atr') is not None
        and math.isfinite(candidate[1][1]['breakout_atr'])
        and math.isfinite(candidate[1][1]['volume_ratio'])
        and math.isfinite(candidate[1][1]['turnover_5m_usdt'])
    ]
    if not valid_candidates:
        return []

    volume_ratios = [candidate[1][1]['volume_ratio'] for candidate in valid_candidates]
    breakout_extensions = [candidate[1][1]['breakout_atr'] for candidate in valid_candidates]
    turnovers = [candidate[1][1]['turnover_5m_usdt'] for candidate in valid_candidates]

    def normalized(value, values, prefer_lower=False):
        lowest = min(values)
        highest = max(values)
        if highest == lowest:
            return 1.0
        score = (value - lowest) / (highest - lowest)
        return 1.0 - score if prefer_lower else score

    scored_candidates = []
    for candidate in valid_candidates:
        symbol, analysis = candidate
        _, pattern_info, _, _ = analysis
        volume_score = normalized(pattern_info['volume_ratio'], volume_ratios)
        freshness_score = normalized(
            pattern_info['breakout_atr'],
            breakout_extensions,
            prefer_lower=True,
        )
        liquidity_score = normalized(
            pattern_info['turnover_5m_usdt'],
            turnovers,
        )
        quality_score = (
            volume_score * 0.5
            + freshness_score * 0.3
            + liquidity_score * 0.2
        )
        scored_candidates.append((
            (
                quality_score,
                pattern_info['volume_ratio'],
                -pattern_info['breakout_atr'],
                pattern_info['turnover_5m_usdt'],
                symbol,
            ),
            candidate,
        ))

    scored_candidates.sort(key=lambda scored: scored[0], reverse=True)
    return [candidate for _, candidate in scored_candidates]


def calculate_entry_margin(available_balance, position_size_pct=None):
    balance = max(float(available_balance or 0), 0.0)
    percentage = POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    return min(balance, max(MIN_ENTRY_MARGIN_USDT, balance * percentage))


def calculate_position_size(
    pattern_info,
    entry_price,
    df,
    symbol,
    is_short=False,
    available_balance=None,
):
    """
    Usa 100% do saldo disponível como margem e aplica a alavancagem
    para calcular o valor nocional da posição.
    Stop loss em 2x o ATR.
    Não há alvo fixo de lucro, ele corre até perder a mínima.
    """
    usdt_balance = (
        portfolio.get(BASE_ASSET, 0)
        if available_balance is None
        else available_balance
    )
    
    atr = df['ATR'].iloc[-1] if 'ATR' in df.columns and pd.notna(df['ATR'].iloc[-1]) else 0
    
    # Se não tem ATR válido, recusa a operação para proteger o capital
    if atr <= 0:
        print(f'⏭️ Compra cancelada para {symbol}: ATR inválido ({atr}).', flush=True)
        return None, None, None
        
    stop_distance = atr * ATR_MULTIPLIER
    
    if not is_short:
        stop_loss = entry_price - stop_distance
    else:
        stop_loss = entry_price + stop_distance
        
    if entry_price <= 0:
        print(f'⏭️ Compra cancelada para {symbol}: preço de entrada inválido.', flush=True)
        return None, None, None
        
    margin_budget = calculate_entry_margin(usdt_balance, position_size_pct=1.0)
    if margin_budget <= 0:
        print(f'⏭️ Compra cancelada para {symbol}: saldo disponível zerado.', flush=True)
        return None, None, None
    position_value = margin_budget * LEVERAGE

    raw_quantity = position_value / entry_price
    quantity = adjust_quantity_to_lot(symbol, raw_quantity, entry_price)
    
    if quantity is None:
        print(f'⏭️ Compra cancelada para {symbol}: quantidade fora dos filtros do contrato.', flush=True)
        return None, None, None

    actual_position_value = Decimal(str(quantity)) * Decimal(str(entry_price))
    if actual_position_value > Decimal(str(position_value)):
        print(
            f'⏭️ Compra cancelada para {symbol}: valor mínimo do contrato '
            f'excede o nocional permitido de {position_value:.4f} USDT.',
            flush=True,
        )
        return None, None, None
        
    # Como os Turtles deixam o lucro correr, o Take Profit é virtualmente infinito (ou zero para não engatilhar ordens limit)
    take_profit = 0 
    
    return quantity, stop_loss, take_profit


def set_symbol_leverage(symbol):
    if MODE == 'TEST':
        return True

    try:
        response = client.set_leverage(
            category=BYBIT_CATEGORY,
            symbol=symbol,
            buyLeverage=str(LEVERAGE),
            sellLeverage=str(LEVERAGE),
        )
        ret_code = response.get('retCode')
        if ret_code not in (0, 110043):
            print(
                f'❌ Não foi possível configurar alavancagem de {symbol}: '
                f"{ret_code} - {response.get('retMsg', 'resposta inválida')}",
                flush=True,
            )
            return False
        return True
    except InvalidRequestError as exc:
        if exc.status_code == 110043:
            print(
                f'ℹ️ Alavancagem de {symbol} já está em {LEVERAGE}x; '
                'seguindo com a operação.',
                flush=True,
            )
            return True
        print(f'❌ Erro ao configurar alavancagem de {symbol}: {exc}', flush=True)
        return False
    except Exception as exc:
        print(f'❌ Erro ao configurar alavancagem de {symbol}: {exc}', flush=True)
        return False

def execute_buy(symbol, price, pattern_info, df):
    if symbol in current_positions:
        print(f'⏭️ Compra cancelada para {symbol}: já existe uma posição aberta.', flush=True)
        return
    if len(current_positions) >= MAX_OPEN_POSITIONS:
        print(
            f'⏭️ Compra cancelada para {symbol}: limite de '
            f'{MAX_OPEN_POSITIONS} posições simultâneas atingido.',
            flush=True,
        )
        return

    if MODE == 'LIVE':
        available_balance, balance_source = get_available_wallet_balance(force_refresh=True)
        if available_balance is None:
            print(
                f'⏭️ Compra cancelada para {symbol}: saldo disponível indisponível '
                f'({balance_source}).',
                flush=True,
            )
            return
    else:
        available_balance = portfolio.get(BASE_ASSET, 0)

    quantity, stop_loss, take_profit = calculate_position_size(
        pattern_info,
        price,
        df,
        symbol,
        is_short=False,
        available_balance=available_balance,
    )
    
    if quantity is None:
        return
    
    usdt_needed = quantity * price
    margin_needed = usdt_needed / LEVERAGE

    if MODE == 'LIVE':
        if margin_needed > available_balance:
            print(
                f'⏭️ Compra cancelada para {symbol}: saldo disponível insuficiente '
                f'({available_balance} USDT; margem necessária {margin_needed:.4f} USDT '
                f'para posição de {usdt_needed:.4f} USDT; {balance_source}).',
                flush=True,
            )
            return
            
    if usdt_needed < 10.0: # MIN_NOTIONAL
        print(f'⏭️ Compra cancelada para {symbol}: ordem abaixo do mínimo de 10 USDT ({usdt_needed:.4f} USDT).', flush=True)
        return
    
    pattern_name = pattern_info['type'] if pattern_info else 'TURTLE'

    if not set_symbol_leverage(symbol):
        return
    
    if MODE == 'TEST':
        portfolio[BASE_ASSET] -= margin_needed
        base_sym = symbol.replace(BASE_ASSET, '')
        portfolio[base_sym] = portfolio.get(base_sym, 0) + quantity
        current_positions[symbol] = {'type': 'LONG', 'leverage': LEVERAGE}
        
        trade = {
            'time': datetime.now(timezone.utc).isoformat(),
            'symbol': symbol,
            'type': 'BUY',
            'price': price,
            'quantity': quantity,
            'usdt_value': usdt_needed,
            'stop_loss': stop_loss,
            'take_profit': 0, # Turtle não usa alvo fixo
            'pattern': pattern_name,
            'profit': 0.0,
            'status': 'OPEN'
        }
        trade_history.append(trade)
        write_trade_csv(trade)
        
        print(f'✅ COMPRA SIMULADA TURTLE {symbol} | Exposição: {usdt_needed:.4f} USDT | Margem: {margin_needed:.4f} USDT')
        print(f'   💵 Preço: ${price:.4f}')
        print(f'   🛑 Stop Loss 2x ATR: ${stop_loss:.4f}')
    else:
        try:
            order = client.place_order(category=BYBIT_CATEGORY, symbol=symbol,
                                        side='Buy', orderType='Market',
                                        qty=str(quantity), positionIdx=0)
            if order.get('retCode') != 0:
                raise RuntimeError(
                    f"{order.get('retCode')} - {order.get('retMsg', 'ordem rejeitada')}"
                )
            
            portfolio[BASE_ASSET] -= margin_needed
            base_sym = symbol.replace(BASE_ASSET, '')
            portfolio[base_sym] = portfolio.get(base_sym, 0) + quantity
            current_positions[symbol] = {'type': 'LONG', 'leverage': LEVERAGE}
            
            trade = {
                'time': datetime.now(timezone.utc).isoformat(),
                'symbol': symbol,
                'type': 'BUY',
                'price': price,
                'quantity': quantity,
                'usdt_value': usdt_needed,
                'stop_loss': stop_loss,
                'take_profit': 0,
                'pattern': pattern_name,
                'profit': 0.0,
                'status': 'OPEN'
            }
            trade_history.append(trade)
            write_trade_csv(trade)
            print(
                f'✅ COMPRA LIVE TURTLE {symbol} a ${price:.4f} | '
                f'Exposição: {usdt_needed:.4f} USDT | Margem: {margin_needed:.4f} USDT'
            )
        except InvalidRequestError as exc:
            if exc.status_code == 110007:
                print(
                    f'❌ Compra LIVE recusada para {symbol}: saldo/margem disponível '
                    f'insuficiente na Bybit (110007). Margem estimada: '
                    f'{margin_needed:.4f} USDT; o bot tenta usar 100% do saldo '
                    'disponível como margem.',
                    flush=True,
                )
                return
            print(f'❌ Erro BUY LIVE: {exc}', flush=True)
        except Exception as e:
            print(f'❌ Erro BUY LIVE: {e}')


def execute_sell(symbol, price, reason='TURTLE_EXIT'):
    base_sym = symbol.replace(BASE_ASSET, '')
    qty = portfolio.get(base_sym, 0)
    
    if qty <= 0:
        return
    
    qty_to_sell = adjust_quantity_to_lot(symbol, qty, price)
    if qty_to_sell is None or qty_to_sell <= 0:
        return
    
    usdt_received = qty_to_sell * price * 0.999  
    
    buy_trade = next(
        (t for t in trade_history if t['symbol'] == symbol and t['type'] == 'BUY' and t['status'] == 'OPEN'),
        None,
    )
    profit = usdt_received - buy_trade['usdt_value'] if buy_trade else 0.0
    position = current_positions.get(symbol)
    position_leverage = position.get('leverage', LEVERAGE) if position else LEVERAGE
    margin_used = buy_trade['usdt_value'] / position_leverage if buy_trade else 0.0

    if MODE == 'TEST':
        portfolio[BASE_ASSET] += margin_used + profit
        del portfolio[base_sym]

        if buy_trade:
            buy_trade['profit'] = profit
            buy_trade['status'] = 'CLOSED'
        
        trade = {
            'time': datetime.now(timezone.utc).isoformat(),
            'symbol': symbol,
            'type': 'SELL',
            'price': price,
            'quantity': qty_to_sell,
            'usdt_value': usdt_received,
            'stop_loss': 0,
            'take_profit': 0,
            'pattern': buy_trade['pattern'] if buy_trade else 'TURTLE',
            'profit': profit,
            'status': reason
        }
        trade_history.append(trade)
        write_trade_csv(trade)
        current_positions.pop(symbol, None)
        
        print_terminal_status(
            symbol,
            'VENDIDO',
            f'Lucro: ${profit:.2f} | Motivo: {reason}',
        )
    else:
        try:
            order = client.place_order(category=BYBIT_CATEGORY, symbol=symbol,
                                        side='Sell', orderType='Market',
                                        qty=str(qty_to_sell), positionIdx=0,
                                        reduceOnly=True)
            if order.get('retCode') != 0:
                raise RuntimeError(
                    f"{order.get('retCode')} - {order.get('retMsg', 'ordem rejeitada')}"
                )
            
            portfolio[BASE_ASSET] += margin_used + profit
            del portfolio[base_sym]
            current_positions.pop(symbol, None)

            if buy_trade:
                buy_trade['profit'] = profit
                buy_trade['status'] = 'CLOSED'
            
            trade = {
                'time': datetime.now(timezone.utc).isoformat(),
                'symbol': symbol,
                'type': 'SELL',
                'price': price,
                'quantity': qty_to_sell,
                'usdt_value': usdt_received,
                'stop_loss': 0,
                'take_profit': 0,
                'pattern': buy_trade['pattern'] if buy_trade else 'TURTLE',
                'profit': profit,
                'status': reason
            }
            trade_history.append(trade)
            write_trade_csv(trade)
            print_terminal_status(
                symbol,
                'VENDIDO',
                f'Lucro: ${profit:.2f} | Motivo: {reason}',
            )
        except Exception as e:
            print(f'❌ Erro SELL LIVE: {e}')


def execute_short(symbol, price, pattern_info, df):
    # Lógica de Short simplificada para Turtles mantida do sistema original
    pass

def execute_cover(symbol, price, reason='TURTLE_EXIT'):
    # Lógica de Cover simplificada para Turtles mantida do sistema original
    pass


def write_trade_csv(trade):
    with open(TRADE_LOG_CSV, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'time', 'symbol', 'type', 'price', 'quantity', 'usdt_value',
            'stop_loss', 'take_profit', 'pattern', 'profit', 'status'
        ])
        writer.writerow(trade)


def get_closed_trade_history():
    pending_by_symbol = {}
    closed_trades = []

    try:
        with open(TRADE_LOG_CSV, 'r', newline='', encoding='utf-8-sig') as f:
            rows = csv.DictReader(f)
            for row_number, row in enumerate(rows, start=2):
                symbol = (row.get('symbol') or '').strip()
                action = (row.get('type') or '').upper()
                status = (row.get('status') or '').upper()

                if not symbol:
                    continue
                if action in {'BUY', 'SHORT'} and status == 'OPEN':
                    pending_by_symbol.setdefault(symbol, []).append(row)
                    continue
                if action not in {'SELL', 'COVER'} or status == 'OPEN':
                    continue

                opening_action = 'BUY' if action == 'SELL' else 'SHORT'
                pending = pending_by_symbol.get(symbol, [])
                opening_index = next(
                    (
                        index
                        for index in range(len(pending) - 1, -1, -1)
                        if (pending[index].get('type') or '').upper() == opening_action
                    ),
                    None,
                )
                opening = pending.pop(opening_index) if opening_index is not None else {}

                try:
                    closed_trades.append({
                        'time': row.get('time') or '',
                        'symbol': symbol,
                        'type': opening_action,
                        'entry_price': float(opening.get('price') or 0),
                        'exit_price': float(row.get('price') or 0),
                        'usdt_value': float(row.get('usdt_value') or 0),
                        'profit': float(row.get('profit') or 0),
                        'status': row.get('status') or 'CLOSED',
                        'pattern': row.get('pattern') or opening.get('pattern') or 'TURTLE',
                    })
                except (TypeError, ValueError) as exc:
                    print(
                        f'❌ Registro inválido no histórico de operações, linha '
                        f'{row_number}: {exc}',
                        flush=True,
                    )
    except OSError as exc:
        print(f'❌ Não foi possível ler o histórico de operações: {exc}', flush=True)

    return closed_trades


def write_analysis_log(entry):
    ensure_analysis_log_file()
    if isinstance(entry, dict):
        row = entry
    else:
        row = {'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'symbol': 'UNKNOWN', 'status': 'N/A'}
    with open(ANALYSIS_LOG_CSV, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=ANALYSIS_LOG_FIELDS)
        cleaned = {field: row.get(field, '') for field in ANALYSIS_LOG_FIELDS}
        writer.writerow(cleaned)


def ensure_analysis_log_file():
    if not os.path.exists(ANALYSIS_LOG_CSV):
        with open(ANALYSIS_LOG_CSV, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=ANALYSIS_LOG_FIELDS)
            writer.writeheader()

def run_bot():
    global last_bot_error
    
    print('🚀 Iniciando Bot de Trading (Estratégia Turtle de Richard Dennis)')
    print(f'💰 Capital inicial: ${INITIAL_CAPITAL}')
    print(
        f'📊 Margem por entrada: 100% do saldo disponível '
        f'| Alavancagem: {LEVERAGE}x'
    )
    print('='*60)

    while not bot_stop_event.is_set():
        try:
            symbols = get_scan_symbols()
            symbols = list(current_positions) + [
                symbol for symbol in symbols if symbol not in current_positions
            ]
            had_open_position = bool(current_positions)
            buy_candidates = []
            last_bot_error = None

            for symbol, analysis in analyze_symbols_concurrently(symbols, bot_stop_event):
                if analysis is None:
                    continue
                signal, pattern_info, df, price = analysis

                position = current_positions.get(symbol)
                if position is None:
                    if (
                        not had_open_position
                        and signal == 'COMPRA'
                        and pattern_info is not None
                    ):
                        buy_candidates.append((symbol, analysis))
                else:
                    open_trade = next((t for t in trade_history
                                       if t['symbol'] == symbol and t['status'] == 'OPEN'), None)
                    if not open_trade:
                        print(
                            f'❌ Posição {symbol} sem operação aberta correspondente; '
                            'removida do acompanhamento.',
                            flush=True,
                        )
                        current_positions.pop(symbol, None)
                        continue

                    stop_loss = open_trade['stop_loss']
                    trade_type = open_trade['type']

                    if trade_type == 'BUY' and price <= stop_loss:
                        print(f'🛑 STOP LOSS ACIONADO ({symbol})')
                        execute_sell(symbol, price, reason='STOP_LOSS')
                        continue

                    lowest_10 = df['Lowest_Low'].iloc[-1]
                    if trade_type == 'BUY' and price < lowest_10:
                        print(f'🎯 SAÍDA TURTLE: {symbol} perdeu a mínima de 10 candles.')
                        execute_sell(symbol, price, reason='TURTLE_LOW_EXIT')
                        continue

                    current_profit = price - open_trade['price']
                    if current_profit > 0:
                        print(f'📈 {symbol} em curso. Lucro atual: ${current_profit:.2f}. Mínima de 10c: ${lowest_10:.4f}')

            if (
                not had_open_position
                and not bot_stop_event.is_set()
                and len(current_positions) < MAX_OPEN_POSITIONS
            ):
                for symbol, _ in rank_buy_candidates(buy_candidates):
                    if bot_stop_event.is_set() or len(current_positions) >= MAX_OPEN_POSITIONS:
                        break
                    fresh_analysis = analyze_market_symbol(symbol)
                    if fresh_analysis is None or fresh_analysis[0] != 'COMPRA':
                        print(
                            f'⏭️ Entrada de {symbol} descartada na revalidação; '
                            'tentando o próximo melhor sinal.',
                            flush=True,
                        )
                        continue
                    _, pattern_info, df, price = fresh_analysis
                    print(
                        f'🟢 Melhor entrada selecionada: {symbol} | '
                        f'volume {pattern_info["volume_ratio"]:.2f}x | '
                        f'rompimento {pattern_info["breakout_atr"]:.2f} ATR.',
                        flush=True,
                    )
                    execute_buy(symbol, price, pattern_info, df)
                    if symbol in current_positions:
                        break
            
            bot_stop_event.wait(SCAN_INTERVAL_SECONDS)
        
        except KeyboardInterrupt:
            break
        except Exception as exc:
            last_bot_error = str(exc)
            print(f'❌ Erro no ciclo do bot: {exc}', flush=True)
            bot_stop_event.wait(30)


def start_bot():
    global bot_thread, last_bot_error
    with bot_state_lock:
        if bot_thread and bot_thread.is_alive():
            return False

    if MODE == 'LIVE':
        positions, source = get_dashboard_open_positions()
        if source != 'BYBIT':
            last_bot_error = f'Bot LIVE bloqueado: não foi possível validar posições abertas ({source}).'
            return False
        synchronized, sync_error = synchronize_live_position(positions)
        if not synchronized:
            last_bot_error = sync_error
            return False

    with bot_state_lock:
        if bot_thread and bot_thread.is_alive():
            return False
        last_bot_error = None
        read_only_scan_stop_event.set()
        bot_stop_event.clear()
        bot_thread = threading.Thread(target=run_bot, name='trading-bot', daemon=True)
        bot_thread.start()
        return True


def synchronize_live_position(positions):
    positions_by_symbol = {}
    for position in positions:
        symbol = position['symbol']
        if symbol in positions_by_symbol:
            return False, f'Bot LIVE não iniciado: posição duplicada para {symbol}.'
        if position['side'] != 'LONG':
            return False, (
                f"Bot LIVE não iniciado: a posição {symbol} é SHORT, "
                'mas o robô ainda não implementa gerenciamento de posições SHORT.'
            )

        quantity = float(position['quantity'])
        entry_price = float(position['entry_price'])
        leverage = float(position.get('leverage') or LEVERAGE)
        if not symbol or symbol == 'UNKNOWN' or quantity <= 0 or entry_price <= 0 or leverage <= 0:
            return False, 'Bot LIVE não iniciado: os dados da posição Bybit são inválidos.'

        positions_by_symbol[symbol] = position

    for trade in trade_history:
        symbol = trade.get('symbol')
        if (
            trade.get('type') == 'BUY'
            and trade.get('status') == 'OPEN'
            and symbol not in positions_by_symbol
        ):
            trade['status'] = 'CLOSED_EXTERNALLY'
            stale_asset = symbol.replace(BASE_ASSET, '')
            portfolio.pop(stale_asset, None)

    current_positions.clear()
    for symbol, position in positions_by_symbol.items():
        quantity = float(position['quantity'])
        entry_price = float(position['entry_price'])
        leverage = float(position.get('leverage') or LEVERAGE)
        tracked_trade = next(
            (
                trade for trade in trade_history
                if trade.get('symbol') == symbol
                and trade.get('type') == 'BUY'
                and trade.get('status') == 'OPEN'
            ),
            None,
        )
        if tracked_trade:
            tracked_trade.update({
                'price': entry_price,
                'quantity': quantity,
                'usdt_value': quantity * entry_price,
                'stop_loss': float(position.get('stop_loss') or 0),
                'take_profit': float(position.get('take_profit') or 0),
            })
        else:
            tracked_trade = {
                'time': datetime.now(timezone.utc).isoformat(),
                'symbol': symbol,
                'type': 'BUY',
                'price': entry_price,
                'quantity': quantity,
                'usdt_value': quantity * entry_price,
                'stop_loss': float(position.get('stop_loss') or 0),
                'take_profit': float(position.get('take_profit') or 0),
                'pattern': 'BYBIT_SYNC',
                'profit': 0.0,
                'status': 'OPEN',
            }
            try:
                write_trade_csv(tracked_trade)
            except OSError as exc:
                return False, (
                    f'Bot LIVE não iniciado: não foi possível registrar a posição '
                    f'{symbol} sincronizada ({exc}).'
                )
            trade_history.append(tracked_trade)

        portfolio[symbol.replace(BASE_ASSET, '')] = quantity
        current_positions[symbol] = {'type': 'LONG', 'leverage': leverage}
        print(
            f'🔄 Posição Bybit sincronizada: {symbol} LONG | '
            f'Quantidade: {quantity:g} | Entrada: {entry_price:g}',
            flush=True,
        )

    if not positions_by_symbol:
        print('🔄 Sincronização Bybit concluída: nenhuma posição aberta.', flush=True)
    return True, None


def stop_bot():
    if not bot_thread or not bot_thread.is_alive(): return False
    bot_stop_event.set()
    return True

def is_bot_running():
    return bool(bot_thread and bot_thread.is_alive())

def run_read_only_market_scan():
    while not read_only_scan_stop_event.is_set():
        try:
            symbols = get_scan_symbols()
            for _ in analyze_symbols_concurrently(symbols, read_only_scan_stop_event):
                pass
            read_only_scan_stop_event.wait(SCAN_INTERVAL_SECONDS)
        except Exception as exc:
            print(f'❌ Erro no scanner somente leitura: {exc}', flush=True)
            read_only_scan_stop_event.wait(30)


def ensure_read_only_market_scan():
    if read_only_scan_stop_event.is_set():
        read_only_scan_stop_event.clear()
    if not any(thread.name == 'read-only-market-scan' and thread.is_alive() for thread in threading.enumerate()):
        scan_thread = threading.Thread(target=run_read_only_market_scan, name='read-only-market-scan', daemon=True)
        scan_thread.start()


def get_dashboard_open_positions():
    if MODE == 'LIVE':
        try:
            response = client.get_positions(category=BYBIT_CATEGORY, settleCoin=BASE_ASSET)
            if response.get('retCode') != 0:
                raise RuntimeError(response.get('retMsg', 'resposta inválida'))
            positions = []
            for position in response.get('result', {}).get('list', []):
                quantity = float(position.get('size') or 0)
                if quantity <= 0:
                    continue
                positions.append({
                    'symbol': position.get('symbol', 'UNKNOWN'),
                    'side': 'LONG' if position.get('side') == 'Buy' else 'SHORT',
                    'pattern': 'BYBIT',
                    'quantity': quantity,
                    'leverage': float(position.get('leverage') or LEVERAGE),
                    'entry_price': float(position.get('avgPrice') or position.get('entryPrice') or 0),
                    'stop_loss': float(position.get('stopLoss') or 0),
                    'take_profit': float(position.get('takeProfit') or 0),
                    'mark_price': float(position.get('markPrice') or 0),
                    'pnl': float(position.get('unrealisedPnl') or 0),
                })
            return positions, 'BYBIT'
        except Exception as exc:
            message = f'BYBIT indisponível: {exc}'
            print(f'❌ Erro ao consultar posições abertas: {exc}', flush=True)
            return [], message

    open_positions = []
    for trade in trade_history:
        if trade.get('status') == 'OPEN':
            trade_type = trade.get('type', 'BUY')
            open_positions.append({
                'symbol': trade.get('symbol', 'UNKNOWN'),
                'side': 'LONG' if trade_type == 'BUY' else 'SHORT',
                'pattern': trade.get('pattern', 'TURTLE'),
                'quantity': trade.get('quantity', 0),
                'entry_price': trade.get('price', 0),
                'stop_loss': trade.get('stop_loss', 0),
                'take_profit': trade.get('take_profit', 0),
                'mark_price': trade.get('price', 0),
                'pnl': trade.get('profit', 0),
            })
    return open_positions, 'SIMULADO'


def get_trade_chart_data():
    chart_points = []
    for trade in trade_history:
        if trade.get('status') == 'OPEN':
            continue
        chart_points.append({
            'symbol': trade.get('symbol', 'UNKNOWN'),
            'time': trade.get('time', datetime.now(timezone.utc).isoformat()),
            'profit': trade.get('profit', 0),
        })
    return chart_points


def get_open_position_chart_data():
    positions, source = get_dashboard_open_positions()
    if source.startswith('BYBIT indisponível:'):
        return [], source

    charts = []
    for position in positions:
        symbol = position.get('symbol', 'UNKNOWN')
        candles = get_historical_data(symbol, TICKER_INTERVAL, 60)
        chart_candles = [
            {
                'time': timestamp.isoformat(),
                'open': float(candle['Open']),
                'high': float(candle['High']),
                'low': float(candle['Low']),
                'close': float(candle['Close']),
                'volume': float(candle['Volume']),
            }
            for timestamp, candle in candles.iterrows()
        ]
        charts.append({
            'symbol': symbol,
            'side': position.get('side', 'LONG'),
            'entry_price': float(position.get('entry_price') or 0),
            'candles': chart_candles,
        })
    return charts, source


def queue_tradingview_alert(data):
    if not isinstance(data, dict):
        return False, 400, 'Envie um objeto JSON válido.'
    symbol = (data.get('symbol') or data.get('ticker') or 'UNKNOWN').upper()
    timestamp = datetime.now(timezone.utc).isoformat()
    scan_status[symbol] = {
        'symbol': symbol,
        'status': 'ALERTA',
        'updated_at': timestamp,
        'buy_analysis': {
            'score': 0,
            'price_change_pct': 0,
            'volume_5m_vs_avg20': 0,
            'volume_latest_5m': 0,
            'risk_status': 'ALERTA TRADINGVIEW',
        },
    }
    return True, 202, f'Alerta TradingView recebido para {symbol}.'


@app.route('/')
@login_required
def dashboard():
    balance, balance_source = get_cached_dashboard_wallet_balance()
    open_positions, positions_source = get_dashboard_open_positions()
    closed_trades = get_closed_trade_history()
    closed_trades.reverse()
    open_trade_rows = []
    for position in open_positions:
        symbol = position.get('symbol', 'UNKNOWN')
        opening_trade = next(
            (
                trade for trade in reversed(trade_history)
                if trade.get('symbol') == symbol
                and trade.get('type') in {'BUY', 'SHORT'}
                and trade.get('status') == 'OPEN'
            ),
            {},
        )
        entry_price = float(position.get('entry_price') or 0)
        quantity = float(position.get('quantity') or 0)
        open_trade_rows.append({
            'time': opening_trade.get('time', ''),
            'symbol': symbol,
            'type': 'BUY' if position.get('side') == 'LONG' else 'SHORT',
            'entry_price': entry_price,
            'exit_price': None,
            'usdt_value': quantity * entry_price,
            'profit': float(position.get('pnl') or 0),
            'status': 'OPEN',
        })
    history_trades = open_trade_rows + closed_trades
    total_profit = sum(float(t.get('profit', 0) or 0) for t in closed_trades)
    total_trades = len(closed_trades)
    winning_trades = sum(1 for t in closed_trades if float(t.get('profit', 0) or 0) > 0)
    losing_trades = total_trades - winning_trades
    win_rate = (winning_trades / total_trades * 100) if total_trades else 0.0
    gross_wins = sum(max(float(t.get('profit', 0) or 0), 0) for t in closed_trades)
    gross_losses = sum(abs(min(float(t.get('profit', 0) or 0), 0)) for t in closed_trades)
    profit_factor = (gross_wins / max(gross_losses, 1e-9)) if closed_trades else 0.0
    expectancy = (total_profit / total_trades) if total_trades else 0.0
    total_unrealized = sum(float(item.get('pnl', 0) or 0) for item in open_positions)
    total_return = 0.0
    entry_margin_amount = calculate_entry_margin(balance, position_size_pct=1.0) if balance is not None else None
    entry_amount = entry_margin_amount * LEVERAGE if entry_margin_amount is not None else None
    capital_history = [{'time': item.get('time', datetime.now(timezone.utc).isoformat()), 'capital': float(item.get('usdt_value', 0) or 0)} for item in trade_history if item.get('status') == 'CLOSED']
    if not capital_history:
        capital_history = [{'time': datetime.now(timezone.utc).isoformat(), 'capital': float(portfolio.get(BASE_ASSET, INITIAL_CAPITAL))}]

    return render_template("index.html",
        initial_capital=INITIAL_CAPITAL,
        current_capital=portfolio.get(BASE_ASSET, 0),
        available_wallet_balance=balance,
        wallet_balance_source=balance_source,
        wallet_balance=balance if balance is not None else portfolio.get(BASE_ASSET, INITIAL_CAPITAL),
        entry_amount=entry_amount,
        entry_margin_amount=entry_margin_amount,
        total_profit=total_profit,
        total_losses=gross_losses,
        total_wins=gross_wins,
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        total_return=total_return,
        max_open_positions=MAX_OPEN_POSITIONS,
        bot_running=is_bot_running(),
        leverage=LEVERAGE,
        position_size_pct=100,
        open_positions=open_positions,
        history_trades=history_trades,
        closed_trades=closed_trades,
        capital_history=capital_history,
        mode_label=MODE,
        cooldown_active=False,
        trading_halted=False,
        consecutive_losses=0,
        partial_realized=0,
        win_rate=win_rate,
        profit_factor=profit_factor,
        avg_win=(gross_wins / winning_trades) if winning_trades else 0.0,
        avg_loss=(gross_losses / losing_trades) if losing_trades else 0.0,
        max_drawdown_pct=0.0,
        positions_source=positions_source,
        total_realized=total_profit,
        expectancy=expectancy,
        total_unrealized=total_unrealized,
    )

if __name__ == '__main__':
    sys.modules.setdefault('app', sys.modules[__name__])
    print('🔎 Iniciando análise de mercado somente leitura no terminal...', flush=True)
    ensure_read_only_market_scan()
    app.run(host='127.0.0.1', port=5000, debug=False, use_reloader=False)