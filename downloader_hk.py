# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 下載設定：港股建議低並發以確保成功率
MAX_WORKERS = 2 if IS_GITHUB_ACTIONS else 4

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫與清單獲取 (修改版) ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

def normalize_code5_any(s: str) -> str:
    """抓出字串中的數字，取最後5碼並左側補零；用於清單、檔案命名"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-5:].zfill(5) if digits and digits.isdigit() else ""

def normalize_code4_any(s: str) -> str:
    """抓出字串中的數字，取最後4碼並左側補零；專用於生成 Yahoo Finance 符號"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-4:].zfill(4) if digits and digits.isdigit() else ""

def to_yahoo_symbol(code: str) -> str:
    """HK YFinance Symbol: <code4>.HK (兼容 Yahoo 習慣)"""
    return f"{normalize_code4_any(code)}.HK"

def download_hkex_xls(url: str) -> pd.DataFrame:
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    r = requests.get(url, headers=headers, timeout=30, verify=False)
    r.raise_for_status()
    return pd.read_excel(io.BytesIO(r.content), header=None)

def locate_header(df: pd.DataFrame):
    code_pat = re.compile(r"stock\s*code", re.I)
    name_pat = re.compile(r"english\s*stock\s*short\s*name", re.I)
    for i in range(min(20, len(df))):
        row = [str(x or "").replace('\xa0', ' ') for x in df.iloc[i].tolist()]
        if any(code_pat.search(x) for x in row) and any(name_pat.search(x) for x in row):
            return i
    return None

def parse_hkex_table(df_raw: pd.DataFrame):
    hdr_idx = locate_header(df_raw)
    if hdr_idx is None:
        raise RuntimeError(f"找不到表頭列")
    cols = df_raw.iloc[hdr_idx].tolist()
    df = df_raw.iloc[hdr_idx+1:].copy()
    df.columns = cols
    df = df.dropna(how="all")
    return df

def clean_to_equities(df: pd.DataFrame):
    # 尋找正確的欄位名稱
    col_code = next((c for c in df.columns if re.search(r"stock\s*code", str(c), re.I)), None)
    col_name = next((c for c in df.columns if re.search(r"english\s*stock\s*short\s*name", str(c), re.I)), None)
    
    if not col_code or not col_name:
        raise RuntimeError(f"無法辨識欄位，columns={list(df.columns)}")
    
    df = df[[col_code, col_name]].copy()
    
    # 使用5位數正規化
    df[col_code] = df[col_code].astype(str).map(normalize_code5_any)
    
    # 剔除衍生品/基金/債等
    bad_kw = r"CBBC|WARRANT|RIGHTS|ETF|ETN|REIT|BOND|NOTE|PREF|PREFERENCE|TRUST|FUND|DERIV|牛熊|權證|輪證|房託|債"
    df = df[~df[col_name].astype(str).str.contains(bad_kw, case=False, regex=True, na=False)]
    
    # 只保留5位數的正股代碼
    df = df[df[col_code].str.fullmatch(r"\d{5}")]
    
    # 排除特定registrar機構
    bad_names = [
        "Pilare Ltd.",
        "The Bank of New York Mellon SA/NV, Luxembourg Branch",
        "Deutsche Bank AG, Singapore Branch",
        "BNP Paribas Securities Services S.C.A., Zweigniederlassung Frankfurt am Main"
    ]
    df = df[~df[col_name].astype(str).isin(bad_names)]
    
    # 排除code < 00100的小數字（防呆更多機構）
    df[col_code] = pd.to_numeric(df[col_code], errors='coerce')
    df = df[df[col_code] >= 100]
    df[col_code] = df[col_code].astype(int).astype(str).str.zfill(5)
    
    df = df.drop_duplicates(subset=[col_code]).reset_index(drop=True)
    return df.rename(columns={col_code:"code", col_name:"name"})

def get_hk_stock_list():
    """獲取港股清單 - 使用Colab版本的可靠邏輯"""
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    log(f"📡 正在從港交所同步最新名單...")
    try:
        df_raw = download_hkex_xls(url)
        df_tbl = parse_hkex_table(df_raw)
        df_eq = clean_to_equities(df_tbl)
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df_eq.iterrows():
            code_5d = row["code"]  # 5位數代碼
            name = row["name"]
            
            # 轉換為Yahoo格式（4位數）
            yahoo_symbol = to_yahoo_symbol(code_5d)
            
            # 檢查是否為有效的港股代碼（排除太小的數字）
            if code_5d and int(code_5d) >= 100:
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (yahoo_symbol, name, "Unknown", "HKEX", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((yahoo_symbol, name, code_5d))
        
        conn.commit()
        conn.close()
        
        log(f"✅ 港股清單同步成功: {len(stock_list)} 檔")
        return [(symbol, name) for symbol, name, _ in stock_list]
        
    except Exception as e:
        log(f"⚠️ 名單抓取失敗: {e}，使用保底清單")
        # 保底清單也使用正確的Yahoo格式
        return [
            ("0700.HK", "TENCENT"), 
            ("0005.HK", "HSBC"), 
            ("0941.HK", "CHINA MOBILE"),
            ("0001.HK", "CK HUTCHISON"),
            ("0011.HK", "HANG SENG BANK")
        ]

# ========== 3. 單檔下載邏輯 (增強版) ==========

def safe_history(symbol: str, start: str, end: str, interval="1d", max_retries=3, base_delay=1.0):
    """嘗試用不同period抓取歷史資料"""
    periods = ["max", "10y", "5y", "2y", "1y"]
    for i in range(max_retries):
        try:
            tk = yf.Ticker(symbol)
            if i < len(periods):
                p = periods[i]
                df = tk.history(period=p, interval=interval, auto_adjust=True)
            else:
                df = tk.history(start=start, end=end, interval=interval, auto_adjust=True)
            
            if df is not None and not df.empty:
                return df
            time.sleep(base_delay + 0.5*i + random.uniform(0, 0.7))
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                return None  # 股票可能已下市
            time.sleep(base_delay + 0.5*i + random.uniform(0, 1.0))
    return None

def standardize_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """統一欄位名稱、處理日期時區"""
    if df is None or df.empty:
        return pd.DataFrame()
    
    df = df.reset_index()
    if 'Date' not in df.columns:
        first_col = df.columns[0]
        if str(first_col).lower().startswith("date"):
            df.rename(columns={first_col: 'Date'}, inplace=True)
        else:
            return pd.DataFrame()
    
    df['date'] = pd.to_datetime(df['Date'], errors='coerce', utc=True)
    
    # 處理時區
    try:
        df['date'] = df['date'].dt.tz_convert(None)
    except Exception:
        try:
            df['date'] = df['date'].dt.tz_localize(None)
        except Exception:
            pass
    
    # 標準化欄位名稱
    col_mapping = {
        'Open': 'open', 'High': 'high', 'Low': 'low', 
        'Close': 'close', 'Volume': 'volume'
    }
    
    for old, new in col_mapping.items():
        if old in df.columns:
            df[new] = pd.to_numeric(df[old], errors='coerce')
    
    required = ['date', 'open', 'high', 'low', 'close', 'volume']
    if not all(col in df.columns for col in required):
        return pd.DataFrame()
    
    df = df.dropna(subset=['date'])
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    df = df.dropna(subset=['open', 'high', 'low', 'close', 'volume'])
    df = df[df['volume'] >= 0]
    
    # 格式化日期
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    df['symbol'] = symbol
    
    return df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

def download_one(symbol, name, mode):
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 檢查是否為有效的股票代碼（過濾已下市股票）
    if "09988" in symbol:  # 阿里巴巴已改為9988
        symbol = "9988.HK"
    elif "00005" in symbol:  # HSBC
        pass  # 保持不變
    
    for attempt in range(3):
        try:
            # 增加隨機延遲，防止 429 或 404 錯誤
            time.sleep(random.uniform(2.5, 4.5) if IS_GITHUB_ACTIONS else random.uniform(0.5, 1.5))
            
            # 使用安全的歷史數據獲取
            hist = safe_history(symbol, start_date, end_date, "1d")
            
            if hist is None or hist.empty:
                log(f"⚠️ {symbol}: 無數據，可能已下市")
                continue
            
            # 標準化數據
            df_final = standardize_df(hist, symbol)
            
            if df_final.empty:
                log(f"⚠️ {symbol}: 數據標準化失敗")
                continue
            
            # 寫入資料庫
            conn = sqlite3.connect(DB_PATH, timeout=60)
            
            # 使用批量插入
            data_to_insert = df_final.to_dict('records')
            placeholders = ', '.join(['?'] * len(data_to_insert[0]))
            columns = ', '.join(df_final.columns)
            
            conn.executemany(
                f"INSERT OR REPLACE INTO stock_prices ({columns}) VALUES ({placeholders})",
                [tuple(row.values()) for row in data_to_insert]
            )
            
            conn.close()
            return True
            
        except Exception as e:
            if attempt == 2:
                log(f"❌ {symbol} 下載失敗: {e}")
            time.sleep(5)  # 錯誤後冷靜 5 秒
    
    return False

# ========== 4. 預篩機制 (新增) ==========

def quick_symbol_check(symbol: str) -> bool:
    """快速檢查股票是否有效"""
    try:
        tk = yf.Ticker(symbol)
        # 嘗試獲取少量數據檢查
        df = tk.history(period="5d", interval="1d", auto_adjust=True)
        return df is not None and not df.empty
    except Exception:
        return False

def prefilter_stocks(items):
    """預先過濾可能無效的股票"""
    log("🔍 正在預篩股票清單...")
    valid_items = []
    
    for symbol, name in items:
        if quick_symbol_check(symbol):
            valid_items.append((symbol, name))
        else:
            log(f"⏭️  跳過無效股票: {symbol} ({name})")
    
    log(f"✅ 預篩完成: {len(valid_items)}/{len(items)} 檔有效")
    return valid_items

# ========== 5. 主流程 (修改版) ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    # 獲取股票清單
    items = get_hk_stock_list()
    if not items:
        log("❌ 無法獲取股票清單")
        return {"success": 0, "has_changed": False}
    
    log(f"📊 獲取到 {len(items)} 檔股票")
    
    # 預篩股票
    valid_items = prefilter_stocks(items)
    
    if not valid_items:
        log("❌ 沒有有效的股票可下載")
        return {"success": 0, "has_changed": False}
    
    log(f"🚀 開始同步港股 | 執行緒: {MAX_WORKERS} | 有效股票: {len(valid_items)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, it[0], it[1], mode): it[0] for it in valid_items}
        pbar = tqdm(total=len(valid_items), desc="HK同步中")
        
        for f in as_completed(futures):
            if f.result():
                total_success += 1
            pbar.update(1)
        pbar.close()

    log("🧹 執行 VACUUM...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！成功: {total_success}/{len(valid_items)} 檔 | 費時: {duration:.1f} 分鐘")
    
    # 計算覆蓋率
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices")
    actual_count = cursor.fetchone()[0]
    conn.close()
    
    log(f"📈 資料庫中現有股票數: {actual_count} 檔")
    
    return {
        "success": total_success,
        "error": len(valid_items) - total_success,
        "has_changed": total_success > 0,
        "coverage": f"{(total_success/len(items)*100):.1f}%"
    }

if __name__ == "__main__":
    result = run_sync(mode='hot')
    print(f"\n🏁 最終結果:")
    print(f"   成功下載: {result['success']} 檔")
    print(f"   下載失敗: {result['error']} 檔")
    print(f"   數據覆蓋率: {result['coverage']}")
