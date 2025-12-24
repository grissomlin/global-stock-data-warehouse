# -*- coding: utf-8 -*-
import os, io, re, time, random, requests, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")

# 💡 自動判斷是否為 GitHub Actions 環境 (關鍵功能：不刪除)
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定 (本機回測專用)
CACHE_DIR = os.path.join(BASE_DIR, "cache_hk")
DATA_EXPIRY_SECONDS = 3600  # 本機跑時，1小時內視為有效快取

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# ✅ 效能設定
MAX_WORKERS = 2 if IS_GITHUB_ACTIONS else 4 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (4 位數.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    if not digits: return ""
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品 (確保只抓普通股)"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

def init_db():
    """初始化資料庫結構"""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY,
                            name TEXT,
                            sector TEXT,
                            updated_at TEXT)''')
        conn.commit()
    finally:
        conn.close()

def get_hk_stock_list():
    """從 HKEX 獲取清單並同步寫入 stock_info"""
    log(f"📡 正在獲取最新名單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        hdr_idx = 0
        for row_i in range(20):
            row_str = "".join([str(x) for x in df_raw.iloc[row_i]]).lower()
            if "stock code" in row_str and "short name" in row_str:
                hdr_idx = row_i
                break
        
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].tolist()
        
        col_code = [c for c in df.columns if "Stock Code" in str(c)][0]
        col_name = [c for c in df.columns if "Short Name" in str(c)][0]
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            name = str(row[col_name])
            if classify_security(name) == "Common Stock":
                symbol = to_symbol_yf(row[col_code])
                if symbol:
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                                 (symbol, name, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        
        conn.commit()
        conn.close()
        log(f"✅ 成功獲取並同步港股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 港股清單抓取失敗: {e}")
        return [("0700.HK", "TENCENT"), ("9988.HK", "BABA")]

def download_one(args):
    """具備環境偵測與快取機制的單檔下載邏輯"""
    symbol, name, mode = args
    csv_path = os.path.join(CACHE_DIR, f"{symbol}.csv")
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    # --- 💡 步驟 1: 判斷是否使用 CSV 快取 (僅限本地環境) ---
    use_cache = False
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            use_cache = True

    try:
        if use_cache:
            # 本機模式：直接從 CSV 讀取，不發送網路請求
            return {"symbol": symbol, "status": "cache"}
        
        # --- 💡 步驟 2: 下載邏輯 ---
        time.sleep(random.uniform(0.8, 2.0))
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, timeout=30)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
        # 寫入資料庫 (無論哪種環境都要進 DB)
        conn = sqlite3.connect(DB_PATH)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method='multi')
        conn.close()

        # 如果是本地環境，下載完存成 CSV 方便回測
        if not IS_GITHUB_ACTIONS:
            df_final.to_csv(csv_path, index=False)
        
        return {"symbol": symbol, "status": "success"}
    except Exception:
        return {"symbol": symbol, "status": "error"}

def run_sync(mode='hot'):
    """執行同步主流程"""
    start_time = time.time()
    init_db()
    
    # 1. 獲取名單並同步名稱
    items = get_hk_stock_list()
    if not items:
        log("❌ 無法取得名單，終止任務。")
        return

    log(f"🚀 開始執行 HK ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載/讀取
    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"HK處理進度({mode})")
        
        for f in as_completed(futures):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()

    # 3. 資料庫優化
    log("🧹 正在優化資料庫空間 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 {MARKET_CODE} 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 新增: {stats['success']} | ⚡ 快取: {stats['cache']} | 📭 空資料: {stats['empty']} | ❌ 錯誤: {stats['error']}")

if __name__ == "__main__":
    run_sync(mode='hot')
