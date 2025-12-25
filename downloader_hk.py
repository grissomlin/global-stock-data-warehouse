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

# 💡 自動判斷環境：GitHub Actions 會自動帶入此變數
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定
CACHE_DIR = os.path.join(BASE_DIR, "cache_hk")
DATA_EXPIRY_SECONDS = 86400  # 本機快取效期：24小時

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能設定
MAX_WORKERS = 2 if IS_GITHUB_ACTIONS else 2 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 輔助函式 ==========

def insert_or_replace(table, conn, keys, data_iter):
    """
    💡 核心功能：防止資料重複
    當 (date, symbol) 已經存在時，會自動替換為最新數據
    """
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

def to_symbol_yf(code: str) -> str:
    digits = re.sub(r"\D", "", str(code or ""))
    return f"{digits[-4:].zfill(4)}.HK" if digits else ""

def classify_security(name: str) -> str:
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    return "Exclude" if any(kw in n for kw in bad_kw) else "Common Stock"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        # 💡 這裡設定 PRIMARY KEY 是防重複的第一道防線
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, updated_at TEXT)''')
        conn.commit()
    finally:
        conn.close()

# ========== 3. 核心下載/讀取邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    # --- 🟢 分流點：本地環境優先讀取 CSV ---
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            try:
                df_cache = pd.read_csv(csv_path)
                conn = sqlite3.connect(DB_PATH, timeout=30)
                # 使用覆蓋模式寫入資料庫
                df_cache.to_sql('stock_prices', conn, if_exists='append', index=False, 
                               method=insert_or_replace)
                conn.close()
                return {"symbol": symbol, "status": "cache"}
            except:
                pass # CSV 損壞則嘗試重新下載

    # --- 🔵 分流點：下載新數據 (GitHub 或無快取) ---
    try:
        time.sleep(random.uniform(1.5, 3.5))
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

        # 1. 存成本機 CSV
        if not IS_GITHUB_ACTIONS:
            df_final.to_csv(csv_path, index=False)

        # 2. 存入 SQL (使用 INSERT OR REPLACE)
        conn = sqlite3.connect(DB_PATH, timeout=30)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                       method=insert_or_replace)
        conn.close()
        
        return {"symbol": symbol, "status": "success"}
    except Exception:
        return {"symbol": symbol, "status": "error"}

# ========== 4. 主流程 ==========

def get_hk_stock_list():
    log(f"📡 正在獲取港股清單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        hdr_idx = 0
        for i in range(20):
            if "stock code" in str(df_raw.iloc[i]).lower():
                hdr_idx = i
                break
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].tolist()
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        for _, row in df.iterrows():
            name = str(row.get("Short Name", ""))
            if classify_security(name) == "Common Stock":
                code = row.get("Stock Code", "")
                symbol = to_symbol_yf(code)
                if symbol:
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                                 (symbol, name, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        conn.commit()
        conn.close()
        return stock_list
    except Exception as e:
        log(f"❌ 清單失敗: {e}")
        return []

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    items = get_hk_stock_list()
    if not items: return {"fail_list": [], "success": 0}

    log(f"🚀 開始任務 | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        pbar = tqdm(total=len(items), desc="HK處理中")
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error": fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 新增: {stats['success']} | ⚡ 快取: {stats['cache']} | ❌ 錯誤: {stats['error']}")
    
    return {
        "success": stats['success'] + stats['cache'],
        "fail_list": fail_list
    }

if __name__ == "__main__":
    run_sync(mode='hot')
