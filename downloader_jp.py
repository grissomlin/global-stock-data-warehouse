# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, subprocess, io
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")

IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

CACHE_DIR = os.path.join(BASE_DIR, "cache_jp")
DATA_EXPIRY_SECONDS = 86400

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 6

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg_install_name, import_name):
    """確保必要套件已安裝"""
    try:
        __import__(import_name)
    except ImportError:
        log(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ========== 2. 核心輔助函式 ==========

def insert_or_replace(table, conn, keys, data_iter):
    """防止重複寫入的核心 SQL 邏輯"""
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

def init_db():
    """初始化資料庫結構，並自動檢查/新增 market 欄位"""
    conn = sqlite3.connect(DB_PATH)
    try:
        # 價格表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        # 資訊表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, 
                            name TEXT, 
                            sector TEXT, 
                            updated_at TEXT)''')
        
        # 💡 檢查是否需要新增 market 欄位 (針對舊資料庫升級)
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 偵測到舊版資料庫，正在新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
            
        conn.commit()
    finally:
        conn.close()

def get_jp_stock_list():
    """獲取日股清單並同步更新名稱、產業別與市場別"""
    log(f"📡 獲取日股名單 (含產業與市場別)... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    try:
        # 強制更新 CSV，確保拿到最新資料
        tse.download_csv(destination=tse.csv_file_path, overwrite=True)
        df = pd.read_csv(tse.csv_file_path)
        
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Issues'] if c in df.columns), None)
        sector_col = next((c for c in ['33業種区分', 'Sector', 'industry'] if c in df.columns), None) # 💡 這裡就是產業別
        
        # 判斷必要欄位是否存在
        if not all([code_col, name_col, sector_col]):
            log("❌ 日股清單 CSV 缺少必要的欄位 (代碼/名稱/產業)。")
            return [] # 返回空列表

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            # 過濾掉非股票代碼的資料 (例如標題或說明)
            if len(raw_code) >= 4 and raw_code[:4].isdigit():
                symbol = f"{raw_code[:4]}.T" # 日股的 Yahoo Finance 後綴通常是 .T
                name = str(row[name_col]).strip()
                sector = str(row[sector_col]).strip() if pd.notna(row[sector_col]) else "Unknown"
                market = "TSE" # 💡 日股統一標記為 TSE

                conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) VALUES (?, ?, ?, ?, ?)",
                             (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
                
        conn.commit()
        conn.close()
        log(f"✅ 成功同步日股清單: {len(stock_list)} 檔 (含產業別)")
        return stock_list
    except Exception as e:
        log(f"❌ 日股清單獲取失敗: {e}")
        return [] # 失敗時返回空列表

# ========== 3. 核心下載/重試邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    # 日股沒有本地快取功能 (tokyo-stock-exchange 已經做了一次文件快取)
    start_date = "2020-01-01" if mode == 'hot' else "1999-01-01"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            wait_time = random.uniform(1.5, 3.0) if IS_GITHUB_ACTIONS else random.uniform(0.1, 0.4)
            time.sleep(wait_time)
            
            df = yf.download(symbol, start=start_date, progress=False, timeout=25)
            
            if df.empty:
                return {"symbol": symbol, "status": "empty"}
                
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            if isinstance(df.columns, pd.MultiIndex): # 處理 yfinance 可能的 MultiIndex 欄位
                df.columns = df.columns.get_level_values(0)

            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda table, conn, keys, data_iter: 
                            conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            conn.close()
            
            return {"symbol": symbol, "status": "success"}
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 12))
                continue
            return {"symbol": symbol, "status": "error"}

# ========== 4. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_jp_stock_list()
    if not items:
        log("❌ 無法取得日股名單，任務終止。")
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行日股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0} # 日股無本地快取，所以 cache 統計不適用
    fail_list = []
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"JP處理中({mode})")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s if s in stats else 'error'] += 1
            if s == "error":
                fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()

    has_changed = stats['success'] > 0 # 有新的成功下載才算有變動
    
    # 無論有無變動，GitHub Actions 上傳前都應做 VACUUM
    log("🧹 執行資料庫優化 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    
    return {
        "success": stats['success'],
        "cache": 0, # 日股 downloader 沒有實現本地快取機制，所以回傳 0
        "error": stats['error'],
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": has_changed
    }

if __name__ == "__main__":
    run_sync(mode='hot')
