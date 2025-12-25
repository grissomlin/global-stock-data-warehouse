# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, subprocess, io
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

CACHE_DIR = os.path.join(BASE_DIR, "cache_kr")
if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能調優：韓股連線較嚴格，GitHub 模式降至 3 執行緒
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 4 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    """確保必要套件已安裝"""
    try:
        __import__(pkg.replace('-', '_'))
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

# ========== 2. 資料庫初始化 (含自動升級邏輯) ==========

def init_db():
    """初始化資料庫並自動檢查/新增 market 欄位"""
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
        
        # 💡 自動升級：檢查並新增 market 欄位 (針對舊資料庫)
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 偵測到舊版 KR 資料庫，正在新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取韓股清單 (含市場別) ==========

def get_kr_stock_list():
    """從 FDR 同步韓股清單，並區分 KOSPI/KOSDAQ"""
    ensure_pkg("finance-datareader")
    import FinanceDataReader as fdr
    
    log(f"📡 正在從 FDR 同步韓股清單 (含 KOSPI/KOSDAQ 市場別)...")
    try:
        # FDR 的 StockListing('KRX') 同時包含兩大交易所
        df = fdr.StockListing('KRX') 
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            code = str(row['Code']).strip()
            market_type = str(row['Market']).upper() # KOSPI 或 KOSDAQ
            
            # 💡 根據市場別決定 Yahoo 代號後綴 (.KS / .KQ)
            suffix = ".KS" if market_type == 'KOSPI' else ".KQ"
            symbol = f"{code}{suffix}"
            name = row['Name']
            sector = row.get('Sector', 'Unknown')
            
            # 💡 存入資料庫時包含市場別
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market_type, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 成功同步韓股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 韓股清單獲取失敗: {e}")
        return []

# ========== 4. 核心下載邏輯 (加強穩定性) ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-03"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 💡 長延遲 Jitter (1.8 ~ 3.8 秒) 防止封鎖
            wait_time = random.uniform(1.8, 3.8) if IS_GITHUB_ACTIONS else random.uniform(0.2, 0.5)
            time.sleep(wait_time)
            
            tk = yf.Ticker(symbol)
            # 使用 auto_adjust=True 處理除權息
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # 處理 MultiIndex 欄位
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda table, conn, keys, data_iter: 
                            conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            conn.close()
            
            return {"symbol": symbol, "status": "success"}
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(6, 15))
                continue
            return {"symbol": symbol, "status": "error"}

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行韓股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="KR處理中"):
            res = f.result()
            s = res.get("status", "error")
            stats[s if s in stats else 'error'] += 1
            if s == "error": fail_list.append(res.get("symbol"))

    # 優化資料庫
    log("🧹 執行資料庫優化 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    
    return {
        "success": stats['success'],
        "error": stats['error'],
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": stats['success'] > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
