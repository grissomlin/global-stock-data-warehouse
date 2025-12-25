# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "tw-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tw_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

CACHE_DIR = os.path.join(BASE_DIR, "cache_tw")
DATA_EXPIRY_SECONDS = 86400

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能優化設定：GitHub 模式 6 執行緒，Local 10 執行緒
MAX_WORKERS = 6 if IS_GITHUB_ACTIONS else 10 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫初始化與結構維護 ==========

def init_db():
    """初始化並自動升級資料庫結構"""
    conn = sqlite3.connect(DB_PATH)
    try:
        # 建立價格表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        # 建立資訊表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, updated_at TEXT)''')
        
        # 💡 自動升級：檢查並新增缺失的 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

def get_tw_stock_list():
    """從證交所獲取清單並標記市場別"""
    market_map = {
        'listed': '上市',
        'otc': '上櫃',
        'etf': 'ETF',
        'rotc': '興櫃'
    }
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    log(f"📡 獲取台股清單並同步資訊...")
    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            df = pd.read_html(StringIO(resp.text), header=0)[0]
            market_label = market_map.get(cfg['name'], '其他')
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                
                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
                    # 💡 寫入包含市場別的完整資訊
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (symbol, name, sector, market_label, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        except Exception as e:
            log(f"⚠️ 獲取 {cfg['name']} 市場失敗: {e}")
            
    conn.commit()
    conn.close()
    return list(set(stock_list))

# ========== 3. 核心下載邏輯 (具備重試機制) ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache", "data": None}

    # 💡 增加重試機制 (最多嘗試 3 次)
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            time.sleep(random.uniform(0.1, 0.3))
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, timeout=20, auto_adjust=True)
            
            if hist is None or hist.empty:
                if attempt < max_retries: continue
                return {"symbol": symbol, "status": "empty", "data": None}
            
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # ✅ 壓平 MultiIndex (處理 yfinance 隨機出現的雙層表頭)
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            if not IS_GITHUB_ACTIONS:
                df_final.to_csv(csv_path, index=False)
                
            return {"symbol": symbol, "status": "success", "data": df_final}
        except Exception:
            if attempt < max_retries:
                time.sleep(random.uniform(1.5, 3.0)) # 錯誤後隨機等待久一點再重試
                continue
            return {"symbol": symbol, "status": "error", "data": None}

# ========== 4. 主流程 (批次寫入) ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_tw_stock_list()
    if not items:
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始同步 TW | 目標: {len(items)} 檔 | 執行緒: {MAX_WORKERS}")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    
    # 💡 使用單一連線批次寫入，避免頻繁 IO
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        pbar = tqdm(total=len(items), desc=f"TW同步({mode})")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status")
            stats[s] += 1
            
            if s == "success" and res["data"] is not None:
                # 寫入價格資料
                res["data"].to_sql('stock_prices', conn, if_exists='append', index=False, 
                                 method=lambda table, conn, keys, data_iter: 
                                 conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            
            if s in ["error", "empty"]:
                fail_list.append(res.get("symbol"))
            pbar.update(1)
            
        pbar.close()
    
    conn.commit()
    conn.close()

    # 💡 修正回傳統計：查詢資料庫中實際擁有的不重複標的總數
    conn = sqlite3.connect(DB_PATH)
    final_db_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    has_changed = stats['success'] > 0
    if has_changed or IS_GITHUB_ACTIONS:
        log("🧹 優化資料庫 (VACUUM)...")
        conn = sqlite3.connect(DB_PATH)
        conn.execute("VACUUM")
        conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 資料庫總數: {final_db_count} | 本次更新: {stats['success']} | ❌ 錯誤/無資料: {stats['error'] + stats['empty']}")

    return {
        "success": final_db_count,     # 回傳資料庫實有總數，防止 Coverage 爆表
        "total": len(items),          # 本次目標清單總數
        "fail_list": fail_list,
        "has_changed": has_changed
    }

if __name__ == "__main__":
    run_sync(mode='hot')
