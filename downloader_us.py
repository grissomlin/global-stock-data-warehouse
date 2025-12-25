# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "us-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "us_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

CACHE_DIR = os.path.join(BASE_DIR, "cache_us")
DATA_EXPIRY_SECONDS = 86400

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能調優：GitHub 伺服器 IP 較敏感，降低併發數以求穩定
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 8 
LIST_THRESHOLD = 3000

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 核心輔助函式 ==========

def insert_or_replace(table, conn, keys, data_iter):
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

def init_db():
    """初始化資料庫並自動升級結構"""
    conn = sqlite3.connect(DB_PATH)
    try:
        # 價格表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        # 資訊表
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, updated_at TEXT)''')
        
        # 💡 自動升級：檢查並新增 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 US 資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

def classify_security(name: str, is_etf: bool) -> str:
    """過濾掉權證、優先股等非普通股標的"""
    if is_etf: return "Exclude"
    n_upper = str(name).upper()
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE", "PWT"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_us_stock_list():
    """獲取美股清單並自動標記市場 (NASDAQ/NYSE/AMEX)"""
    all_items = []
    log(f"📡 獲取美股清單... (環境: {'GitHub' if IS_GITHUB_ACTIONS else 'Local'})")
    
    # 網址對應市場
    market_configs = [
        {"url": "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt", "label": "NASDAQ"},
        {"url": "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt", "label": "NYSE/AMEX"}
    ]
    
    conn = sqlite3.connect(DB_PATH)
    for cfg in market_configs:
        try:
            r = requests.get(cfg['url'], timeout=15)
            df = pd.read_csv(StringIO(r.text), sep="|")
            df = df[df["Test Issue"] == "N"]
            
            sym_col = "Symbol" if "nasdaqlisted" in cfg['url'] else "NASDAQ Symbol"
            name_col = "Security Name"
            etf_col = "ETF"
            
            for _, row in df.iterrows():
                name = str(row[name_col])
                is_etf = str(row[etf_col]) == "Y"
                
                if classify_security(name, is_etf) == "Common Stock":
                    symbol = str(row[sym_col]).strip().replace('$', '-')
                    # 💡 存入資訊表，sector 暫時標記為 Unknown，market 根據來源標記
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (symbol, name, "Unknown", cfg['label'], datetime.now().strftime("%Y-%m-%d")))
                    all_items.append((symbol, name))
            time.sleep(1) 
        except Exception as e:
            log(f"⚠️ 清單抓取失敗 ({cfg['url']}): {e}")
            
    conn.commit()
    conn.close()
    
    unique_items = list(set(all_items))
    if len(unique_items) >= LIST_THRESHOLD:
        log(f"✅ 成功同步美股清單: {len(unique_items)} 檔")
        return unique_items
    return [("AAPL", "APPLE INC"), ("TSLA", "TESLA INC")]

# ========== 3. 核心下載/重試邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1962-01-02"
    
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache"}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 💡 大幅拉長 GitHub 環境的等待時間 (1.5 ~ 3.5 秒) 以防被封
            wait_time = random.uniform(1.5, 3.5) if IS_GITHUB_ACTIONS else random.uniform(0.1, 0.4)
            time.sleep(wait_time)
            
            tk = yf.Ticker(symbol)
            # 使用 auto_adjust=True 處理美股常見的除權息
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # 處理 MultiIndex 欄位 (yfinance 偶發 Bug)
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            if not IS_GITHUB_ACTIONS:
                df_final.to_csv(csv_path, index=False)

            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method=insert_or_replace)
            conn.close()
            
            return {"symbol": symbol, "status": "success"}
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(10, 20)) # 下載失敗多等一下
                continue
            return {"symbol": symbol, "status": "error"}

# ========== 4. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_us_stock_list()
    if not items:
        log("❌ 無法取得美股清單，任務終止。")
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行美股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"US處理中")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error":
                fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()

    has_changed = stats['success'] > 0
    
    # 執行資料庫優化
    log("🧹 執行資料庫優化 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    
    return {
        "success": stats['success'],
        "cache": stats['cache'],
        "error": stats['error'],
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": has_changed
    }

if __name__ == "__main__":
    run_sync(mode='hot')
