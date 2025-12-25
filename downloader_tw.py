# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
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

# ✅ 效能調優：台股對連線極其敏感，GitHub 模式降至 2~3 執行緒
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 4 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 核心輔助函式 ==========

def insert_or_replace(table, conn, keys, data_iter):
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

def init_db():
    """初始化資料庫結構 (確保包含 market 欄位)"""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        # 💡 確保有 name, sector, market 三大資訊欄位
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, 
                            name TEXT, 
                            sector TEXT, 
                            market TEXT, 
                            updated_at TEXT)''')
        conn.commit()
    finally:
        conn.close()

def get_tw_stock_list():
    """從證交所獲取包含市場別與產業別的完整清單"""
    market_map = {
        'listed': '上市',
        'otc': '上櫃',
        'etf': 'ETF',
        'rotc': '興櫃',
        'tw_innovation': '創新板',
        'otc_innovation': '戰略新板',
        'dr': '存託憑證'
    }

    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'tw_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
    ]

    log(f"📡 正在同步台股清單 (含產業與市場別)...")
    conn = sqlite3.connect(DB_PATH)
    stock_list = []

    for cfg in url_configs:
        try:
            time.sleep(random.uniform(0.5, 1.0))
            resp = requests.get(cfg['url'], timeout=15)
            # 讀取網頁表格
            df = pd.read_html(StringIO(resp.text), header=0)[0]

            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                market_label = market_map.get(cfg['name'], '其他')

                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
                    # 💡 存入資料庫時包含詳細資訊
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (symbol, name, sector, market_label, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        except Exception as e:
            log(f"⚠️ 抓取 {cfg['name']} 市場失敗: {e}")

    conn.commit()
    conn.close()
    
    unique_items = list(dict.fromkeys(stock_list))
    log(f"✅ 台股清單同步完成，標的總數: {len(unique_items)} 檔")
    return unique_items

# ========== 3. 核心下載/重試邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache"}

    # 💡 增加重試機制與長延遲
    max_retries = 3
    for attempt in range(max_retries):
        try:
            wait_time = random.uniform(1.5, 3.2) if IS_GITHUB_ACTIONS else random.uniform(0.2, 0.5)
            time.sleep(wait_time)
            
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
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
                time.sleep(random.uniform(5, 12))
                continue
            return {"symbol": symbol, "status": "error"}

# ========== 4. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_tw_stock_list()
    if not items:
        log("❌ 無法取得清單，任務終止。")
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行台股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"TW處理中({mode})")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error":
                fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()

    has_changed = stats['success'] > 0
    if has_changed or IS_GITHUB_ACTIONS:
        log("🧹 執行資料庫優化 (VACUUM)...")
        conn = sqlite3.sqlite3.connect(DB_PATH)
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
