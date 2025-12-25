# -*- coding: utf-8 -*-
import os, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "tw-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tw_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ GitHub Actions 模式下使用較慢的頻率，防止被封鎖
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 4 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

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

# ========== 3. 獲取台股清單 (含產業與市場) ==========

def get_tw_stock_list():
    """從證交所獲取包含市場別與產業別的完整清單"""
    market_map = {
        'listed': '上市',
        'otc': '上櫃',
        'etf': 'ETF',
        'rotc': '興櫃',
        'tw_innovation': '創新板(C)',
        'otc_innovation': '戰略新板(A)',
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
            time.sleep(random.uniform(1.0, 2.0)) # 避免過快請求
            resp = requests.get(cfg['url'], timeout=15)
            df = pd.read_html(StringIO(resp.text), header=0)[0]

            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                market_label = market_map.get(cfg['name'], '其他')

                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
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
    log(f"✅ 台股清單同步完成，總數: {len(unique_items)} 檔")
    return unique_items

# ========== 4. 下載邏輯與資料寫入 ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # GitHub 模式下增加隨機延遲，降低被 Yahoo 封鎖機率
            wait = random.uniform(2.0, 4.0) if IS_GITHUB_ACTIONS else random.uniform(0.1, 0.3)
            time.sleep(wait)
            
            df = yf.download(symbol, start=start_date, progress=False, timeout=20)
            
            if df.empty:
                return {"symbol": symbol, "status": "empty"}
                
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            # 處理 MultiIndex 欄位 (yfinance 隨機出現的情況)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            # 寫入資料庫
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda table, conn, keys, data_iter: 
                            conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            conn.close()
            
            return {"symbol": symbol, "status": "success"}
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(random.uniform(5, 10))
                continue
            return {"symbol": symbol, "status": "error"}

def run_sync(mode='hot'):
    init_db()
    items = get_tw_stock_list()
    if not items: return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行台股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="TW同步"):
            res = f.result()
            s = res.get("status", "error")
            stats[s if s in stats else 'error'] += 1
            if s == "error": fail_list.append(res.get("symbol"))

    # 執行資料庫清理優化
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    log(f"📊 同步完成！成功: {stats['success']}, 失敗: {stats['error']}")
    return {"success": stats['success'], "error": stats['error'], "has_changed": stats['success'] > 0}

if __name__ == "__main__":
    run_sync(mode='hot')
