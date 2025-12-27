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

# ✅ 效能優化設定
MAX_WORKERS = 6 if IS_GITHUB_ACTIONS else 10 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. 資料庫初始化 ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        # 檢查並新增 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取台股清單 ==========

def get_tw_stock_list():
    market_map = {'listed': '上市', 'otc': '上櫃', 'etf': 'ETF', 'rotc': '興櫃'}
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
            dfs = pd.read_html(StringIO(resp.text), header=0)
            if not dfs: continue
            df = dfs[0]
            market_label = market_map.get(cfg['name'], '其他')
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                
                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
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

# ========== 4. 核心下載邏輯 (強化重試與 MultiIndex 處理) ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    
    # 增加初始隨機等待，避免併發過高被 Yahoo 暫封
    time.sleep(random.uniform(0.1, 0.5))
    
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            # 使用 yfinance 下載
            df = yf.download(symbol, start=start_date, progress=False, timeout=25, auto_adjust=True)
            
            if df is None or df.empty:
                if attempt < max_retries:
                    time.sleep(random.uniform(2, 5))
                    continue
                return {"symbol": symbol, "status": "empty", "data": None}
            
            # ✅ 處理 yfinance 可能回傳的 MultiIndex 結構
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            # 標準化日期與格式
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            return {"symbol": symbol, "status": "success", "data": df_final}
        except Exception:
            if attempt < max_retries:
                time.sleep(random.uniform(3, 7))
                continue
            return {"symbol": symbol, "status": "error", "data": None}

# ========== 5. 主流程與日期統計 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_tw_stock_list()
    if not items:
        log("❌ 無法獲取股票清單，終止任務")
        return {"success": 0, "total": 0, "has_changed": False}

    log(f"🚀 開始同步 TW | 目標: {len(items)} 檔 | 執行緒: {MAX_WORKERS}")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        pbar = tqdm(total=len(items), desc=f"TW同步({mode})")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status")
            if s in stats: stats[s] += 1
            
            if s == "success" and res["data"] is not None:
                # 批次寫入資料庫
                res["data"].to_sql('stock_prices', conn, if_exists='append', index=False, 
                                 method=lambda table, conn, keys, data_iter: 
                                 conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            
            if s in ["error", "empty"]:
                fail_list.append(res.get("symbol"))
            pbar.update(1)
        pbar.close()
    
    conn.commit()
    conn.close()

    # 💡 關鍵：同步完成後的日期偵察與統計
    conn = sqlite3.connect(DB_PATH)
    try:
        # 查詢整張價格表的最大日期
        max_date = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
        # 查詢有多少股票擁有這個最新的日期
        latest_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices WHERE date = ?", (max_date,)).fetchone()[0]
        # 查詢 info 表中的總股票數
        total_info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
    except Exception as e:
        max_date = "Error"
        latest_count = 0
        total_info_count = 0
    
    log(f"🧹 優化資料庫 (VACUUM)...")
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"📅 資料庫最新交易日: {max_date} (共有 {latest_count} 檔更新至此日期)")
    log(f"✅ 資料庫總數: {total_info_count} | 本次更新: {stats['success']} | ❌ 無資料或異常: {stats['error'] + stats['empty']}")

    return {
        "success": total_info_count,
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": stats['success'] > 0,
        "latest_date": max_date
    }

if __name__ == "__main__":
    run_sync(mode='hot')
