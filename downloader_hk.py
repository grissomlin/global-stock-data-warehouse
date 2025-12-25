# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 效能調優：港股連線較嚴格，GitHub 模式降至 2~3 執行緒
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 5 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫初始化 (含自動升級邏輯) ==========

def init_db():
    """初始化資料庫並自動檢查/新增 market 欄位"""
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
        
        # 💡 自動升級：檢查並新增 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 HK 資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取港股清單 ==========

def get_hk_stock_list():
    """獲取港股清單 (包含過濾權證與 ETF)"""
    # 港交所官方代碼清單 Excel 下載網址 (Standard Transfer Form)
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
    
    log(f"📡 正在從港交所獲取名單...")
    try:
        r = requests.get(url, headers=headers, timeout=15)
        # 讀取 Excel (跳過前面的標題行)
        df = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 尋找包含 "Stock Code" 的那一行作為標題
        hdr_idx = None
        for i in range(len(df)):
            if "Stock Code" in str(df.iloc[i].values):
                hdr_idx = i
                break
        
        if hdr_idx is None: raise RuntimeError("找不到港股清單表頭")
        
        df.columns = df.iloc[hdr_idx]
        df = df.iloc[hdr_idx+1:].copy()
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            raw_code = str(row['Stock Code']).strip()
            name = str(row.get('English Stock Short Name', 'Unknown')).strip()
            
            # 過濾權證與衍生品 (通常港股普通股代碼在 1-9999 之間)
            if raw_code.isdigit() and int(raw_code) < 10000:
                # Yahoo 格式: 0001.HK (4位補零)
                symbol = f"{raw_code.zfill(4)}.HK"
                market = "HKEX"
                
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, "Unknown", market, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
                
        conn.commit()
        conn.close()
        log(f"✅ 成功同步港股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 港股清單獲取失敗: {e}")
        # 如果失敗，回傳一個基本名單避免任務崩潰
        return [("0700.HK", "TENCENT"), ("09988.HK", "BABA-SW")]

# ========== 4. 核心下載邏輯 (加強穩定性) ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 💡 港股下載延遲 (2.0 ~ 4.0秒) 防止 Yahoo 封鎖
            wait_time = random.uniform(2.0, 4.0) if IS_GITHUB_ACTIONS else random.uniform(0.2, 0.5)
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
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            # 使用 INSERT OR REPLACE 避免重複
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

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_hk_stock_list()
    if not items:
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行港股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="HK處理中"):
            res = f.result()
            s = res.get("status", "error")
            stats[s if s in stats else 'error'] += 1
            if s == "error": fail_list.append(res.get("symbol"))

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
