# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, random, io
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "cn-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "cn_stock_warehouse.db")

# 💡 自動判斷環境：GitHub Actions 執行時此變數為 true
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 快取設定
CACHE_DIR = os.path.join(BASE_DIR, "cache_cn")
DATA_EXPIRY_SECONDS = 86400  # 本機快取效期：24小時

if not IS_GITHUB_ACTIONS and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# ✅ 效能設定：GitHub 模式降低併發數以降低 IP 被封鎖機率
THREADS_CN = 3 if IS_GITHUB_ACTIONS else 6 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 核心輔助函式 ==========

def insert_or_replace(table, conn, keys, data_iter):
    """防止重複寫入的核心 SQL 邏輯"""
    sql = f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})"
    conn.executemany(sql, data_iter)

def init_db():
    """初始化資料庫結構 (支援自動升級市場欄位)"""
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
        
        # 💡 自動升級：檢查並新增 market 欄位 (確保全球資料庫結構統一)
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 CN 資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
            
        conn.commit()
    finally:
        conn.close()

def get_cn_stock_list():
    """從 Akshare 獲取清單並標記市場 (上證/深證)"""
    import akshare as ak
    log(f"📡 正在從 Akshare 同步 A 股清單...")
    
    # 💡 針對網路超時加入 3 次重試
    for attempt in range(3):
        try:
            df_sh = ak.stock_sh_a_spot_em()
            df_sz = ak.stock_sz_a_spot_em()
            df = pd.concat([df_sh, df_sz], ignore_index=True)
            
            df['code'] = df['代码'].astype(str).str.zfill(6)
            # 過濾主流 A 股代碼
            valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
            df = df[df['code'].str.startswith(valid_prefixes)]
            
            name_col = '名称' if '名称' in df.columns else '名稱'
            conn = sqlite3.connect(DB_PATH)
            stock_list = []
            
            for _, row in df.iterrows():
                code = row['code']
                # 💡 自動判斷市場別
                if code.startswith('6'):
                    symbol = f"{code}.SS"
                    market = "SSE (上證)"
                else:
                    symbol = f"{code}.SZ"
                    market = "SZSE (深證)"
                
                name = row[name_col]
                # 寫入資訊表 (產業目前標記為 Unknown)
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, "Unknown", market, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
                
            conn.commit()
            conn.close()
            log(f"✅ 成功同步 A 股清單: {len(stock_list)} 檔")
            return stock_list
        except Exception as e:
            if attempt < 2:
                log(f"⚠️ 獲取名單失敗 ({attempt+1}/3): {e}，15秒後重試...")
                time.sleep(15)
            else:
                log(f"❌ 獲取名單失敗，終止任務。")
    return []

# ========== 3. 核心下載/重試邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    csv_path = os.path.abspath(os.path.join(CACHE_DIR, f"{symbol}.csv"))
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    # ⚡ 閃電快取
    if not IS_GITHUB_ACTIONS and os.path.exists(csv_path):
        file_age = time.time() - os.path.getmtime(csv_path)
        if file_age < DATA_EXPIRY_SECONDS:
            return {"symbol": symbol, "status": "cache"}

    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 💡 增加等待時間避開頻率限制 (1.5 ~ 3.5秒)
            wait = random.uniform(1.5, 3.5) if IS_GITHUB_ACTIONS else random.uniform(0.1, 0.4)
            time.sleep(wait)
            
            tk = yf.Ticker(symbol)
            # 使用 auto_adjust=True 處理分紅派息
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
            # 寫入價格資料
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda table, conn, keys, data_iter: 
                            conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
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
    
    items = get_cn_stock_list()
    if not items:
        return {"fail_list": [], "success": 0, "has_changed": False}

    log(f"🚀 開始執行 A 股同步 ({mode.upper()}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "cache": 0, "empty": 0, "error": 0}
    fail_list = []
    task_args = [(item[0], item[1], mode) for item in items]
    
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"CN處理中")
        
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
