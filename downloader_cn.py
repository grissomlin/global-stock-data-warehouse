# -*- coding: utf-8 -*-
import os, sys, time, random, json, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "cn-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 預設資料庫路徑 (建議放在專案根目錄)
DB_PATH = os.path.join(BASE_DIR, "cn_stock_warehouse.db")

# 穩定性設定
THREADS_CN = 4 
DATA_EXPIRY_SECONDS = 3600  # 1小時內不重複抓同支股票

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def init_db():
    """初始化資料庫結構，確保有 stock_info 表"""
    conn = sqlite3.connect(DB_PATH)
    try:
        # 行情表 (如果不存在)
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        # 公司資訊表 (關鍵：存放名稱)
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY,
                            name TEXT,
                            sector TEXT,
                            updated_at TEXT)''')
        conn.commit()
    finally:
        conn.close()

def get_cn_stock_list():
    """從 Akshare 獲取清單並同步寫入 stock_info"""
    import akshare as ak
    log("📡 正在從接口獲取最新 A 股名單與名稱...")
    try:
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        df = pd.concat([df_sh, df_sz], ignore_index=True)
        
        # 過濾與格式化代碼
        df['code'] = df['代码'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        name_col = '名称' if '名称' in df.columns else '名稱'
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        log(f"📝 同步 {len(df)} 檔公司名稱至 stock_info 表...")
        for _, row in df.iterrows():
            symbol = f"{row['code']}.SS" if row['code'].startswith('6') else f"{row['code']}.SZ"
            name = row[name_col]
            # 💡 同步名稱：每次執行都會更新，確保名稱最新
            conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                         (symbol, name, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        return stock_list
    except Exception as e:
        log(f"⚠️ 獲取名單失敗: {e}")
        return []

def download_one(args):
    """單檔下載核心邏輯"""
    symbol, name, mode = args
    
    # 決定下載起點
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    try:
        # 增加一點隨機延遲避開風控
        time.sleep(random.uniform(1.2, 2.5))
        
        tk = yf.Ticker(symbol)
        # 下載數據
        hist = tk.history(start=start_date, timeout=25)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        # 資料清洗
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.strftime('%Y-%m-%d')
        
        # 只要我們需要的欄位
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
        # 寫入 SQLite (使用 append 模式)
        conn = sqlite3.connect(DB_PATH)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method='multi')
        # 處理重複：SQLite to_sql 不支援 INSERT OR IGNORE，所以後續用 SQL 處理重複或改用一次性寫入
        conn.close()
        
        return {"symbol": symbol, "status": "success", "rows": len(df_final)}
    except Exception as e:
        return {"symbol": symbol, "status": "error", "reason": str(e)}

def run_sync(mode='hot'):
    """執行同步主流程"""
    start_time = time.time()
    init_db()
    
    # 1. 獲取名單與同步名稱
    items = get_cn_stock_list()
    if not items:
        log("❌ 無法取得名單，終止。")
        return

    log(f"🚀 開始下載 ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載
    stats = {"success": 0, "empty": 0, "error": 0}
    # 將模式包入參數
    task_args = [(item[0], item[1], mode) for item in items]
    
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"A股({mode})下載中")
        
        for f in as_completed(futures):
            res = f.result()
            stats[res['status']] += 1
            pbar.update(1)
        pbar.close()

    # 3. 執行 VACUUM 優化資料庫體積
    log("🧹 正在優化資料庫空間 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 {MARKET_CODE} 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 成功: {stats['success']} | 📭 空資料: {stats['empty']} | ❌ 錯誤: {stats['error']}")

if __name__ == "__main__":
    # 測試執行：預設為 hot 模式
    # 如果要抓全量，請改為 run_sync(mode='cold')
    run_sync(mode='hot')
