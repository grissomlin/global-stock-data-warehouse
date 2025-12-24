# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 💡 指向核心資料庫
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")

# ✅ 效能與穩定性設定
MAX_WORKERS = 3  # 韓股對頻繁請求較敏感，建議設為 3
DATA_EXPIRY_SECONDS = 3600

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    """確保必要套件已安裝"""
    try:
        __import__(pkg.replace('-', '_'))
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

def init_db():
    """初始化資料庫結構"""
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
        conn.commit()
    finally:
        conn.close()

def get_kr_stock_list():
    """獲取韓股清單並同步更新名稱"""
    ensure_pkg("finance-datareader")
    import FinanceDataReader as fdr
    
    log("📡 正在獲取韓國市場 (KOSPI/KOSDAQ) 完整清單與名稱...")
    try:
        # 獲取韓國兩大交易所清單
        df_kospi = fdr.StockListing('KOSPI')
        df_kosdaq = fdr.StockListing('KOSDAQ')
        df = pd.concat([df_kospi, df_kosdaq])
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            code = str(row['Code']).strip()
            # Yahoo 格式：KOSPI(.KS), KOSDAQ(.KQ)
            suffix = ".KS" if row['Market'] == 'KOSPI' else ".KQ"
            symbol = f"{code}{suffix}"
            name = row['Name']
            sector = row.get('Sector', 'Unknown')
            
            # 💡 同步名稱與產業資訊到 stock_info
            conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, sector, updated_at) VALUES (?, ?, ?, ?)",
                         (symbol, name, sector, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 成功同步韓股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 韓股清單獲取失敗: {e}")
        return [("005930.KS", "SAMSUNG ELECTRONICS"), ("000660.KS", "SK HYNIX")]

def download_one(args):
    """單檔下載邏輯"""
    symbol, name, mode = args
    # 決定起點 (Hot: 2020 / Cold: 2000)
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-03"
    
    try:
        time.sleep(random.uniform(0.7, 1.5))
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, timeout=30)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
        # 寫入資料庫
        conn = sqlite3.connect(DB_PATH)
        df_final.to_sql('stock_prices', conn, if_exists='append', index=False, method='multi')
        conn.close()
        
        return {"symbol": symbol, "status": "success"}
    except Exception:
        return {"symbol": symbol, "status": "error"}

def run_sync(mode='hot'):
    """執行同步主流程"""
    start_time = time.time()
    init_db()
    
    # 1. 獲取名單並同步名稱
    items = get_kr_stock_list()
    if not items:
        log("❌ 無法取得名單，終止。")
        return

    log(f"🚀 開始下載 KR ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載
    stats = {"success": 0, "empty": 0, "error": 0}
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"KR({mode})下載中")
        
        for f in as_completed(futures):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()

    # 3. 優化資料庫
    log("🧹 正在優化資料庫空間 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 {MARKET_CODE} 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 成功: {stats['success']} | 📭 空資料: {stats['empty']} | ❌ 錯誤: {stats['error']}")

if __name__ == "__main__":
    # 測試執行
    run_sync(mode='hot')
