# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 💡 指向您的核心資料庫
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")

# ✅ 效能與穩定性設定
MAX_WORKERS = 4 
DATA_EXPIRY_SECONDS = 3600

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg_install_name, import_name):
    """確保必要套件已安裝"""
    try:
        __import__(import_name)
    except ImportError:
        log(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

# 載入日股清單工具
ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

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

def get_jp_stock_list():
    """獲取日股清單並同步更新名稱"""
    log("📡 正在從 TSE 數據庫獲取最新日股名單與名稱...")
    try:
        # 讀取套件內建的日股清單 CSV
        df = pd.read_csv(tse.csv_file_path)
        
        # 識別欄位 (日本股代號通常在 'Code' 或 'コード')
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Issues'] if c in df.columns), None)
        sector_col = next((c for c in ['33業種区分', 'Sector', 'industry'] if c in df.columns), None)

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            # 格式轉換：1234 -> 1234.T
            if len(raw_code) >= 4 and raw_code[:4].isdigit():
                symbol = f"{raw_code[:4]}.T"
                name = str(row[name_col]) if name_col else "Unknown"
                sector = str(row[sector_col]) if sector_col else "Unknown"
                
                # 💡 同步到 stock_info
                conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, sector, updated_at) VALUES (?, ?, ?, ?)",
                             (symbol, name, sector, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
        
        conn.commit()
        conn.close()
        log(f"✅ 成功同步日股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 日股清單獲取失敗: {e}")
        return [("7203.T", "TOYOTA MOTOR")]

def download_one(args):
    """單檔下載邏輯"""
    symbol, name, mode = args
    # 決定下載起點 (Hot: 2020-01-01 / Cold: 1999-01-01)
    start_date = "2020-01-01" if mode == 'hot' else "1999-01-01"
    
    try:
        time.sleep(random.uniform(0.6, 1.3))
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, timeout=30)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            # 移除時區資訊並轉為字串日期
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
    items = get_jp_stock_list()
    if not items:
        log("❌ 無法取得名單，終止任務。")
        return

    log(f"🚀 開始下載 JP ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載
    stats = {"success": 0, "empty": 0, "error": 0}
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"JP({mode})下載中")
        
        for f in as_completed(futures):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()

    # 3. 資料庫優化
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
