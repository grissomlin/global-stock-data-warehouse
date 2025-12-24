# -*- coding: utf-8 -*-
import os, sys, time, random, subprocess, sqlite3, json
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 核心參數設定 ==========
MARKET_CODE = "kr-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 資料與審計資料庫路徑
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

# ✅ 效能與時效設定
MAX_WORKERS = 3 
DATA_EXPIRY_SECONDS = 3600  # 1 小時內抓過則跳過

os.makedirs(DATA_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    """確保必要套件已安裝"""
    try:
        __import__(pkg)
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

def init_audit_db():
    """初始化審計資料庫"""
    conn = sqlite3.connect(AUDIT_DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS sync_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_time TEXT,
        market_id TEXT,
        total_count INTEGER,
        success_count INTEGER,
        fail_count INTEGER,
        success_rate REAL
    )''')
    conn.close()

def get_full_stock_list():
    """獲取韓股完整清單 (KOSPI & KOSDAQ)"""
    # 使用 FinanceDataReader 獲取清單，這比手動爬網頁穩定
    ensure_pkg("finance-datareader")
    import FinanceDataReader as fdr
    
    print("📡 正在獲取韓國市場 (KOSPI/KOSDAQ) 完整清單...")
    try:
        df_kospi = fdr.StockListing('KOSPI')
        df_kosdaq = fdr.StockListing('KOSDAQ')
        df = pd.concat([df_kospi, df_kosdaq])
        
        # Yahoo Finance 格式：KOSPI 為 .KS, KOSDAQ 為 .KQ
        res = []
        for _, row in df.iterrows():
            code = str(row['Code']).strip()
            # 判斷市場後綴
            suffix = ".KS" if row['Market'] == 'KOSPI' else ".KQ"
            res.append(f"{code}{suffix}")
        
        final_list = list(set(res))
        print(f"✅ 成功獲取 {len(final_list)} 檔韓股代號")
        return final_list
    except Exception as e:
        print(f"❌ 韓股清單獲取失敗: {e}")
        return ["005930.KS", "000660.KS"] # 三星電子 & SK海力士保底

def download_one(symbol, period):
    """單檔下載邏輯：智慧快取 + 重試"""
    out_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    
    # 💡 智慧快取檢查 (抓過且在效期內則跳過)
    if os.path.exists(out_path):
        file_age = time.time() - os.path.getmtime(out_path)
        if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": symbol}

    try:
        time.sleep(random.uniform(0.6, 1.2))
        tk = yf.Ticker(symbol)
        hist = tk.history(period=period, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                # 過濾並儲存標準欄位
                hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']].to_csv(out_path, index=False, encoding='utf-8-sig')
                return {"status": "success", "tkr": symbol}
        return {"status": "empty", "tkr": symbol}
    except:
        return {"status": "error", "tkr": symbol}

def main():
    """主進入點：對接 main.py 邏輯"""
    start_time = time.time()
    init_audit_db()
    
    # 判斷是否為初次抓取，可由 main.py 呼叫時決定，這裡預設為 7d
    is_first_time = False 
    period = "max" if is_first_time else "7d"
    
    items = get_full_stock_list()
    log(f"🚀 韓股任務啟動: {period}, 目標總數: {len(items)} 檔")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, tkr, period): tkr for tkr in items}
        pbar = tqdm(total=len(items), desc="KR 下載進度")
        
        for future in as_completed(futures):
            res = future.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s in ["error", "empty"]:
                fail_list.append(res.get("tkr", "Unknown"))
            pbar.update(1)
        pbar.close()

    total = len(items)
    success = stats['success'] + stats['exists']
    fail = stats['error'] + stats['empty']
    rate = round((success / total * 100), 2) if total > 0 else 0

    # 🚀 紀錄 Audit DB (台北時間 UTC+8)
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        now_ts = (datetime.utcnow() + pd.Timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute('''INSERT INTO sync_audit 
            (execution_time, market_id, total_count, success_count, fail_count, success_rate)
            VALUES (?, ?, ?, ?, ?, ?)''', (now_ts, MARKET_CODE, total, success, fail, rate))
        conn.commit()
    finally:
        conn.close()

    # 回傳統計字典
    download_stats = {
        "total": total,
        "success": success,
        "fail": fail,
        "fail_list": fail_list
    }

    log(f"📊 韓股執行報告: 成功={success}, 失敗={fail}, 成功率={rate}%")
    return download_stats

if __name__ == "__main__":
    main()
