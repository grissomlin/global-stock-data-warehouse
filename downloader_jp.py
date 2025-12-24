# -*- coding: utf-8 -*-
import os, sys, time, random, subprocess, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ========== 核心參數設定 ==========
MARKET_CODE = "jp-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 資料與審計資料庫路徑
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

# ✅ 效能與時效設定
MAX_WORKERS = 4 
DATA_EXPIRY_SECONDS = 3600  # 1 小時內抓過則跳過

os.makedirs(DATA_DIR, exist_ok=True)

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
    """獲取日股完整清單 (TSE)"""
    threshold = 3000
    print("📡 正在從 TSE 資料庫獲取日股清單...")
    try:
        df = pd.read_csv(tse.csv_file_path)
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        
        res = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            # 日本股 Yahoo 格式為 1234.T
            if len(code) >= 4 and code[:4].isdigit():
                res.append(f"{code[:4]}.T")
        
        final_list = list(set(res))
        if len(final_list) >= threshold:
            print(f"✅ 成功獲取 {len(final_list)} 檔日股代號")
            return final_list
    except Exception as e:
        print(f"❌ 日股清單獲取失敗: {e}")
    
    return ["7203.T"] # 豐田汽車保底

def download_one(symbol, period):
    """單檔下載邏輯：智慧快取 + 抗封鎖機制"""
    out_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    
    # 💡 智慧快取檢查 (1小時內抓過則跳過)
    if os.path.exists(out_path):
        file_age = time.time() - os.path.getmtime(out_path)
        if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": symbol}

    try:
        # 下載 max 歷史數據量大，隨機休眠 0.6 ~ 1.3 秒
        time.sleep(random.uniform(0.6, 1.3))
        tk = yf.Ticker(symbol)
        hist = tk.history(period=period, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                # 儲存 CSV
                hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']].to_csv(out_path, index=False, encoding='utf-8-sig')
                return {"status": "success", "tkr": symbol}
        return {"status": "empty", "tkr": symbol}
    except:
        return {"status": "error", "tkr": symbol}

# ✨ 關鍵進入點：改名為 main() 以對齊 main.py
def main():
    start_time = time.time()
    init_audit_db()
    
    # 判斷是否初次下載 (透過 main.py 或內部設定)
    # 這裡為了維持一致性，預設為 7d，若要補歷史請改為 max
    period = "7d" 
    
    items = get_full_stock_list()
    print(f"🚀 日股任務啟動: {period}, 目標: {len(items)} 檔")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, tkr, period): tkr for tkr in items}
        pbar = tqdm(total=len(items), desc="JP 下載進度")
        
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

    # ✨ 回傳 Dictionary 給 main.py 產生通知報表
    download_stats = {
        "total": total,
        "success": success,
        "fail": fail,
        "fail_list": fail_list
    }

    duration = (time.time() - start_time) / 60
    print(f"📊 日股處理完成，耗時 {duration:.1f} 分鐘。成功率: {rate}%")
    
    return download_stats

if __name__ == "__main__":
    main()
