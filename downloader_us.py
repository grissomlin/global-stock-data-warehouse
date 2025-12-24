# -*- coding: utf-8 -*-
import os, time, random, requests, json, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 核心參數設定 ==========
MARKET_CODE = "us-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 資料與審計資料庫路徑
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

# ✅ 效能與時效設定
MAX_WORKERS = 5  # 美股標的多，稍微調高執行緒，配合 Jitter 延遲
DATA_EXPIRY_SECONDS = 3600  # 1 小時內抓過則跳過
LIST_THRESHOLD = 3000

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

def classify_security(name: str, is_etf: bool) -> str:
    """過濾掉權證、優先股、ADR 等非普通股標的"""
    if is_etf: return "Exclude"
    n_upper = str(name).upper()
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """獲取美股普通股清單 (NASDAQ & NYSE)"""
    all_tickers = []
    print("📡 正在從 Nasdaq 獲取最新美股清單...")
    for site in ["nasdaqlisted.txt", "otherlisted.txt"]:
        try:
            url = f"https://www.nasdaqtrader.com/dynamic/symdir/{site}"
            r = requests.get(url, timeout=15)
            df = pd.read_csv(StringIO(r.text), sep="|")
            df = df[df["Test Issue"] == "N"]
            sym_col = "Symbol" if site == "nasdaqlisted.txt" else "NASDAQ Symbol"
            
            df["Category"] = df.apply(lambda row: classify_security(row["Security Name"], row["ETF"] == "Y"), axis=1)
            valid_df = df[df["Category"] == "Common Stock"]
            
            for _, row in valid_df.iterrows():
                ticker = str(row[sym_col]).strip().replace('$', '-')
                all_tickers.append(ticker)
            time.sleep(1) 
        except Exception as e:
            print(f"⚠️ {site} 清單抓取失敗: {e}")

    final_list = list(set(all_tickers))
    if len(final_list) >= LIST_THRESHOLD:
        print(f"✅ 成功獲取 {len(final_list)} 檔美股代號")
        return final_list
    return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"] # 保底標的

def fetch_single_stock(tkr, period):
    """單檔下載邏輯：智慧快取 + 抗封鎖機制"""
    out_path = os.path.join(DATA_DIR, f"{tkr}.csv")
    
    # 💡 智慧快取檢查 (1小時內抓過且內容正常則跳過)
    if os.path.exists(out_path):
        file_age = time.time() - os.path.getmtime(out_path)
        if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": tkr}

    time.sleep(random.uniform(0.5, 1.2))
    try:
        tk = yf.Ticker(tkr)
        hist = tk.history(period=period, auto_adjust=True, timeout=30)
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = tkr
                cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
                hist[cols].to_csv(out_path, index=False, encoding='utf-8-sig')
                return {"status": "success", "tkr": tkr}
        return {"status": "empty", "tkr": tkr}
    except:
        return {"status": "error", "tkr": tkr}

def main():
    """主進入點：由 main.py 呼叫"""
    start_time = time.time()
    init_audit_db()
    
    # 預設為增量更新 (7d)
    is_first_time = False 
    period = "max" if is_first_time else "7d"
    
    items = get_full_stock_list()
    print(f"🚀 美股任務啟動: {period}, 目標: {len(items)} 檔")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_stock, tkr, period): tkr for tkr in items}
        pbar = tqdm(total=len(items), desc="US 下載進度")
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

    return {
        "total": total,
        "success": success,
        "fail": fail,
        "fail_list": fail_list
    }

if __name__ == "__main__":
    main()
