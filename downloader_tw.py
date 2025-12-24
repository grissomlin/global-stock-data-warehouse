# -*- coding: utf-8 -*-
import time, random, requests, os, sqlite3, json
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 核心參數設定 ==========
MARKET_CODE = "tw-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 資料與審計資料庫路徑
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

# ✅ 效能與時效設定
MAX_WORKERS = 3  # 維持低執行緒以防 Yahoo 封鎖 IP
DATA_EXPIRY_SECONDS = 3600  # 1 小時內抓過則跳過

os.makedirs(DATA_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def init_audit_db():
    """初始化審計資料庫紀錄表"""
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS sync_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execution_time TEXT,
            market_id TEXT,
            total_count INTEGER,
            success_count INTEGER,
            fail_count INTEGER,
            success_rate REAL
        )''')
        conn.commit()
    finally:
        conn.close()

def get_full_stock_list():
    """獲取台股全市場清單 (包含上市、上櫃、ETF、興櫃、創新板、存託憑證)"""
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'tw_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    all_items = []
    print("📡 正在從證交所獲取 7 大類市場完整清單...")
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                if code and '有價證券' not in code:
                    all_items.append(f"{code}{cfg['suffix']}")
        except Exception as e:
            print(f"⚠️ 獲取 {cfg['name']} 失敗: {e}")
            
    unique_items = list(set(all_items))
    print(f"✅ 台股清單獲取完成，總計標的: {len(unique_items)} 檔")
    return unique_items

def fetch_single_stock(yf_tkr, period):
    """單檔下載邏輯：智慧快取 + 重試機制"""
    # 建立 CSV 檔案路徑
    out_path = os.path.join(DATA_DIR, f"{yf_tkr}.csv")
    
    # 💡 智慧快取檢查 (1小時內抓過且有資料則跳過)
    if os.path.exists(out_path):
        file_age = time.time() - os.path.getmtime(out_path)
        if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": yf_tkr}

    try:
        time.sleep(random.uniform(0.6, 1.2))
        tk = yf.Ticker(yf_tkr)
        for attempt in range(2):
            try:
                hist = tk.history(period=period, auto_adjust=True, timeout=25)
                if hist is not None and not hist.empty:
                    hist = hist.reset_index()
                    hist.columns = [c.lower() for c in hist.columns]
                    if 'date' in hist.columns:
                        hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                        hist['symbol'] = yf_tkr
                        # 儲存到本地 CSV 快取
                        hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']].to_csv(out_path, index=False, encoding='utf-8-sig')
                        return {"status": "success", "tkr": yf_tkr}
                break 
            except Exception as e:
                if "Rate limited" in str(e): 
                    time.sleep(random.uniform(20, 40))
                time.sleep(random.uniform(2, 5))
    except: 
        return None
    return None

def main():
    """主進入點：由 main.py 呼叫"""
    start_time = time.time()
    init_audit_db()
    
    # 預設為增量更新，若需全量可調整
    is_first_time = False 
    period = "max" if is_first_time else "7d"
    
    items = get_full_stock_list()
    log(f"🚀 台股任務啟動: {period}, 目標總數: {len(items)}")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_stock, tkr, period): tkr for tkr in items}
        pbar = tqdm(total=len(items), desc="TW 下載進度")
        
        for future in as_completed(futures):
            res = future.result()
            s = res.get("status", "error") if res else "error"
            stats[s] += 1
            
            # 收集失敗名單
            if s in ["error", "empty"]:
                # 取得該任務對應的代號
                fail_list.append(futures[future])
                
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

    # 回傳統計結果給 main.py 與 notifier.py
    return {
        "total": total,
        "success": success,
        "fail": fail,
        "fail_list": fail_list
    }

if __name__ == "__main__":
    main()
