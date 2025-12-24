# -*- coding: utf-8 -*-
import os, io, re, time, random, requests, sqlite3, json
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "hk-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 資料儲存與審計資料庫路徑
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

# ✅ 效能與時效設定
MAX_WORKERS = 4  # 港股建議維持在此數量以防觸發 Yahoo 限流
DATA_EXPIRY_SECONDS = 3600  # 1 小時內抓過則跳過本地下載

os.makedirs(DATA_DIR, exist_ok=True)

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (4 位數.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    if not digits: return ""
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品 (牛熊、權證等)"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

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
    """從 HKEX 獲取最新普通股清單"""
    print("📡 正在從港交所 (HKEX) 獲取最新普通股清單...")
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 定位表頭位置
        hdr_idx = 0
        for row_i in range(20):
            row_str = "".join([str(x) for x in df_raw.iloc[row_i]]).lower()
            if "stock code" in row_str and "short name" in row_str:
                hdr_idx = row_i
                break
        
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].tolist()
        
        col_code = [c for c in df.columns if "Stock Code" in str(c)][0]
        col_name = [c for c in df.columns if "Short Name" in str(c)][0]
        
        res = []
        for _, row in df.iterrows():
            name = str(row[col_name])
            if classify_security(name) == "Common Stock":
                yf_sym = to_symbol_yf(row[col_code])
                if yf_sym:
                    res.append(yf_sym)
        
        final_list = list(set(res))
        print(f"✅ 成功獲取港股清單: {len(final_list)} 檔")
        return final_list
    except Exception as e:
        print(f"❌ 港股清單抓取失敗: {e}")
        return ["0700.HK", "9988.HK", "3690.HK"] # 保底核心股

def download_one(symbol, period):
    """單檔下載邏輯：智慧快取 + 重試"""
    out_path = os.path.join(DATA_DIR, f"{symbol}.csv")
    
    # 💡 智慧快取檢查 (抓過且在效期內則跳過)
    if os.path.exists(out_path):
        file_age = time.time() - os.path.getmtime(out_path)
        if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": symbol}

    try:
        time.sleep(random.uniform(0.6, 1.5))
        tk = yf.Ticker(symbol)
        hist = tk.history(period=period, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']].to_csv(out_path, index=False, encoding='utf-8-sig')
                return {"status": "success", "tkr": symbol}
        return {"status": "empty", "tkr": symbol}
    except:
        return {"status": "error", "tkr": symbol}

# ✨ 關鍵進入點：必須定義為 main() 以對接 main.py 邏輯
def main():
    start_time = time.time()
    init_audit_db()
    
    # 判斷是否為首次執行 (由 main.py 決定，此處預設 7d)
    period = "7d" 
    items = get_full_stock_list()
    
    print(f"🚀 港股任務啟動: {period}, 目標總數: {len(items)} 檔")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, tkr, period): tkr for tkr in items}
        pbar = tqdm(total=len(items), desc="HK 下載進度")
        
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

    # 回傳統計字典給 main.py
    download_stats = {
        "total": total,
        "success": success,
        "fail": fail,
        "fail_list": fail_list
    }

    print(f"📊 港股報告: 成功={success}, 失敗={fail}, 成功率={rate}%")
    return download_stats

if __name__ == "__main__":
    main()
