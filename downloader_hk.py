# -*- coding: utf-8 -*-
import os, io, re, time, random, requests, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 💡 指向您的核心資料庫
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")

# ✅ 效能與穩定性設定
MAX_WORKERS = 4 
DATA_EXPIRY_SECONDS = 3600

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (4 位數.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    if not digits: return ""
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品 (確保只抓普通股)"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

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

def get_hk_stock_list():
    """從 HKEX 獲取清單並同步寫入 stock_info"""
    log("📡 正在從港交所 (HKEX) 獲取最新名單與名稱...")
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 定位表頭
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
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            name = str(row[col_name])
            if classify_security(name) == "Common Stock":
                symbol = to_symbol_yf(row[col_code])
                if symbol:
                    # 💡 同步名稱到 stock_info
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                                 (symbol, name, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        
        conn.commit()
        conn.close()
        log(f"✅ 成功獲取並同步港股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 港股清單抓取失敗: {e}")
        return [("0700.HK", "TENCENT"), ("9988.HK", "BABA")]

def download_one(args):
    """單檔下載邏輯"""
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "1990-01-01"
    
    try:
        time.sleep(random.uniform(0.8, 2.0))
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
        
        conn = sqlite3.connect(DB_PATH)
        # 💡 使用增量寫入，重複的部分由 SQL 處理或在此處過濾
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
    items = get_hk_stock_list()
    if not items:
        log("❌ 無法取得名單，終止任務。")
        return

    log(f"🚀 開始下載 HK ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載
    stats = {"success": 0, "empty": 0, "error": 0}
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"HK({mode})下載中")
        
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
    # 測試執行：預設 hot 模式
    run_sync(mode='hot')
