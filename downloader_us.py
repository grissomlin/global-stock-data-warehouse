# -*- coding: utf-8 -*-
import os, time, random, requests, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "us-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 💡 指向您的核心資料庫
DB_PATH = os.path.join(BASE_DIR, "us_stock_warehouse.db")

# ✅ 效能與時效設定
MAX_WORKERS = 5  # 美股量大，稍微提高，但需配合延遲
DATA_EXPIRY_SECONDS = 3600
LIST_THRESHOLD = 3000

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

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

def classify_security(name: str, is_etf: bool) -> str:
    """過濾掉權證、優先股、ETF 等非普通股標的"""
    if is_etf: return "Exclude"
    n_upper = str(name).upper()
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE", "PWT"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_us_stock_list():
    """從 Nasdaq 獲取最新美股清單並同步名稱至 stock_info"""
    all_items = []
    log("📡 正在從 Nasdaq 獲取最新美股清單與名稱...")
    
    # 讀取 Nasdaq 官方交易符號表
    urls = [
        "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
        "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt"
    ]
    
    conn = sqlite3.connect(DB_PATH)
    
    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            df = pd.read_csv(StringIO(r.text), sep="|")
            # 移除最後一行的檔案時間戳記
            df = df[df["Test Issue"] == "N"]
            
            # 判斷代號欄位名稱 (nasdaq 使用 Symbol, 其他交易所使用 NASDAQ Symbol)
            sym_col = "Symbol" if "nasdaqlisted" in url else "NASDAQ Symbol"
            name_col = "Security Name"
            etf_col = "ETF"
            
            for _, row in df.iterrows():
                name = str(row[name_col])
                is_etf = str(row[etf_col]) == "Y"
                
                if classify_security(name, is_etf) == "Common Stock":
                    symbol = str(row[sym_col]).strip().replace('$', '-')
                    # 💡 同步名稱到 stock_info
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, updated_at) VALUES (?, ?, ?)",
                                 (symbol, name, datetime.now().strftime("%Y-%m-%d")))
                    all_items.append((symbol, name))
            
            time.sleep(1) 
        except Exception as e:
            log(f"⚠️ 清單抓取失敗 ({url}): {e}")

    conn.commit()
    conn.close()
    
    unique_items = list(set(all_items))
    if len(unique_items) >= LIST_THRESHOLD:
        log(f"✅ 成功同步美股清單: {len(unique_items)} 檔")
        return unique_items
    return [("AAPL", "APPLE INC"), ("TSLA", "TESLA INC")]

def download_one(args):
    """單檔下載邏輯"""
    symbol, name, mode = args
    # 決定起點 (Hot: 2020 / Cold: 1962)
    start_date = "2020-01-01" if mode == 'hot' else "1962-01-02"
    
    try:
        # 美股建議 Jitter 稍長，避免大量併發被封
        time.sleep(random.uniform(0.5, 1.5))
        tk = yf.Ticker(symbol)
        hist = tk.history(start=start_date, auto_adjust=True, timeout=30)
        
        if hist is None or hist.empty:
            return {"symbol": symbol, "status": "empty"}
            
        hist.reset_index(inplace=True)
        hist.columns = [c.lower() for c in hist.columns]
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.strftime('%Y-%m-%d')
        
        df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
        df_final['symbol'] = symbol
        
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
    items = get_us_stock_list()
    if not items:
        log("❌ 無法取得名單，終止任務。")
        return

    log(f"🚀 開始下載 US ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載
    stats = {"success": 0, "empty": 0, "error": 0}
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"US({mode})下載中")
        
        for f in as_completed(futures):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()

    # 3. 優化資料庫
    log("掃描完成，執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 {MARKET_CODE} 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 成功: {stats['success']} | 📭 空資料: {stats['empty']} | ❌ 錯誤: {stats['error']}")

if __name__ == "__main__":
    # 測試執行
    run_sync(mode='hot')
