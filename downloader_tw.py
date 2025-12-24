# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "tw-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 💡 指向您的核心資料庫
DB_PATH = os.path.join(BASE_DIR, "tw_stock_warehouse.db")

# ✅ 效能與穩定性設定
MAX_WORKERS = 3  # 台股對 yfinance 請求較敏感，維持低執行緒
DATA_EXPIRY_SECONDS = 3600

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

def get_tw_stock_list():
    """從證交所獲取全市場清單並同步寫入 stock_info"""
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    log("📡 正在獲取台股全市場 (上市/上櫃/興櫃) 名單與名稱...")
    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                
                if code and '有價證券' not in code:
                    symbol = f"{code}{cfg['suffix']}"
                    # 💡 同步名稱與產業資訊到 stock_info
                    conn.execute("INSERT OR REPLACE INTO stock_info (symbol, name, sector, updated_at) VALUES (?, ?, ?, ?)",
                                 (symbol, name, sector, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        except Exception as e:
            log(f"⚠️ 獲取 {cfg['name']} 市場失敗: {e}")
            
    conn.commit()
    conn.close()
    
    unique_items = list(set(stock_list))
    log(f"✅ 台股清單同步完成，總計標的: {len(unique_items)} 檔")
    return unique_items

def download_one(args):
    """單檔下載邏輯"""
    symbol, name, mode = args
    # 決定起點 (Hot: 2020 / Cold: 1993)
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    
    try:
        time.sleep(random.uniform(0.8, 1.5))
        tk = yf.Ticker(symbol)
        # 台股下載建議開啟 auto_adjust=True
        hist = tk.history(start=start_date, auto_adjust=True, timeout=25)
        
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
    items = get_tw_stock_list()
    if not items:
        log("❌ 無法取得名單，終止。")
        return

    log(f"🚀 開始下載 TW ({mode.upper()} 模式)，目標: {len(items)} 檔")

    # 2. 多執行緒下載
    stats = {"success": 0, "empty": 0, "error": 0}
    task_args = [(it[0], it[1], mode) for it in items]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, arg): arg for arg in task_args}
        pbar = tqdm(total=len(items), desc=f"TW({mode})下載中")
        
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
