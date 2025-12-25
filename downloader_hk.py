# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 下載設定：港股建議低並發以確保成功率
MAX_WORKERS = 2 if IS_GITHUB_ACTIONS else 4 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫與清單獲取 ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

def get_hk_stock_list():
    """獲取港股清單並確保寫入 stock_info (修復解析問題)"""
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    log(f"📡 正在從港交所同步最新名單...")
    try:
        r = requests.get(url, headers=headers, timeout=20, verify=False)
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 💡 使用寬鬆比對尋找表頭
        hdr_idx = None
        for i in range(min(30, len(df_raw))):
            row_vals = [str(x).replace('\xa0', ' ').strip() for x in df_raw.iloc[i].values]
            if any("Stock Code" in val for val in row_vals):
                hdr_idx = i
                break
        
        if hdr_idx is None: raise ValueError("找不到 Excel 標題列")

        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = [str(x).replace('\xa0', ' ').strip() for x in df_raw.iloc[hdr_idx].values]
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        # 抓取 code 與 name 的正確欄位名
        code_col = [c for c in df.columns if "Stock Code" in c][0]
        name_col = [c for c in df.columns if "Short Name" in c][0]

        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            name = str(row[name_col]).strip()
            
            if raw_code.isdigit() and int(raw_code) < 10000:
                symbol = f"{raw_code.zfill(4)}.HK"
                
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, "Unknown", "HKEX", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
                
        conn.commit()
        conn.close()
        log(f"✅ 港股清單同步成功: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"⚠️ 名單抓取失敗: {e}，使用保底清單")
        return [("0700.HK", "TENCENT"), ("09988.HK", "BABA-SW"), ("00005.HK", "HSBC")]

# ========== 3. 單檔下載邏輯 (修復 yf.download 錯誤) ==========

def download_one(symbol, name, mode):
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    # 重試機制
    for attempt in range(3):
        try:
            # 💡 增加隨機延遲，防止 429 或 404 錯誤
            time.sleep(random.uniform(2.5, 4.5) if IS_GITHUB_ACTIONS else 0.5)
            
            # 使用 yf.Ticker 比較穩定
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, auto_adjust=True, timeout=20)
            
            if hist is None or hist.empty:
                continue
            
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            # 寫入資料庫
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False,
                            method=lambda t, c, k, d: c.executemany(
                                f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
            conn.close()
            return True
        except Exception as e:
            if attempt == 2: log(f"❌ {symbol} 下載失敗: {e}")
            time.sleep(10) # 錯誤後冷靜 10 秒
    return False

# ========== 4. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_hk_stock_list()
    if not items: return {"success": 0, "has_changed": False}

    log(f"🚀 開始同步港股 | 執行緒: {MAX_WORKERS}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, it[0], it[1], mode): it[0] for it in items}
        pbar = tqdm(total=len(items), desc="HK同步中")
        
        for f in as_completed(futures):
            if f.result():
                total_success += 1
            pbar.update(1)
        pbar.close()

    log("🧹 執行 VACUUM...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！成功: {total_success} 檔 | 費時: {duration:.1f} 分鐘")
    
    return {
        "success": total_success,
        "error": len(items) - total_success,
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
