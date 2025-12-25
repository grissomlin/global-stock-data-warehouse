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

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 大幅縮減等待時間，並提高並發數
MAX_WORKERS = 6 if IS_GITHUB_ACTIONS else 10 
WAIT_TIME_RANGE = (0.3, 0.8) if IS_GITHUB_ACTIONS else (0.1, 0.3)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 輔助函式 ==========

def format_hk_symbol(raw_code):
    """確保代碼為 4 位數補零 (例如 700 -> 0700.HK)"""
    digits = re.sub(r"\D", "", str(raw_code))
    if not digits: return None
    return f"{digits[-4:].zfill(4)}.HK"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        # 自動檢查並新增 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

def get_hk_stock_list():
    """獲取港股清單並過濾掉人民幣櫃台 (-R) 與非正股"""
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    log(f"📡 正在從港交所獲取清單...")
    try:
        r = requests.get(url, headers=headers, timeout=20, verify=False)
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        hdr_idx = None
        for i in range(min(20, len(df_raw))):
            row_str = " ".join([str(x) for x in df_raw.iloc[i].values])
            if "Stock Code" in row_str:
                hdr_idx = i
                break
        
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].values
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        # 排除衍生品與人民幣櫃台的關鍵字
        bad_kw = r"CBBC|WARRANT|RIGHTS|ETF|ETN|REIT|BOND|TRUST|FUND|牛熊|權證|輪證|-R|SWR"

        for _, row in df.iterrows():
            code = str(row['Stock Code']).strip()
            name = str(row.get('English Stock Short Name', 'Unknown')).strip()
            
            # 過濾 1-9999 的正股
            if code.isdigit() and int(code) < 10000 and not re.search(bad_kw, name, re.I):
                symbol = format_hk_symbol(code)
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
        log(f"⚠️ 名單抓取失敗: {e}，改用保底名單")
        return [("0700.HK", "TENCENT"), ("9988.HK", "BABA"), ("0005.HK", "HSBC")]

# ========== 3. 核心下載邏輯 ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    # 💡 增加重試機制
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            # 🚀 大幅縮短等待時間
            time.sleep(random.uniform(*WAIT_TIME_RANGE))
            
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, auto_adjust=True, timeout=20)
            
            if hist is None or hist.empty:
                # 備案：嘗試使用 yf.download 繞過
                hist = yf.download(symbol, start=start_date, auto_adjust=True, progress=False, timeout=20)

            if hist is None or hist.empty:
                if attempt < max_retries: continue
                return False
                
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False,
                            method=lambda t, c, k, d: c.executemany(
                                f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
            conn.close()
            return True
        except:
            if attempt < max_retries:
                time.sleep(2)
                continue
    return False

# ========== 4. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_hk_stock_list()
    if not items: return {"success": 0, "has_changed": False}

    log(f"🚀 開始港股同步 | 執行緒: {MAX_WORKERS} | 目標: {len(items)} 檔")

    total_success = 0
    # 💡 移除預篩流程，直接併發下載
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        pbar = tqdm(total=len(items), desc="HK同步")
        
        for f in as_completed(futures):
            if f.result():
                total_success += 1
            pbar.update(1)
        pbar.close()

    # 💡 修正回傳統計：查詢不重複總數
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    db_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    log(f"📊 同步完成！庫存總數: {db_count} | 費時: {(time.time()-start_time)/60:.1f} 分鐘")
    
    return {
        "success": db_count,
        "total": len(items),
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
