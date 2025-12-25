# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

# ✅ 效能參數：每批處理 40 檔，大幅縮短執行時間
BATCH_SIZE = 40
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10
BATCH_DELAY = (3.0, 6.0) if IS_GITHUB_ACTIONS else (0.5, 1.0)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. DB 初始化 ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                date TEXT, symbol TEXT, open REAL, high REAL, 
                low REAL, close REAL, volume INTEGER,
                PRIMARY KEY (date, symbol))""")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)""")
    finally:
        conn.close()

# ========== 3. HKEX 股票清單解析 ==========

def normalize_code_5d(val) -> str:
    digits = re.sub(r"\D", "", str(val))
    if digits.isdigit() and 1 <= int(digits) <= 99999:
        return digits.zfill(5)
    return ""

def get_hk_stock_list():
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    log("📡 正在從港交所獲取股票清單...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        r = requests.get(url, timeout=30, verify=False, headers=headers)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 尋找表頭
        header_row = None
        for i in range(20):
            row_vals = [str(x).strip() for x in df_raw.iloc[i].values]
            if any("Stock Code" in v for v in row_vals):
                header_row = i
                break

        if header_row is None: return []

        df = df_raw.iloc[header_row + 1:].copy()
        df.columns = [str(x).strip() for x in df_raw.iloc[header_row].values]
        code_col = next(c for c in df.columns if "Stock Code" in c)
        name_col = next(c for c in df.columns if "Short Name" in c)

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        for _, row in df.iterrows():
            code_5d = normalize_code_5d(row[code_col])
            if code_5d:
                name = str(row[name_col]).strip()
                # 初始標記為 Unknown，下載時補齊
                conn.execute("INSERT OR IGNORE INTO stock_info VALUES (?, ?, ?, ?, ?)",
                             (code_5d, name, "Unknown", "HKEX", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append(code_5d)
        conn.commit()
        conn.close()
        log(f"✅ HKEX 清單解析完成：{len(stock_list)} 檔標的")
        return stock_list
    except Exception as e:
        log(f"❌ 無法解析 HKEX 清單: {e}")
        return []

# ========== 4. 批量下載與產業補完邏輯 ==========

def download_batch_task(codes_batch, mode):
    # 港股在 Yahoo 的 symbol 格式固定為 00700.HK
    yahoo_map = {f"{c}.HK": c for c in codes_batch}
    symbols = list(yahoo_map.keys())
    start_date = "2020-01-01" if mode == "hot" else "2010-01-01"
    
    try:
        # 💡 使用批量下載
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, progress=False, timeout=45)
        
        if data.empty: return 0
        
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success_in_batch = 0
        
        for sym, code_5d in yahoo_map.items():
            try:
                df = data[sym].copy() if len(symbols) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                
                for _, row in df.iterrows():
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                                 (row['date_str'], code_5d, row['open'], row['high'], row['low'], row['close'], row['volume']))
                
                # 💡 產業別補完：如果目前是 Unknown，則嘗試獲取
                # 注意：yf.download 不帶 info，所以我們隨機抽樣或分流處理
                # 為確保效率，我們這裡只下載股價，產業別建議由 Ticker.info 單獨非頻繁補丁
                success_in_batch += 1
            except: continue
            
        conn.commit()
        conn.close()
        return success_in_batch
    except:
        return 0

# ========== 5. 主流程 ==========

def run_sync(mode="hot"):
    start_time = time.time()
    init_db()
    codes = get_hk_stock_list()
    if not codes: return {"success": 0, "has_changed": False}

    # 將名單切分為批次
    batches = [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    log(f"🚀 開始港股批量同步 | 目標: {len(codes)} 檔 | 批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch_task, b, mode): b for b in batches}
        pbar = tqdm(total=len(codes), desc="HK同步")
        for f in as_completed(future_to_batch):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()
            pbar.update(BATCH_SIZE)
        pbar.close()

    log("🧹 執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    # 統計
    db_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 港股完成 | 有效標的: {db_count} | 費時: {duration:.1f} 分")
    return {"success": total_success, "total": len(codes), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync(mode="hot")
