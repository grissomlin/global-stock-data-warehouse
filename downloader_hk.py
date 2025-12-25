# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

# ✅ 效能優化參數
BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.0)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

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

# ========== 2. 獲取名單 ==========

def normalize_code_5d(val) -> str:
    digits = re.sub(r"\D", "", str(val))
    if digits.isdigit() and 1 <= int(digits) <= 99999:
        return digits.zfill(5)
    return ""

def get_hk_stock_list():
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    log("📡 正在從港交所下載股票清單...")
    try:
        r = requests.get(url, timeout=30, verify=False)
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 尋找表頭
        header_row = 0
        for i in range(20):
            row_vals = [str(x) for x in df_raw.iloc[i].values]
            if any("Stock Code" in v for v in row_vals):
                header_row = i
                break
        
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
                # 這裡 sector 先維持原本的，後續下載時會補齊
                conn.execute("INSERT OR IGNORE INTO stock_info (symbol, name, sector, market, updated_at) VALUES (?, ?, ?, ?, ?)",
                             (code_5d, name, "Unknown", "HKEX", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append(code_5d)
        conn.commit()
        conn.close()
        return stock_list
    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        return []

# ========== 3. 批量下載與產業補完 ==========

def download_batch_task(codes_batch, mode):
    # 港股在 Yahoo 必須是 00700.HK 格式
    yahoo_symbols = [f"{c}.HK" for c in codes_batch]
    start_date = "2020-01-01" if mode == "hot" else "2010-01-01"
    
    try:
        # 💡 使用批量下載並請求 info (用於補齊產業)
        tickers = yf.Tickers(" ".join(yahoo_symbols))
        success_count = 0
        conn = sqlite3.connect(DB_PATH, timeout=60)

        for code_5d in codes_batch:
            sym = f"{code_5d}.HK"
            try:
                tk = tickers.tickers[sym]
                # 1. 下載歷史數據
                hist = tk.history(start=start_date, auto_adjust=True)
                if not hist.empty:
                    hist = hist.reset_index()
                    hist.columns = [c.lower() for c in hist.columns]
                    hist['date_str'] = pd.to_datetime(hist['date']).dt.strftime('%Y-%m-%d')
                    
                    for _, r in hist.iterrows():
                        conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                                     (r['date_str'], code_5d, r['open'], r['high'], r['low'], r['close'], r['volume']))
                    
                    # 2. 💡 補齊產業別 (Sector)
                    # 只有當目前為 Unknown 時才去抓，節省效能
                    cursor = conn.execute("SELECT sector FROM stock_info WHERE symbol = ?", (code_5d,))
                    current_sector = cursor.fetchone()[0]
                    if current_sector == "Unknown":
                        sector = tk.info.get('sector', 'Unknown')
                        if sector != 'Unknown':
                            conn.execute("UPDATE stock_info SET sector = ? WHERE symbol = ?", (sector, code_5d))
                    
                    success_count += 1
            except:
                continue
        
        conn.commit()
        conn.close()
        return success_count
    except:
        return 0

# ========== 4. 主流程 ==========

def run_sync(mode="hot"):
    start_time = time.time()
    init_db()
    codes = get_hk_stock_list()
    if not codes: return {"success": 0, "has_changed": False}

    batches = [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    log(f"🚀 開始港股同步 | 目標: {len(codes)} 檔 | 總批次: {len(batches)}")

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
    # 統計產業別覆蓋率
    unknown_cnt = conn.execute("SELECT COUNT(*) FROM stock_info WHERE sector = 'Unknown'").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 港股完成！費時: {duration:.1f} 分鐘 | 剩餘 Unknown: {unknown_cnt}")
    
    return {"success": total_success, "total": len(codes), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync(mode="hot")
