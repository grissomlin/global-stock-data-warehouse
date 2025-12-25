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

# ✅ 效能優化參數
BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫初始化 ==========

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
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    log("📡 正在從港交所獲取最新股票清單...")
    
    try:
        r = requests.get(url, timeout=30, verify=False, headers=headers)
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
                # 初始標記為 Unknown，稍後執行補丁
                conn.execute("INSERT OR IGNORE INTO stock_info VALUES (?, ?, ?, ?, ?)",
                             (code_5d, name, "Unknown", "HKEX", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append(code_5d)
        conn.commit()
        conn.close()
        log(f"✅ HKEX 清單解析完成：{len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 港股清單解析失敗: {e}")
        return []

# ========== 4. 批量下載核心邏輯 ==========

def download_batch_task(codes_batch, mode):
    yahoo_map = {f"{c}.HK": c for c in codes_batch}
    symbols = list(yahoo_map.keys())
    start_date = "2020-01-01" if mode == "hot" else "2010-01-01"
    
    try:
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, progress=False, timeout=45)
        
        if data.empty: return 0
        
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success_in_batch = 0
        
        current_symbols = symbols if len(symbols) > 1 else [symbols[0]]
        for sym in current_symbols:
            try:
                df = data[sym].dropna(how='all')
                if df.empty: continue
                
                code_5d = yahoo_map[sym]
                df = df.reset_index()
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                
                for _, row in df.iterrows():
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                                 (row['date_str'], code_5d, row['open'], row['high'], row['low'], row['close'], row['volume']))
                success_in_batch += 1
            except: continue
            
        conn.commit(); conn.close()
        return success_in_batch
    except:
        return 0

# ========== 5. 智慧產業別引擎 (Sector Engine) ==========

def apply_sector_engine():
    """使用智慧關鍵字規則引擎對 2000 檔港股進行分類"""
    log("🔧 啟動智慧產業引擎 (Sector Engine)...")
    
    rules = {
        "Financial Services": ["銀行", "保險", "證券", "金融", "Bank", "Insurance", "Finance"],
        "Technology": ["科技", "軟件", "芯片", "半導體", "電訊", "Tech", "Software"],
        "Healthcare": ["醫藥", "生物", "醫療", "健康", "Health", "Pharma"],
        "Real Estate": ["地產", "物業", "發展", "物管", "Property", "Estate"],
        "Consumer Discretionary": ["汽車", "零售", "服飾", "教育", "娛樂", "Retail", "Auto"],
        "Energy": ["石油", "煤炭", "能源", "採礦", "Oil", "Energy"],
        "Utilities": ["電力", "燃氣", "水務", "Power", "Gas", "Water"],
        "Industrials": ["工業", "機械", "運輸", "航運", "Logistic", "Industrial"]
    }
    
    # 權值股精準映射
    precise_patch = {
        "00700": "Communication Services", "09988": "Consumer Discretionary", 
        "03690": "Consumer Discretionary", "01810": "Technology",
        "00005": "Financial Services", "01299": "Financial Services"
    }

    conn = sqlite3.connect(DB_PATH)
    try:
        # 1. 執行精準映射
        for code, sector in precise_patch.items():
            conn.execute("UPDATE stock_info SET sector = ? WHERE symbol = ?", (sector, code))
        
        # 2. 執行規則引擎
        cursor = conn.execute("SELECT symbol, name FROM stock_info WHERE sector = 'Unknown'")
        unknowns = cursor.fetchall()
        for symbol, name in unknowns:
            matched = "Hong Kong Equity" # 預設分類
            for sector, keywords in rules.items():
                if any(k in name for k in keywords):
                    matched = sector
                    break
            conn.execute("UPDATE stock_info SET sector = ? WHERE symbol = ?", (matched, symbol))
        
        conn.commit()
        log("✅ 港股產業分類已完成 (包含智慧規則匹配)")
    finally:
        conn.close()

# ========== 6. 主流程 ==========

def run_sync(mode="hot"):
    start_time = time.time()
    init_db()
    
    codes = get_hk_stock_list()
    if not codes: return {"success": 0, "has_changed": False}

    # 執行批量下載股價
    batches = [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    log(f"🚀 開始港股高速同步 | 目標: {len(codes)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch_task, b, mode): b for b in batches}
        pbar = tqdm(total=len(codes), desc="HK同步")
        for f in as_completed(future_to_batch):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()
            pbar.update(BATCH_SIZE)
        pbar.close()

    # 🔥 關鍵：執行產業引擎補完 Unknown
    apply_sector_engine()

    log("🧹 執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH); conn.execute("VACUUM"); conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 港股完成！費時: {duration:.1f} 分鐘")
    
    return {"success": total_success, "total": len(codes), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync(mode="hot")
