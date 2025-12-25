# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, requests, io, subprocess, re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定與套件檢查 ==========
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 效能設定：GitHub 環境建議執行緒稍微降低確保穩定
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 8 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_excel_tool():
    """確保能讀取舊版 .xls 格式"""
    try:
        import xlrd
    except ImportError:
        log("🔧 正在安裝 Excel 讀取組件 (xlrd)...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd"])

# ========== 2. 資料庫初始化 (具備自動升級) ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        # 💡 自動檢查並新增缺失的 market 欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 JP 資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取日股完整清單 (修正型態與欄位偵測) ==========

def get_jp_stock_list():
    """從 JPX 獲取包含英文產業別的清單，並修正代碼讀取錯誤"""
    ensure_excel_tool()
    
    # 優先使用英文版連結
    primary_url = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
    backup_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvlogs0000001qqy-att/data_j.xls"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html'
    }

    log(f"📡 正在從 JPX 同步最新名單...")
    
    df = None
    for url in [primary_url, backup_url]:
        try:
            resp = requests.get(url, headers=headers, timeout=30, verify=False)
            resp.raise_for_status()
            df = pd.read_excel(io.BytesIO(resp.content))
            log(f"✅ 成功連線至: {url.split('/')[-2]}")
            break
        except Exception as e:
            log(f"⚠️ 連結失敗: {e}")
            continue

    if df is None:
        log("❌ 無法取得日股名單")
        return []

    # 💡 智慧欄位偵測：解決網址變換後欄位名不同的問題
    col_map = {}
    for col in df.columns:
        c_str = str(col).lower()
        if 'local code' in c_str or ('code' in c_str and 'sector' not in c_str): col_map['symbol'] = col
        elif 'name' in c_str and 'english' in c_str: col_map['name'] = col
        elif 'sector' in c_str and 'name' in c_str: col_map['sector'] = col
        elif 'section' in c_str or 'market' in c_str: col_map['market'] = col

    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    # 取得最終要用的欄位標題，若沒偵測到則用範例中的常見位置
    c_code = col_map.get('symbol', df.columns[1])
    c_name = col_map.get('name', df.columns[2])
    c_sect = col_map.get('sector', df.columns[5])
    c_mark = col_map.get('market', df.columns[3])

    for _, row in df.iterrows():
        try:
            # 💡 修正 0 檔導入的關鍵：處理 Local Code 的 float 格式 (1301.0 -> 1301)
            raw_val = row[c_code]
            if pd.isna(raw_val): continue
            
            code_str = str(raw_val).split('.')[0].strip()
            
            # 只保留 4 位純數字的普通股
            if len(code_str) == 4 and code_str.isdigit():
                market_info = str(row[c_mark])
                if "ETFs" in market_info or "ETNs" in market_info:
                    continue
                
                symbol = f"{code_str}.T"
                name = str(row[c_name]).strip()
                sector = str(row[c_sect]).strip()
                
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, sector, market_info, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
        except:
            continue
            
    conn.commit()
    conn.close()
    log(f"✅ 日股清單導入成功，共計 {len(stock_list)} 檔普通股。")
    return stock_list

# ========== 4. 核心下載邏輯 (具備重試與壓平 MultiIndex) ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            time.sleep(random.uniform(0.1, 0.3))
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                if attempt < max_retries: continue
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # 💡 處理 yfinance 雙層表頭 Bug
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda t, c, k, d: c.executemany(
                                f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
            conn.close()
            return {"symbol": symbol, "status": "success"}
        except:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return {"symbol": symbol, "status": "error"}

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_jp_stock_list()
    if not items:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始執行日股同步 ({mode}) | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="JP同步"):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error": fail_list.append(res.get("symbol"))

    # 💡 修正回傳統計：查詢資料庫實有總數
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    final_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 資料庫目前總數: {final_count} | 本次新增: {stats['success']}")
    
    return {
        "success": final_count,
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": stats['success'] > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
