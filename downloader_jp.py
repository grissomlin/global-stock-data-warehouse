# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, requests, io, subprocess, re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 5 

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_excel_tool():
    try:
        import xlrd
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd"])

# ========== 2. 資料庫初始化 ==========

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

# ========== 3. 獲取日股清單 (具備網址失效對策) ==========

def get_jp_stock_list():
    ensure_excel_tool()
    
    # 💡 您提供的最新英文版 XLS 連結
    primary_url = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
    # 💡 備份連結 (日文版常規路徑)
    backup_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvlogs0000001qqy-att/data_j.xls"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html'
    }

    log(f"📡 正在嘗試從 JPX 下載清單...")
    
    df = None
    for url in [primary_url, backup_url]:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            df = pd.read_excel(io.BytesIO(resp.content))
            log(f"✅ 成功連線至: {url.split('/')[-2]}")
            break
        except Exception as e:
            log(f"⚠️ 連結失敗 ({url.split('/')[-2]}): {e}")
            continue

    if df is None:
        log("❌ 所有日股清單連結皆失效，請手動更新程式碼中的 URL。")
        return []

    # 💡 欄位名稱映射邏輯：自動偵測關鍵字，防止網址更換後欄位順序跑掉
    col_map = {}
    for col in df.columns:
        c_str = str(col).lower()
        if 'code' in c_str and 'sector' not in c_str: col_map['symbol'] = col
        elif 'name' in c_str and 'english' in c_str: col_map['name'] = col
        elif 'sector' in c_str and 'name' in c_str: col_map['sector'] = col
        elif 'section' in c_str or 'market' in c_str: col_map['market'] = col

    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    for _, row in df.iterrows():
        try:
            # 取得代碼 (Local Code)
            raw_code = str(row.get(col_map.get('symbol', 'Local Code'), '')).strip()
            
            # 過濾：只抓 4 位數純數字 (排除 130A 這種 PRO Market 或 ETF)
            if len(raw_code) == 4 and raw_code.isdigit():
                # 排除 ETF / ETN (這份文件裡會標註在 Section/Products 欄位)
                market_info = str(row.get(col_map.get('market', 'Section/Products'), ''))
                if "ETFs" in market_info or "ETNs" in market_info:
                    continue
                
                symbol = f"{raw_code}.T"
                name = str(row.get(col_map.get('name', 'Name (English)'), 'Unknown')).strip()
                sector = str(row.get(col_map.get('sector', '33 Sector(name)'), 'Unknown')).strip()
                
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, sector, market_info, datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
        except:
            continue
            
    conn.commit()
    conn.close()
    log(f"✅ 日股清單導入成功，總計 {len(stock_list)} 檔普通股。")
    return stock_list

# ========== 4. 下載與主流程 (與之前邏輯一致) ==========

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
            return {"symbol": symbol, "status": "success"}
        except:
            if attempt < max_retries:
                time.sleep(2)
                continue
            return {"symbol": symbol, "status": "error"}

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    items = get_jp_stock_list()
    if not items: return {"success": 0, "has_changed": False}

    log(f"🚀 開始執行日股同步 ({mode}) | 目標: {len(items)} 檔")
    stats = {"success": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, it): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="JP同步"):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1

    conn = sqlite3.connect(DB_PATH)
    final_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    log(f"📊 同步完成！資料庫總數: {final_count} | 費時: {(time.time() - start_time)/60:.1f} 分鐘")
    return {"success": final_count, "total": len(items), "has_changed": stats['success'] > 0}

if __name__ == "__main__":
    run_sync(mode='hot')
