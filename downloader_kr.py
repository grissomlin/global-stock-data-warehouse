# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 主清單獲取（僅用 FDR）==========
def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader 獲取韓股清單（使用內建 Sector）...")
    
    try:
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 原始資料: {len(df_fdr)} 檔")

        conn = sqlite3.connect(DB_PATH)
        items = []
        samples = []

        for _, row in df_fdr.iterrows():
            code_clean = str(row['Code']).strip()
            if not code_clean.isdigit() or len(code_clean) != 6:
                continue

            mkt = str(row.get('Market', 'Unknown')).strip()
            suffix = ".KS" if mkt == "KOSPI" else ".KQ"
            symbol = f"{code_clean}{suffix}"
            name = str(row['Name']).strip()
            
            sector = "Other/Unknown"
            if pd.notna(row.get('Sector')):
                sector_raw = str(row['Sector']).strip()
                if sector_raw and sector_raw not in ['-', '', 'N/A', 'NaN']:
                    sector = sector_raw

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, mkt, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))
            if len(samples) < 5 and sector != "Other/Unknown":
                samples.append(f"   ✅ {symbol} | {name[:12]} | {sector}")

        conn.commit()
        conn.close()

        log(f"✅ 韓股清單整合成功: {len(items)} 檔")
        for s in samples: print(s)
        return items

    except Exception as e:
        log(f"❌ 清單整合失敗: {e}")
        return []

# ========== 3. 批量下載股價（保持不變）==========
def download_batch(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    try:
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, threads=False, progress=False, timeout=45)
        if data.empty: return 0
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0
        target_list = symbols if isinstance(symbols, list) else [symbols]
        for symbol in target_list:
            try:
                df = data[symbol].copy() if len(target_list) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                for _, r in df.iterrows():
                    vol = int(r['volume']) if pd.notna(r['volume']) else 0
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)", 
                                 (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], vol))
                success += 1
            except: continue
        conn.commit(); conn.close()
        return success
    except: return 0

# ========== 4. 初始化 DB & 主流程 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                        date TEXT, symbol TEXT, open REAL, high REAL, 
                        low REAL, close REAL, volume INTEGER,
                        PRIMARY KEY (date, symbol))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                        symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
    conn.close()

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        log("🛑 無有效股票清單，跳過同步")
        return {"success": 0, "total": 0, "has_changed": False}

    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始韓股同步 | 目標: {len(items)} 檔 | 批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_batch, b, mode): b for b in batches}
        for f in tqdm(as_completed(futures), total=len(batches), desc="KR同步"):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()

    log("🧹 資料庫優化...")
    conn = sqlite3.connect(DB_PATH); conn.execute("VACUUM"); conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！有效標的: {total_success} | 費時: {duration:.1f} 分鐘")
    return {"success": total_success, "total": len(items), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
