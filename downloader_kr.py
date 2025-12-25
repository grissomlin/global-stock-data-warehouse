# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr  # ✅ 使用韓國數據最強工具
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 獲取名單 (使用 FDR 徹底修復產業別) ==========

def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader 獲取韓股完整產業清單...")
    
    try:
        # 1. 一次抓取 KOSPI 和 KOSDAQ 的所有上市股票資訊
        df_krx = fdr.StockListing('KRX') # 包含所有市場
        
        conn = sqlite3.connect(DB_PATH)
        items = []
        samples = []

        # FDR 返回的欄位通常包含: Code, Name, Sector, Industry, ListingDate...
        for _, row in df_krx.iterrows():
            code_clean = str(row['Code']).strip().zfill(6)
            
            # 判斷市場後綴
            mkt = str(row['Market'])
            suffix = ".KS" if mkt == "KOSPI" else ".KQ"
            symbol = f"{code_clean}{suffix}"
            
            name = str(row['Name']).strip()
            # 💡 FDR 的 Sector 欄位非常準確
            sector = str(row['Sector']).strip() if pd.notna(row['Sector']) else "Other/Unknown"
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, mkt, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))
            if len(samples) < 5 and sector != "Other/Unknown":
                samples.append(f"   ✅ 成功對接: {symbol} | {name[:8]} | 產業: {sector}")
            
        conn.commit()
        conn.close()
        
        log(f"✅ 韓股名單導入成功: {len(items)} 檔 (產業別已修復)")
        for s in samples: print(s)
        return items

    except Exception as e:
        log(f"❌ FDR 獲取失敗: {e}")
        return []

# ========== 3. 批量下載與同步 (維持高效版) ==========

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
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)", 
                                 (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], r['volume']))
                success += 1
            except: continue
        conn.commit(); conn.close()
        return success
    except: return 0

def run_sync(mode='hot'):
    start_time = time.time()
    # 初始化資料庫
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS stock_prices (date TEXT, symbol TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, PRIMARY KEY (date, symbol))")
    conn.execute("CREATE TABLE IF NOT EXISTS stock_info (symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)")
    conn.close()

    items = get_kr_stock_list()
    if not items: return {"success": 0, "has_changed": False}

    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始韓股同步 | 總目標: {len(items)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_batch, b, mode): b for b in batches}
        for f in tqdm(as_completed(futures), total=len(batches), desc="KR同步"):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()

    log("🧹 資料庫優化...")
    conn = sqlite3.connect(DB_PATH); conn.execute("VACUUM"); conn.close()
    log(f"📊 同步完成！有效標的: {total_success} | 費時: {(time.time() - start_time)/60:.1f} 分鐘")
    return {"success": total_success, "total": len(items), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
