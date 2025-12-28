# -*- coding: utf-8 -*-
"""
downloader_kr.py
----------------
韓股資料下載器（穩定單執行緒版）

✔ 改為單執行緒循環：徹底解決 yfinance 批量下載時的記憶體衝突
✔ 整合 KIND & FDR：獲取最準確的韓國產業分類 (業種)
✔ 日期標準化：自動處理 KST 時區問題，確保 DB 格式統一
"""

import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. KIND 產業資料抓取 ==========
def fetch_kind_industry_map():
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    log("📡 正在從 KIND 下載韓股權威產業對照表...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        dfs = pd.read_html(io.BytesIO(r.content))
        if not dfs: return {}
        
        df = dfs[0]
        industry_map = {}
        for _, row in df.iterrows():
            code = str(row['종목코드']).strip().zfill(6)
            sector = str(row['업종']).strip()
            industry_map[code] = sector
        return industry_map
    except Exception as e:
        log(f"⚠️ KIND 抓取失敗: {e}")
        return {}

# ========== 3. 資料庫與清單初始化 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
    finally:
        conn.close()

def get_kr_stock_list():
    log("📡 正在獲取完整韓股清單...")
    try:
        df_fdr = fdr.StockListing('KRX')
        kind_map = fetch_kind_industry_map()

        conn = sqlite3.connect(DB_PATH)
        items = []
        
        for _, row in df_fdr.iterrows():
            code = str(row['Code']).strip().zfill(6)
            market = str(row.get('Market', 'Unknown'))
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code}{suffix}"
            name = str(row['Name']).strip()

            sector = kind_map.get(code)
            if not sector:
                sector = str(row.get('Sector', 'Other/Unknown')).strip()

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            items.append((symbol, name))

        conn.commit()
        conn.close()
        log(f"✅ 韓股清單整合成功: {len(items)} 檔")
        return items
    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        return []

# ========== 4. 下載核心 (單執行緒穩定版) ==========
def download_one_kr(symbol, mode):
    start_date = "2023-01-01" if mode == 'hot' else "2010-01-01"
    max_retries = 2
    
    for attempt in range(max_retries + 1):
        try:
            # 💡 核心修正：threads=False 徹底防止記憶體錯亂
            df = yf.download(symbol, start=start_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=30)
            
            if df is None or df.empty:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            # 標準化日期 (處理韓國時區)
            date_col = 'date' if 'date' in df.columns else df.columns[0]
            df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df_final['symbol'] = symbol
            
            return df_final
        except Exception:
            if attempt < max_retries:
                time.sleep(3)
                continue
            return None

# ========== 5. 主程序 ==========
def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始韓股同步 (安全模式) | 目標: {len(items)} 檔")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 單執行緒循環下載
    pbar = tqdm(items, desc="KR同步")
    for symbol, name in pbar:
        df_res = download_one_kr(symbol, mode)
        
        if df_res is not None:
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
            
        # 🟢 控制下載頻率，保護 API
        time.sleep(0.05)

    conn.commit()
    
    log("🧹 執行資料庫 VACUUM...")
    conn.execute("VACUUM")
    conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 韓股完成 | 更新成功: {success_count} / {len(items)} | 耗時: {duration:.1f} 分鐘")
    
    return {"success": success_count, "total": len(items), "has_changed": success_count > 0}

if __name__ == "__main__":
    run_sync(mode='hot')

