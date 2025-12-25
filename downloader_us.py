# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "us-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "us_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 速度優化設定
BATCH_SIZE = 50        # 每批次處理 50 檔股票 (平衡速度與被封風險)
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
# 批次間的等待時間
BATCH_DELAY = (3.0, 7.0) if IS_GITHUB_ACTIONS else (0.5, 1.0)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

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
        
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取帶有產業別的美股名單 (名單優化) ==========

def get_us_stock_list_with_sectors():
    """獲取美股名單並直接映射產業別"""
    log("📡 正在獲取美股官方清單與產業對照表...")
    
    # 來源：高品質的開源美股字典庫
    ref_url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.csv"
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(ref_url, headers=headers, timeout=20)
        df_ref = pd.read_csv(io.StringIO(r.text))
        
        # 準備寫入 stock_info
        conn = sqlite3.connect(DB_PATH)
        items = []
        
        # 過濾常見非正股關鍵字
        exclude_kw = r"Warrant|Right|Preferred|Wrt|Unit"
        df_clean = df_ref[~df_ref['Name'].str.contains(exclude_kw, na=False, case=False)]
        
        for _, row in df_clean.iterrows():
            symbol = str(row['Ticker']).strip().upper()
            name = str(row['Name']).strip()
            sector = str(row.get('Sector', 'Unknown'))
            market = str(row.get('Exchange', 'Unknown'))
            
            if sector == 'nan': sector = 'Unknown'
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            items.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 美股清單同步成功: {len(items)} 檔 (已帶入產業資訊)")
        return items
    except Exception as e:
        log(f"⚠️ 產業名單獲取失敗: {e}，使用備援機制")
        return [("AAPL", "Apple"), ("TSLA", "Tesla")]

# ========== 4. 批量下載下載邏輯 (速度提升 20 倍的關鍵) ==========

def download_batch(symbols_batch, mode):
    """批量下載 50 檔股票數據"""
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    
    try:
        # 💡 使用 yf.download 進行批量請求
        data = yf.download(
            tickers=symbols_batch,
            start=start_date,
            group_by='ticker',
            auto_adjust=True,
            threads=False, # 內部不開執行緒，由我們外部控制
            progress=False,
            timeout=30
        )
        
        if data.empty: return 0
        
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success_in_batch = 0
        
        # 處理下載回來的數據
        for symbol in symbols_batch:
            try:
                # 取得該檔股票的 DF
                if len(symbols_batch) > 1:
                    df = data[symbol].copy()
                else:
                    df = data.copy()
                
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                
                # 標準化日期
                df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                df['symbol'] = symbol
                
                # 寫入資料庫
                final_df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
                final_df.to_sql('stock_prices', conn, if_exists='append', index=False,
                                method=lambda t, c, k, d: c.executemany(
                                    f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
                success_in_batch += 1
            except:
                continue
                
        conn.close()
        return success_in_batch
    except Exception as e:
        log(f"⚠️ 批次下載異常: {e}")
        return 0

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_us_stock_list_with_sectors()
    symbols = [it[0] for it in items]
    
    # 將清單分成 BATCH_SIZE 一組
    batches = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    log(f"🚀 開始美股批量同步 ({mode.upper()}) | 總共 {len(batches)} 個批次")

    total_success = 0
    
    # 併發執行批次
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch, b, mode): b for b in batches}
        
        pbar = tqdm(total=len(symbols), desc="US同步中")
        for f in as_completed(future_to_batch):
            # 💡 每個批次完成後增加隨機等待，防止 IP 被封
            time.sleep(random.uniform(*BATCH_DELAY))
            
            res = f.result()
            total_success += res
            pbar.update(BATCH_SIZE)
        pbar.close()

    # 優化
    log("🧹 執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！成功標的: {total_success} | 費時: {duration:.1f} 分鐘")
    
    return {
        "success": total_success,
        "total": len(symbols),
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
