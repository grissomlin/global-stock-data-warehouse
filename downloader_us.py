# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re  # ✅ 補上 re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境與參數設定 ==========
MARKET_CODE = "us-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "us_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 下載效率核心參數
BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.0)

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

# ========== 3. 獲取美股官方清單 (含產業與市場) ==========

def get_us_stock_list_official():
    log("📡 正在向 Nasdaq 官方 API 請求全體美股清單...")
    
    url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=15000&download=true"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://www.nasdaq.com/market-activity/stocks/screener'
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        data_json = r.json()
        rows = data_json['data']['rows']
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        # 過濾排除字眼 (ETF, 權證等)
        exclude_pattern = re.compile(r"Warrant|Right|Preferred|Unit|ETF", re.I)

        for row in rows:
            symbol = str(row.get('symbol', '')).strip().upper()
            # 排除非標準代碼
            if not symbol or not symbol.isalpha(): continue
            
            name = str(row.get('name', 'Unknown')).strip()
            if exclude_pattern.search(name): continue
            
            sector = str(row.get('sector', 'Unknown')).strip()
            market = str(row.get('exchange', 'Unknown')).strip()
            
            if not sector or sector.lower() == 'nan': sector = 'Unknown'

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 美股清單導入成功: {len(stock_list)} 檔")
        return stock_list
        
    except Exception as e:
        log(f"❌ 官方 API 獲取失敗: {e}")
        return []

# ========== 4. 批量下載邏輯 ==========

def download_batch_task(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    
    try:
        # 批量下載提高效率
        data = yf.download(
            tickers=symbols,
            start=start_date,
            group_by='ticker',
            auto_adjust=True,
            threads=False,
            progress=False,
            timeout=40
        )
        
        if data.empty: return 0
        
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success_count = 0
        
        # 處理 yfinance 多股票下載返回的 MultiIndex 結構
        for symbol in symbols:
            try:
                if len(symbols) > 1:
                    df = data[symbol].copy()
                else:
                    df = data.copy()
                
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                
                # 取得日期
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                
                for _, row in df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_prices (date, symbol, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (row['date_str'], symbol, row['open'], row['high'], row['low'], row['close'], row['volume']))
                success_count += 1
            except:
                continue
        
        conn.commit()
        conn.close()
        return success_count
    except:
        return 0

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_us_stock_list_official()
    if not items:
        return {"success": 0, "has_changed": False}

    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始美股批量同步 | 總目標: {len(items)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch_task, b, mode): b for b in batches}
        
        pbar = tqdm(total=len(items), desc="US數據同步")
        for f in as_completed(future_to_batch):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()
            pbar.update(BATCH_SIZE)
        pbar.close()

    log("🧹 執行資料庫優化 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    db_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！有效標的總數: {db_count} | 本次更新: {total_success} | 費時: {duration:.1f} 分鐘")
    
    return {
        "success": total_success,
        "total": len(items),
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
