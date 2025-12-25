# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
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
BATCH_SIZE = 40        # 批量請求大小
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
            log("🔧 正在升級 US 資料庫結構：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取美股名單 (含精準過濾邏輯) ==========

def get_us_stock_list_official():
    """從 Nasdaq 官方 API 獲取清單，並過濾掉衍生品"""
    log("📡 正在從 Nasdaq 官方獲取美股名單與產業別...")
    
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
        
        # 💡 過濾條件：剔除常見衍生品關鍵字
        exclude_kw = re.compile(r"Warrant|Right|Preferred|Unit|ETF|Index|Index-linked", re.I)

        for row in rows:
            symbol = str(row.get('symbol', '')).strip().upper()
            
            # 💡 核心過濾：
            # 1. 排除代碼含特殊符號 (如 A/B 股通常帶斜線)
            # 2. 排除長度 > 4 且以 R, W, U 結尾的衍生品
            if not symbol or not symbol.isalnum(): continue
            if len(symbol) > 4 and (symbol.endswith('R') or symbol.endswith('W') or symbol.endswith('U')):
                continue
            
            name = str(row.get('name', 'Unknown')).strip()
            if exclude_kw.search(name): continue
            
            sector = str(row.get('sector', 'Unknown')).strip()
            market = str(row.get('exchange', 'Unknown')).strip()
            
            # 處理 API 空值
            if not sector or sector.lower() in ['nan', 'n/a', '']: 
                sector = "Unknown"

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 美股清單導入成功: {len(stock_list)} 檔 (已過濾衍生品)")
        return stock_list
        
    except Exception as e:
        log(f"❌ 獲取名單失敗: {e}")
        return []

# ========== 4. 批量下載邏輯 (效能優化核心) ==========

def download_batch_task(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    
    try:
        # 💡 使用批量模式請求 Yahoo Finance
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
        
        # 判斷是單一標的還是多標的回傳
        target_symbols = [symbols] if isinstance(symbols, str) else symbols

        for symbol in target_symbols:
            try:
                df = data[symbol].copy() if len(target_symbols) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                
                # 日期標準化
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
    
    # 1. 獲取並清洗名單
    items = get_us_stock_list_official()
    if not items:
        return {"success": 0, "has_changed": False}

    # 2. 執行分批同步
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

    # 3. 維護與統計
    log("🧹 執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    # 統計資料庫內真實的有效標的數
    db_info_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    db_price_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 名單總數: {db_info_count} | 成功獲取價格標的: {db_price_count}")
    
    return {
        "success": db_price_count,
        "total": db_info_count,
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
