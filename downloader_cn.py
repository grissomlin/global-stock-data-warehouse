# -*- coding: utf-8 -*-
import os, sys, sqlite3, time, random, io, re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "cn-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "cn_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 效能調優核心參數
BATCH_SIZE = 50        # 每批次處理 50 檔股票 (平衡速度與伺服器負荷)
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 7.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

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

# ========== 3. 獲取 A 股清單與產業別 (Akshare 強化) ==========

def get_cn_stock_list_with_sector():
    """從 Akshare 獲取清單並盡量補齊產業分類"""
    import akshare as ak
    log("📡 正在從 Akshare 同步 A 股清單與行業分類...")
    
    try:
        # 1. 獲取全體 A 股即時行情作為基礎清單
        df_spot = ak.stock_zh_a_spot_em()
        
        # 2. 獲取行業板塊資訊
        log("🔍 正在獲取行業分類對照表...")
        df_ind = ak.stock_board_industry_name_em()
        
        # 建立 股票代碼 -> 行業名稱 的映射字典
        sector_map = {}
        # 為了效率，我們抓取熱門行業的成分股來映射（這能補齊大部分正股）
        # 這裡只取前 50 個行業板塊以節省名單初始化時間
        for ind_name in df_ind['板块名称'].head(60).tolist():
            try:
                cons = ak.stock_board_industry_cons_em(symbol=ind_name)
                for code in cons['代码'].tolist():
                    sector_map[str(code).zfill(6)] = ind_name
            except:
                continue

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        
        for _, row in df_spot.iterrows():
            code = str(row['代码']).zfill(6)
            if not code.startswith(valid_prefixes): continue
            
            # 自動判斷市場
            symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
            market = "SSE" if code.startswith('6') else "SZSE"
            
            name = row['名称']
            sector = sector_map.get(code, "Unknown")
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 成功同步 A 股清單: {len(stock_list)} 檔 (包含行業映射)")
        return stock_list
    except Exception as e:
        log(f"❌ 獲取名單失敗: {e}")
        return []

# ========== 4. 批量下載邏輯 (速度優化核心) ==========

def download_batch_task(batch_items, mode):
    """執行批次下載與存檔"""
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2015-01-01"
    
    try:
        # 💡 使用批量下載，一次請求多檔股票
        data = yf.download(
            tickers=symbols,
            start=start_date,
            group_by='ticker',
            auto_adjust=True,
            threads=False, # 我們外部已使用線程池
            progress=False,
            timeout=40
        )
        
        if data.empty: return 0
        
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success_count = 0
        
        # 遍歷批次中的每個符號
        for symbol in symbols:
            try:
                # 處理 yf.download 可能回傳的多層或單層索引
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
                
                # 寫入價格
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
    except Exception as e:
        # log(f"批次下載異常: {e}")
        return 0

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    # 1. 獲取名單與產業
    items = get_cn_stock_list_with_sector()
    if not items:
        return {"success": 0, "has_changed": False}

    # 2. 切分批次 (Batching)
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始 A 股同步 | 總目標: {len(items)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch_task, b, mode): b for b in batches}
        
        pbar = tqdm(total=len(items), desc="CN數據同步")
        for f in as_completed(future_to_batch):
            # 💡 批次間隨機等待，避免被 Yahoo 封鎖 IP
            time.sleep(random.uniform(*BATCH_DELAY))
            
            res = f.result()
            total_success += res
            pbar.update(BATCH_SIZE)
        pbar.close()

    # 3. 資料庫優化
    log("🧹 執行資料庫優化 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    # 統計有效標的
    db_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！庫存總數: {db_count} | 本次更新: {total_success} | 費時: {duration:.1f} 分鐘")
    
    return {
        "success": total_success,
        "total": len(items),
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
