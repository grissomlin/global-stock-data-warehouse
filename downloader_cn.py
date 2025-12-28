# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "cn-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "cn_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

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

# ========== 3. 獲取 A 股清單 (穩定版) ==========
def get_cn_stock_list_with_sector():
    import akshare as ak
    log("📡 正在獲取 A 股清單...")
    
    try:
        # 獲取全體 A 股即時行情
        df_spot = ak.stock_zh_a_spot_em()
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        # 只取主要的板塊：主板、創業板、科創板
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        
        for _, row in df_spot.iterrows():
            code = str(row['代码']).zfill(6)
            if not code.startswith(valid_prefixes): continue
            
            # Yahoo Finance A股格式：上海 .SS, 深圳 .SZ
            symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
            market = "SSE" if code.startswith('6') else "SZSE"
            name = row['名称']
            
            # 簡化行業獲取，若無映射則標註為 A-Share
            sector = "A-Share"
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 成功取得 A 股清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 獲取名單失敗: {e}")
        return []

# ========== 4. 核心下載邏輯 (單執行緒穩定版) ==========
def download_one_cn(symbol, mode):
    start_date = "2023-01-01" if mode == 'hot' else "2015-01-01"
    max_retries = 1
    
    for attempt in range(max_retries + 1):
        try:
            # 💡 關鍵修正：threads=False 徹底防止記憶體錯亂，禁止批量模式
            df = yf.download(symbol, start=start_date, progress=False, timeout=25, 
                             auto_adjust=True, threads=False)
            
            if df is None or df.empty:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None
            
            # 處理 MultiIndex 結構 (單檔下載有時也會觸發)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            # 取得日期欄位名稱
            date_col = 'date' if 'date' in df.columns else df.columns[0]
            df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            
            df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df_final['symbol'] = symbol
            
            return df_final
        except:
            if attempt < max_retries:
                time.sleep(3)
                continue
            return None

# ========== 5. 主流程 ==========
def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_cn_stock_list_with_sector()
    if not items:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始 CN 數據同步 (安全模式) | 目標: {len(items)} 檔")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 💡 採用穩定單執行緒循環，徹底解決數據混淆問題
    pbar = tqdm(items, desc="CN同步")
    for symbol, name in pbar:
        df_res = download_one_cn(symbol, mode)
        
        if df_res is not None:
            # 寫入資料庫
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
        
        # 🟢 稍微延遲，避開頻率限制
        time.sleep(0.05)
    
    conn.commit()

    # 優化與統計
    log("🧹 執行資料庫優化 (VACUUM)...")
    conn.execute("VACUUM")
    db_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！庫存總數: {db_count} | 更新成功: {success_count} | 費時: {duration:.1f} 分鐘")
    
    return {
        "success": success_count,
        "total": len(items),
        "has_changed": success_count > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')

