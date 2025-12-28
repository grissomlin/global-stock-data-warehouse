# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from tqdm import tqdm

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "tw-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "tw_stock_warehouse.db")

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
            log("🔧 正在升級資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取台股清單 ==========
def get_tw_stock_list():
    market_map = {'listed': '上市', 'otc': '上櫃', 'etf': 'ETF', 'rotc': '興櫃'}
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    log(f"📡 獲取台股清單並同步資訊...")
    conn = sqlite3.connect(DB_PATH)
    stock_list = []
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            dfs = pd.read_html(StringIO(resp.text), header=0)
            if not dfs: continue
            df = dfs[0]
            market_label = market_map.get(cfg['name'], '其他')
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                sector = str(row.get('產業別', 'Unknown')).strip()
                
                if code.isalnum() and len(code) >= 4:
                    symbol = f"{code}{cfg['suffix']}"
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                        VALUES (?, ?, ?, ?, ?)
                    """, (symbol, name, sector, market_label, datetime.now().strftime("%Y-%m-%d")))
                    stock_list.append((symbol, name))
        except Exception as e:
            log(f"⚠️ 獲取 {cfg['name']} 市場失敗: {e}")
            
    conn.commit()
    conn.close()
    return list(set(stock_list))

# ========== 4. 核心下載邏輯 (改為單執行緒 + 強制關閉 yf 多線程) ==========
def download_one_stable(symbol, mode):
    start_date = "2020-01-01" if mode == 'hot' else "1993-01-04"
    max_retries = 2
    
    for attempt in range(max_retries + 1):
        try:
            # 💡 關鍵修正 1：threads=False 徹底防止 yfinance 記憶體錯亂
            # 💡 關鍵修正 2：auto_adjust=True 確保開盤價與收盤價的一致性
            df = yf.download(symbol, start=start_date, progress=False, timeout=25, 
                             auto_adjust=True, threads=False)
            
            if df is None or df.empty:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None
            
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            df.reset_index(inplace=True)
            df.columns = [c.lower() for c in df.columns]
            
            df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            return df_final
        except Exception:
            if attempt < max_retries:
                time.sleep(3)
                continue
            return None

# ========== 5. 主流程 (單執行緒循環) ==========
def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_tw_stock_list()
    if not items:
        log("❌ 無法獲取股票清單，終止任務")
        return {"success": 0, "total": 0, "has_changed": False}

    log(f"🚀 開始單執行緒同步 TW | 目標: {len(items)} 檔 | 模式: {mode}")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 💡 關鍵修正 3：改用單執行緒 for 迴圈 + tqdm 進度條
    pbar = tqdm(items, desc=f"TW同步({mode})")
    for symbol, name in pbar:
        df_res = download_one_stable(symbol, mode)
        
        if df_res is not None:
            # 寫入資料庫
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
            
        # 🟢 為了避開 Yahoo 封鎖，每下載完一檔稍微停一下
        time.sleep(0.05)
    
    conn.commit()
    
    # 日期統計與資料庫優化
    max_date = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()[0]
    latest_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices WHERE date = ?", (max_date,)).fetchone()[0]
    total_info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
    
    log(f"🧹 優化資料庫 (VACUUM)...")
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"📅 最新交易日: {max_date} ({latest_count} 檔更新)")
    log(f"✅ 更新成功: {success_count} / {len(items)}")

    return {
        "success": success_count,
        "total": len(items),
        "latest_date": max_date
    }

if __name__ == "__main__":
    run_sync(mode='hot')
