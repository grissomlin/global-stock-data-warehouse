# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        print(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

# ========== 核心參數設定 ==========
MAX_WORKERS = 4  # A 股維持 4，避免 Yahoo 封鎖
DB_NAME = "cn_stock_warehouse.db"

def init_db():
    """自動初始化資料庫結構"""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            date TEXT,
            symbol TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            PRIMARY KEY (date, symbol)
        )
    ''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stocks (symbol)')
    conn.commit()
    conn.close()
    print(f"📁 資料庫 {DB_NAME} 已就緒")

def get_full_stock_list():
    """獲取 A 股清單 (含 Akshare 失敗後的強力備援)"""
    ensure_pkg("akshare")
    import akshare as ak
    
    print("📡 正在獲取 A 股清單...")
    try:
        # 嘗試第一個接口 (東方財富)
        df = ak.stock_info_a_code_name()
        df['code'] = df['code'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','300','600','601','603','605')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        res = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df['code']]
        if len(res) > 4000:
            print(f"✅ 透過 akshare 成功獲取 {len(res)} 檔代號")
            return list(set(res))
    except Exception as e:
        print(f"⚠️ Akshare 接口連線異常: {e}")

    # --- 強力備援：核心權值股清單 (避免 GitHub Action 失敗只抓一檔) ---
    print("💡 啟動備援機制：使用 A 股核心權值股清單 (100 檔)")
    backup_list = [
        "600519.SS", "601318.SS", "600036.SS", "601398.SS", "601857.SS", "601288.SS", "601939.SS", "601988.SS", "600028.SS", "600900.SS",
        "601088.SS", "601628.SS", "601166.SS", "600030.SS", "601328.SS", "600309.SS", "601138.SS", "601319.SS", "600048.SS", "600019.SS",
        "000858.SZ", "000333.SZ", "002415.SZ", "000001.SZ", "300750.SZ", "000651.SZ", "002594.SZ", "300059.SZ", "000725.SZ", "002475.SZ",
        "000100.SZ", "000002.SZ", "000768.SZ", "002304.SZ", "002352.SZ", "002714.SZ", "300015.SZ", "300760.SZ", "000538.SZ", "000895.SZ"
    ] # 此處僅列部分示範，可自行增補
    return backup_list

def fetch_single_stock(symbol, period):
    """單檔下載邏輯"""
    try:
        time.sleep(random.uniform(0.5, 1.2)) # 稍作延遲保護
        tk = yf.Ticker(symbol)
        hist = tk.history(period=period, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    except:
        return None
    return None

def fetch_cn_market_data(is_first_time=False):
    """主進入點"""
    init_db() # 確保資料庫存在
    period = "max" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 任務啟動: {'全量(max)' if is_first_time else '增量(7d)'}, 目標: {len(items)} 檔")
    
    all_dfs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_stock, tkr, period): tkr for tkr in items}
        
        count = 0
        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                all_dfs.append(res)
            
            count += 1
            if count % 100 == 0:
                print(f"📊 進度: {count}/{len(items)} 檔處理中...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 處理完成，共獲取 {len(final_df)} 筆交易記錄")
        return final_df
    return pd.DataFrame()

# 測試用執行區塊
if __name__ == "__main__":
    # 測試抓取 (False 代表增量模式)
    df = fetch_cn_market_data(is_first_time=False)
    if not df.empty:
        print(df.head())
