# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 路徑與參數設定 ==========
MAX_WORKERS = 4 
DB_NAME = "cn_stock_warehouse.db"
# 針對 Colab 環境設定快取路徑，若在其他環境執行會自動切換至當前目錄
CACHE_DIR = "/content/drive/MyDrive/各國股票檔案/logs"
CACHE_FILE = os.path.join(CACHE_DIR, "cn_symbols_cache.json")

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        print(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

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
    """獲取 A 股清單 (四層防禦機制)"""
    ensure_pkg("akshare")
    import akshare as ak
    
    threshold = 4000
    res = []

    # --- Level 1: 標準接口 (stock_info_a_code_name) ---
    print("📡 [Level 1] 嘗試 Akshare 標準接口...")
    try:
        df = ak.stock_info_a_code_name()
        if not df.empty:
            df['code'] = df['code'].astype(str).str.zfill(6)
            valid_prefixes = ('000','001','002','300','600','601','603','605')
            df = df[df['code'].str.startswith(valid_prefixes)]
            res = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in df['code']]
            if len(res) >= threshold:
                save_cache(res)
                print(f"✅ Level 1 成功 ({len(res)} 檔)")
                return list(set(res))
    except Exception as e:
        print(f"⚠️ Level 1 異常: {e}")

    # --- Level 2: 即時行情接口 (EM 接口，通常較穩定) ---
    print("📡 [Level 2] 嘗試即時行情接口 (EM)...")
    try:
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        all_codes = []
        if not df_sh.empty: all_codes += df_sh['代码'].astype(str).str.zfill(6).tolist()
        if not df_sz.empty: all_codes += df_sz['代码'].astype(str).str.zfill(6).tolist()
        res = [f"{c}.SS" if c.startswith('6') else f"{c}.SZ" for c in all_codes]
        if len(res) >= threshold:
            save_cache(res)
            print(f"✅ Level 2 成功 ({len(res)} 檔)")
            return list(set(res))
    except Exception as e:
        print(f"⚠️ Level 2 異常: {e}")

    # --- Level 3: 讀取 Drive 快取 ---
    print(f"📡 [Level 3] 嘗試讀取快取檔...")
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                res = json.load(f)
            if len(res) >= threshold:
                print(f"♻️ Level 3 成功: 從快取恢復 {len(res)} 檔")
                return res
        except:
            pass

    # --- Level 4: 最終備援 (核心權值股) ---
    print("🚨 [Level 4] 所有連線失效且無快取，使用權值股保底...")
    return [
        "600519.SS", "601318.SS", "600036.SS", "601398.SS", "601857.SS", 
        "000858.SZ", "000333.SZ", "002415.SZ", "000001.SZ", "300750.SZ"
    ]

def save_cache(data):
    """將清單存入快取"""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(CACHE_FILE, 'w') as f:
            json.dump(data, f)
        print(f"💾 清單已備份至: {CACHE_FILE}")
    except Exception as e:
        print(f"📦 快取寫入失敗: {e}")

def fetch_single_stock(symbol, period):
    """單檔下載邏輯"""
    try:
        time.sleep(random.uniform(0.6, 1.5)) # 略微拉長等待時間保護連線
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
    init_db()
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
                print(f"📊 下載進度: {count}/{len(items)}...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 任務完成，獲取 {len(final_df)} 筆記錄")
        return final_df
    return pd.DataFrame()

if __name__ == "__main__":
    df = fetch_cn_market_data(is_first_time=False)
