# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境判斷與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# 💡 參考 Colab 的高效設定
BATCH_SIZE = 100  # 批次下載數量
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 5

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 代碼正規化 (Colab V5.0 邏輯) ==========

def normalize_code4_yf(s: str) -> str:
    """Yahoo 下載使用 4 位數 (e.g. 0001.HK)"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-4:].zfill(4) if digits and digits.isdigit() else ""

# ========== 3. 資料庫與清單獲取 ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        # 自動升級
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

def get_hk_stock_list():
    """結合 Colab 魯棒性的清單抓取"""
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    
    log(f"📡 正在從港交所獲取清單...")
    try:
        r = requests.get(url, headers=headers, timeout=20, verify=False)
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 智慧找表頭
        hdr_idx = None
        for i in range(min(20, len(df_raw))):
            row_str = " ".join([str(x) for x in df_raw.iloc[i].values])
            if "Stock Code" in row_str:
                hdr_idx = i
                break
        
        if hdr_idx is None: raise ValueError("找不到 Excel 表頭")

        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].values
        
        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        # 排除衍生品關鍵字
        bad_kw = r"CBBC|WARRANT|RIGHTS|ETF|ETN|REIT|BOND|TRUST|FUND|牛熊|權證|輪證"

        for _, row in df.iterrows():
            raw_code = str(row['Stock Code']).strip()
            name = str(row.get('English Stock Short Name', 'Unknown')).strip()
            
            if raw_code.isdigit() and int(raw_code) < 10000 and not re.search(bad_kw, name, re.I):
                symbol = f"{normalize_code4_yf(raw_code)}.HK"
                
                conn.execute("""
                    INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, name, "Unknown", "HKEX", datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((symbol, name))
                
        conn.commit()
        conn.close()
        log(f"✅ 成功同步清單: {len(stock_list)} 檔")
        return stock_list
    except Exception as e:
        log(f"❌ 獲取失敗: {e}，改用保底名單")
        return [("0700.HK", "TENCENT"), ("09988.HK", "BABA-SW"), ("00005.HK", "HSBC")]

# ========== 4. 批次下載邏輯 (Colab 核心優勢) ==========

def download_batch_and_save(symbols_chunk, mode):
    """
    一次下載一批 symbols 並存入資料庫
    """
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    success_count = 0
    
    try:
        # 💡 使用批次下載
        data = yf.download(symbols_chunk, start=start_date, group_by='ticker', auto_adjust=True, progress=False, timeout=30)
        
        conn = sqlite3.connect(DB_PATH, timeout=60)
        
        for symbol in symbols_chunk:
            try:
                # 處理單檔與多檔回傳格式差異
                df = data[symbol] if len(symbols_chunk) > 1 else data
                
                if df is None or df.empty: continue
                
                df = df.reset_index()
                df.columns = [c.lower() for c in df.columns]
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                
                df_final = df[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
                df_final['symbol'] = symbol
                
                # 寫入 SQLite
                df_final.to_sql('stock_prices', conn, if_exists='append', index=False,
                                method=lambda t, c, k, d: c.executemany(
                                    f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
                success_count += 1
            except:
                continue
                
        conn.close()
        return success_count
    except Exception as e:
        log(f"⚠️ 批次下載失敗: {e}")
        return 0

# ========== 5. 主流程 (具備分批處理能力) ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_hk_stock_list()
    if not items: return {"success": 0, "has_changed": False}

    symbols = [it[0] for it in items]
    log(f"🚀 開始批次同步港股 | 總數: {len(symbols)} | 批次大小: {BATCH_SIZE}")

    total_success = 0
    # 將 symbols 分成 chunk
    chunks = [symbols[i:i + BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
    
    with tqdm(total=len(symbols), desc="HK同步中") as pbar:
        for chunk in chunks:
            # 隨機延遲避開 429
            time.sleep(random.uniform(2, 5) if IS_GITHUB_ACTIONS else 0.5)
            
            # 執行批次下載
            count = download_batch_and_save(chunk, mode)
            total_success += count
            pbar.update(len(chunk))

    log("🧹 執行 VACUUM...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 完成！成功更新: {total_success} 檔 | 費時: {duration:.1f} 分鐘")
    
    return {
        "success": total_success,
        "error": len(symbols) - total_success,
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
