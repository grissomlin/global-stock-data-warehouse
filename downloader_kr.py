# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境與參數設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# ✅ 效能參數：每批次處理 40 檔，預計 8 分鐘內完成
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
        if 'market' not in [col[1] for col in cursor.fetchall()]:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取名單 (對接偵察成功的 MDCSTAT03402) ==========

def get_kr_stock_list():
    log("📡 正在向 KRX 請求「詳細基本資料」(MDCSTAT03402) 以獲取產業別...")
    
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03402' # 💡 偵察成功的接口
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/'
    }
    
    try:
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        otp_code = r_otp.text
        
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949'
        
        df = pd.read_csv(io.StringIO(r_csv.text))
        
        # 💡 基於偵察報告的精準欄位映射
        # 종목코드 | 종목명 | 시장구분 | 업종名
        conn = sqlite3.connect(DB_PATH)
        items = []
        
        for _, row in df.iterrows():
            # 處理代碼格式
            code_raw = str(row['종목코드']).strip()
            code_clean = re.sub(r'[^0-9]', '', code_raw).zfill(6)
            
            mkt = str(row['市場區分']).upper()
            suffix = ".KS" if "KOSPI" in mkt else ".KQ"
            symbol = f"{code_clean}{suffix}"
            
            name = str(row['종목명']).strip()
            sector = str(row['업종명']).strip() # 💡 抓到了！
            
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, mkt, datetime.now().strftime("%Y-%m-%d")))
            items.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 韓國清單導入成功: {len(items)} 檔 (產業別已補齊)")
        return items

    except Exception as e:
        log(f"❌ 獲取清單失敗: {e}")
        return []

# ========== 4. 批量下載與同步主流程 ==========

def download_batch(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    try:
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, threads=False, progress=False, timeout=45)
        if data.empty: return 0
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0
        for symbol in symbols:
            try:
                df = data[symbol].copy() if len(symbols) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                
                for _, r in df.iterrows():
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_prices (date, symbol, open, high, low, close, volume)
                        VALUES (?,?,?,?,?,?,?)
                    """, (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], r['volume']))
                success += 1
            except: continue
        conn.commit(); conn.close()
        return success
    except: return 0

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    items = get_kr_stock_list()
    if not items: return {"success": 0, "has_changed": False}

    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始韓股同步 | 總目標: {len(items)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_batch, b, mode): b for b in batches}
        pbar = tqdm(total=len(items), desc="KR同步")
        for f in as_completed(futures):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()
            pbar.update(BATCH_SIZE)
        pbar.close()

    log("🧹 執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    return {"success": total_success, "total": len(items), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
