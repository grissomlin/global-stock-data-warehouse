# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

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

# ========== 2. 獲取名單 (修復 0 檔問題) ==========

def get_kr_stock_list():
    log("📡 正在從 KRX 官方獲取詳細公司資料 (MDCSTAT03402)...")
    
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    
    # 💡 修正參數：對應 MDCSTAT03402 的正確參數結構
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',             # 抓取所有市場
        'share': '1',               # 股票類
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT03402'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'
    }
    
    try:
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        otp_code = r_otp.text
        
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949'
        
        df = pd.read_csv(io.StringIO(r_csv.text))
        
        if df.empty:
            log("⚠️ 警告：KRX 回傳空檔案，嘗試更換 API 參數...")
            # 備援：如果 mktId ALL 失敗，通常是 mktId 要留空
            otp_params['mktId'] = '' 
            r_otp = requests.post(otp_url, data=otp_params, headers=headers)
            r_csv = requests.post(dn_url, data={'code': r_otp.text}, headers=headers)
            r_csv.encoding = 'cp949'
            df = pd.read_csv(io.StringIO(r_csv.text))

        # 欄位偵測 (韓文)
        # 종목코드 | 종목명 | 시장구분 | 업종명
        col_map = {}
        for col in df.columns:
            c = str(col).strip()
            if '종목코드' in c: col_map['code'] = col
            elif '종목명' in c: col_map['name'] = col
            elif '시장구분' in c: col_map['market'] = col
            elif '업종명' in c: col_map['sector'] = col

        conn = sqlite3.connect(DB_PATH)
        items = []
        samples = []

        for _, row in df.iterrows():
            code_raw = str(row.get(col_map.get('code'), '')).strip()
            if not code_raw: continue
            
            # 清理代碼 (部分會帶 A 符號)
            code_clean = re.sub(r'[^0-9]', '', code_raw).zfill(6)
            mkt = str(row.get(col_map.get('market'), ''))
            suffix = ".KS" if "KOSPI" in mkt.upper() else ".KQ"
            symbol = f"{code_clean}{suffix}"
            
            name = str(row.get(col_map.get('name'), 'Unknown')).strip()
            sector = str(row.get(col_map.get('sector'), 'Other/Unknown')).strip()

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, mkt, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))
            if len(samples) < 5:
                samples.append(f"   🔍 實測成功: {symbol} | {name[:8]} | 產業: {sector}")
            
        conn.commit()
        conn.close()
        
        log(f"✅ 韓國清單導入成功: {len(items)} 檔")
        for s in samples: print(s)
        return items

    except Exception as e:
        log(f"❌ 韓國清單抓取失敗: {e}")
        return []

# ========== 3. 批量下載與主流程 (同前) ==========

def download_batch(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    try:
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, threads=False, progress=False, timeout=45)
        if data.empty: return 0
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0
        current_symbols = [symbols] if isinstance(symbols, str) else symbols
        for symbol in current_symbols:
            try:
                df = data[symbol].copy() if len(current_symbols) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                for _, r in df.iterrows():
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)", 
                                 (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], r['volume']))
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
    log(f"🚀 開始韓股高速同步 | 目標: {len(items)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch, b, mode): b for b in batches}
        pbar = tqdm(total=len(items), desc="KR同步")
        for f in as_completed(future_to_batch):
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
