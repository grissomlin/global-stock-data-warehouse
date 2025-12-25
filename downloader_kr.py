# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, subprocess, io, requests, re
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

# ✅ 下載設定：韓國伺服器對 API 請求較敏感
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 5 
BASE_DELAY = 1.5 if IS_GITHUB_ACTIONS else 0.3

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 資料庫初始化 (含自動升級) ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
        
        # 自動升級邏輯
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'market' not in columns:
            log("🔧 正在升級 KR 資料庫：新增 'market' 欄位...")
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 獲取韓國官方清單 (OTP + POST 邏輯) ==========

def get_kr_stock_list():
    """直接從韓國交易所 (KRX) 抓取官方 CSV 清單"""
    log("📡 正在從 KRX 官方獲取最新韓國股票清單 (含產業別)...")
    
    # 步驟 A: 獲取 OTP
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',         # KOSPI, KOSDAQ, KONEX 全抓
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'
    }
    
    try:
        # 1. 取得 OTP 授權碼
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        otp_code = r_otp.text
        
        # 2. 帶上 OTP 下載 CSV
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949' # 💡 韓國交易所固定使用 cp949 編碼
        
        df = pd.read_csv(io.StringIO(r_csv.text))
        
        # 欄位映射：단축코드(代碼), 한글 종목약명(名稱), 시장구분(市場), 업종명(產業)
        mapping = {
            '단축코드': 'code',
            '한글 종목약명': 'name',
            '시장구분': 'market',
            '업종명': 'sector'
        }
        df = df.rename(columns=mapping)

        conn = sqlite3.connect(DB_PATH)
        stock_list = []
        
        for _, row in df.iterrows():
            code = str(row['code']).strip()
            # 💡 判斷市場別，決定 Yahoo 後綴 (.KS=KOSPI, .KQ=KOSDAQ)
            mkt = str(row['market']).upper()
            suffix = ".KS" if "KOSPI" in mkt else ".KQ"
            symbol = f"{code.zfill(6)}{suffix}"
            
            name = str(row['name']).strip()
            sector = str(row.get('sector', 'Unknown')).strip()
            
            # 存入 stock_info
            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, mkt, datetime.now().strftime("%Y-%m-%d")))
            
            stock_list.append((symbol, name))
            
        conn.commit()
        conn.close()
        log(f"✅ 韓國官方清單導入完成: {len(stock_list)} 檔")
        return stock_list

    except Exception as e:
        log(f"❌ 韓國清單抓取失敗: {e}。改用 FDR 作為備援。")
        # 這裡可以保留 FinanceDataReader 作為備援，或者直接回傳空值
        return []

# ========== 4. 下載邏輯 (重試與 MultiIndex 處理) ==========

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            # 💡 針對韓國市場增加隨機等待，避免 429
            time.sleep(random.uniform(BASE_DELAY, BASE_DELAY * 2))
            
            tk = yf.Ticker(symbol)
            hist = tk.history(start=start_date, timeout=25, auto_adjust=True)
            
            if hist is None or hist.empty:
                if attempt < max_retries: continue
                return {"symbol": symbol, "status": "empty"}
                
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # 壓平 MultiIndex
            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final['symbol'] = symbol
            
            conn = sqlite3.connect(DB_PATH, timeout=60)
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False, 
                            method=lambda t, c, k, d: c.executemany(
                                f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
            conn.close()
            return {"symbol": symbol, "status": "success"}
        except:
            if attempt < max_retries:
                time.sleep(5)
                continue
            return {"symbol": symbol, "status": "error"}

# ========== 5. 主流程 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始執行韓股同步 | 目標: {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fail_list = []
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_one, (it[0], it[1], mode)): it[0] for it in items}
        for f in tqdm(as_completed(futures), total=len(items), desc="KR同步"):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s == "error": fail_list.append(res.get("symbol"))

    # 💡 查詢實際庫存數
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    final_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 資料庫總數: {final_count} | 本次新增: {stats['success']}")
    
    return {
        "success": final_count,
        "total": len(items),
        "fail_list": fail_list,
        "has_changed": stats['success'] > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
