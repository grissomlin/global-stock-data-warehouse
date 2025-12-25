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
        columns = [col[1] for col in cursor.fetchall()]
        if 'market' not in columns:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 3. 抓取韓國上市清單（關鍵修正版）==========
def get_kr_stock_list():
    log("📡 正在從 KRX 官方獲取上市股票清單 (MDCSTAT01901)...")
    
    # Step 1: Generate OTP
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    today_str = datetime.today().strftime("%Y%m%d")
    
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',                 # ALL = KOSPI + KOSDAQ
        'trdDd': today_str,             # 觸發最新資料
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'  # ✅ 正確模組！
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101'
    }
    
    try:
        # 避免被限流（尤其 GitHub Actions）
        time.sleep(2)
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        r_otp.raise_for_status()
        otp_code = r_otp.text.strip()
        
        if len(otp_code) < 10:
            raise ValueError(f"Invalid OTP: '{otp_code}'")
        
        log(f"🔑 OTP generated successfully (length: {len(otp_code)})")

        # Step 2: Download CSV with OTP
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949'  # 韓國標準編碼
        
        # 檢查是否回傳錯誤頁面
        if "서비스가 원할하지 않습니다" in r_csv.text or r_csv.status_code != 200:
            raise RuntimeError("KRX returned error page (可能過載或維護中)")

        df = pd.read_csv(io.StringIO(r_csv.text))
        log(f"📥 原始 CSV 行數: {len(df)}")

        if df.empty:
            raise RuntimeError("Downloaded CSV is empty")

        # Step 3: 智能欄位映射（容錯韓文/英文混用）
        col_map = {}
        for col in df.columns:
            c = str(col).strip()
            if re.search(r'종목코드|ISU_SRT_CD|ISU_CD', c): col_map['code'] = col
            elif re.search(r'종목명|ISU_NM', c): col_map['name'] = col
            elif re.search(r'시장구분|MKT_NM', c): col_map['market'] = col
            elif re.search(r'업종명|SECT_TP_NM', c): col_map['sector'] = col

        if not col_map.get('code') or not col_map.get('name'):
            log("⚠️ 無法識別必要欄位，顯示前3列診斷：")
            print(df.head(3).to_string())
            return []

        # Step 4: 寫入資料庫
        conn = sqlite3.connect(DB_PATH)
        items = []
        samples = []

        for _, row in df.iterrows():
            code_raw = str(row[col_map['code']]).strip()
            if not code_raw.replace('A', '').isdigit():  # 移除可能的 'A' 前綴
                continue

            code_clean = re.sub(r'\D', '', code_raw).zfill(6)
            if len(code_clean) != 6:
                continue

            market_raw = str(row.get(col_map.get('market'), 'Unknown')).strip()
            suffix = ".KS" if "KOSPI" in market_raw.upper() else ".KQ"
            symbol = f"{code_clean}{suffix}"
            
            name = str(row[col_map['name']]).strip()
            sector = str(row.get(col_map.get('sector'), 'Other')).strip()

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market_raw, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))
            if len(samples) < 3:
                samples.append(f"   🔍 {symbol} | {name[:12]} | {sector}")

        conn.commit()
        conn.close()
        
        log(f"✅ 韓國清單導入成功: {len(items)} 檔")
        for s in samples: print(s)
        return items

    except Exception as e:
        log(f"❌ 韓國清單抓取失敗: {e}")
        return []

# ========== 4. 批量下載股價 ==========
def download_batch(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    try:
        data = yf.download(
            tickers=symbols,
            start=start_date,
            group_by='ticker',
            auto_adjust=True,
            threads=False,
            progress=False,
            timeout=45
        )
        if data.empty:
            return 0

        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0
        symbol_list = [symbols] if isinstance(symbols, str) else symbols

        for symbol in symbol_list:
            try:
                df_symbol = data[symbol] if len(symbol_list) > 1 else data
                if df_symbol.empty:
                    continue
                df_symbol = df_symbol.reset_index()
                df_symbol.columns = [c.lower() for c in df_symbol.columns]
                date_col = 'date' if 'date' in df_symbol.columns else df_symbol.columns[0]
                df_symbol['date_str'] = pd.to_datetime(df_symbol[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                
                rows = []
                for _, r in df_symbol.iterrows():
                    if pd.notna(r['open']):
                        rows.append((r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], int(r['volume']) if pd.notna(r['volume']) else 0))
                
                conn.executemany(
                    "INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                    rows
                )
                success += 1
            except Exception:
                continue

        conn.commit()
        conn.close()
        return success
    except Exception:
        return 0

# ========== 5. 主流程 ==========
def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        log("🛑 韓股清單為空，跳過同步")
        return {"success": 0, "total": 0, "has_changed": False}

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
    final_count = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！費時: {duration:.1f} 分鐘")
    log(f"✅ 資料庫總數: {final_count} | 本次新增: {total_success}")
    
    return {
        "success": final_count,
        "total": len(items),
        "has_changed": total_success > 0
    }

if __name__ == "__main__":
    run_sync(mode='hot')
