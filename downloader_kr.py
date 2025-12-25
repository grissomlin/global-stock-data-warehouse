# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
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

# ========== 2. 從 KRX MDCSTAT01801 抓取「產業分類」==========
def fetch_sector_mapping():
    """回傳 dict: {stock_code (6碼): sector_name}，自動使用最近交易日"""
    log("📡 正在從 KRX MDCSTAT01801 抓取產業分類（自動尋找最近交易日）...")
    
    # 從今天往前試最多 7 天（跳過週末）
    for days_back in range(0, 7):
        query_date = datetime.today() - pd.Timedelta(days=days_back)
        date_str = query_date.strftime("%Y%m%d")
        weekday = query_date.weekday()  # Mon=0, ..., Sun=6
        
        if weekday >= 5:  # 跳過週六日
            continue

        try:
            otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
            dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
            
            otp_params = {
                'locale': 'ko_KR',
                'mktId': 'ALL',
                'trdDd': date_str,
                'money': '1',
                'csvxls_isNo': 'false',
                'name': 'fileDown',
                'url': 'dbms/MDC/STAT/standard/MDCSTAT01801'
            }
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201030201'
            }

            time.sleep(1 + random.uniform(0, 0.5))
            r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
            if r_otp.status_code != 200 or len(r_otp.text.strip()) < 5:
                log(f"   ⏭️  {date_str} OTP 失敗")
                continue

            otp = r_otp.text.strip()
            r_csv = requests.post(dn_url, data={'code': otp}, headers=headers, timeout=20)
            r_csv.encoding = 'cp949'

            raw_text = r_csv.text.strip()
            if "서비스가 원할하지 않습니다" in raw_text or len(raw_text) < 100:
                log(f"   ⏭️  {date_str} 無效或空資料，嘗試前一日...")
                continue

            # 成功取得有效資料！
            log(f"✅ 使用交易日: {date_str}")
            df = pd.read_csv(io.StringIO(raw_text))

            sector_map = {}
            # 💡 強制使用位置：第0欄=代碼，第1欄=產業（最穩方案）
            for i in range(len(df)):
                try:
                    code_raw = str(df.iloc[i, 0]).strip().replace('"', '').replace("'", "")
                    sector_raw = str(df.iloc[i, 1]).strip().replace('"', '').replace("'", "")
                    if code_raw.isdigit() and len(code_raw) == 6:
                        if sector_raw and sector_raw not in ['-', '', 'N/A', 'NaN', 'null']:
                            sector_map[code_raw] = sector_raw
                except Exception:
                    continue

            log(f"✅ 成功載入 {len(sector_map)} 個產業對應")
            sample_items = list(sector_map.items())[:3]
            for code, sect in sample_items:
                log(f"   🔍 映射範例: {code} → {sect}")
            return sector_map

        except Exception as e:
            log(f"   ⏭️  {date_str} 請求異常: {e}")
            continue

    log("❌ 所有日期嘗試失敗，無法取得產業分類")
    return {}

# ========== 3. 主清單獲取（FDR + KRX Sector 合併）==========
def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader + KRX 產業表 獲取完整清單...")
    
    try:
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 原始資料: {len(df_fdr)} 檔")

        sector_map = fetch_sector_mapping()

        conn = sqlite3.connect(DB_PATH)
        items = []
        samples = []

        for _, row in df_fdr.iterrows():
            code_clean = str(row['Code']).strip()
            if not code_clean.isdigit() or len(code_clean) != 6:
                continue

            mkt = str(row.get('Market', 'Unknown')).strip()
            suffix = ".KS" if mkt == "KOSPI" else ".KQ"
            symbol = f"{code_clean}{suffix}"
            name = str(row['Name']).strip()
            
            sector = sector_map.get(code_clean)
            if not sector and pd.notna(row.get('Sector')):
                sector = str(row['Sector']).strip()
            if not sector:
                sector = "Other/Unknown"

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, mkt, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))
            if len(samples) < 5 and sector != "Other/Unknown":
                samples.append(f"   ✅ {symbol} | {name[:12]} | {sector}")

        conn.commit()
        conn.close()

        log(f"✅ 韓股清單整合成功: {len(items)} 檔")
        for s in samples: print(s)
        return items

    except Exception as e:
        log(f"❌ 清單整合失敗: {e}")
        return []

# ========== 4. 批量下載股價 ==========
def download_batch(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    try:
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, threads=False, progress=False, timeout=45)
        if data.empty: return 0
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0
        target_list = symbols if isinstance(symbols, list) else [symbols]
        for symbol in target_list:
            try:
                df = data[symbol].copy() if len(target_list) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                for _, r in df.iterrows():
                    vol = int(r['volume']) if pd.notna(r['volume']) else 0
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)", 
                                 (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], vol))
                success += 1
            except: continue
        conn.commit(); conn.close()
        return success
    except: return 0

# ========== 5. 初始化 DB & 主流程 ==========
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                        date TEXT, symbol TEXT, open REAL, high REAL, 
                        low REAL, close REAL, volume INTEGER,
                        PRIMARY KEY (date, symbol))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                        symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')
    conn.close()

def run_sync(mode='hot'):
    start_time = time.time()
    init_db()
    
    items = get_kr_stock_list()
    if not items:
        log("🛑 無有效股票清單，跳過同步")
        return {"success": 0, "total": 0, "has_changed": False}

    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始韓股同步 | 目標: {len(items)} 檔 | 批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_batch, b, mode): b for b in batches}
        for f in tqdm(as_completed(futures), total=len(batches), desc="KR同步"):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()

    log("🧹 資料庫優化...")
    conn = sqlite3.connect(DB_PATH); conn.execute("VACUUM"); conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！有效標的: {total_success} | 費時: {duration:.1f} 分鐘")
    return {"success": total_success, "total": len(items), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
