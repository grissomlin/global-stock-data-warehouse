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
    """回傳 dict: {stock_code (6碼): sector_name}"""
    log("📡 正在從 KRX MDCSTAT01801 抓取產業分類...")
    try:
        today_str = datetime.today().strftime("%Y%m%d")
        otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        
        otp_params = {
            'locale': 'ko_KR',
            'mktId': 'ALL',
            'trdDd': today_str,
            'money': '1',
            'csvxls_isNo': 'false',
            'name': 'fileDown',
            'url': 'dbms/MDC/STAT/standard/MDCSTAT01801'
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201030201'
        }

        time.sleep(1)
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        r_otp.raise_for_status()
        otp = r_otp.text.strip()

        r_csv = requests.post(dn_url, data={'code': otp}, headers=headers, timeout=20)
        r_csv.encoding = 'cp949'

        if "서비스가 원할하지 않습니다" in r_csv.text or len(r_csv.text.strip()) < 100:
            raise RuntimeError("KRX returned error or empty response")

        raw_text = r_csv.text.strip()
        lines = raw_text.split('\n')
        log(f"📄 原始 CSV 行數: {len(lines)}")

        # === 🔍 診斷輸出：顯示前 3 行（幫助你分析結構）===
        for i, line in enumerate(lines[:3]):
            clean_line = line.replace('\r', '').replace('"', '')
            log(f"   📌 Line {i}: {clean_line[:120]}{'...' if len(clean_line) > 120 else ''}")

        # 嘗試用 pandas 讀取
        df = pd.read_csv(io.StringIO(raw_text))
        log(f"📊 Pandas 解析後形狀: {df.shape}")
        log(f"   欄位名稱: {list(df.columns)}")

        sector_map = {}
        code_col = None
        sector_col = None

        # 嘗試智能匹配欄位名（支援新舊格式）
        for col in df.columns:
            c = str(col).strip()
            if re.search(r'단축코드|종목코드|ISU_SRT_CD|CODE', c, re.IGNORECASE):
                code_col = col
            elif re.search(r'업종명|SECT_TP_NM|IDX_IND_NM|산업|INDUSTRY', c, re.IGNORECASE):
                sector_col = col

        if not code_col or not sector_col:
            log("⚠️ 自動識別失敗 → 改用固定位置解析（第0欄=代碼, 第1欄=產業）")
            # === 💡 強制使用位置解析（最穩方案）===
            for i in range(len(df)):
                try:
                    code_raw = str(df.iloc[i, 0]).strip().replace('"', '').replace("'", "")
                    sector_raw = str(df.iloc[i, 1]).strip().replace('"', '').replace("'", "")
                    if code_raw.isdigit() and len(code_raw) == 6:
                        if sector_raw and sector_raw not in ['-', '', 'N/A', 'NaN', 'null']:
                            sector_map[code_raw] = sector_raw
                except Exception:
                    continue
        else:
            log(f"✅ 成功識別欄位: 代碼={code_col}, 產業={sector_col}")
            for _, row in df.iterrows():
                code_raw = str(row[code_col]).strip()
                if code_raw.isdigit() and len(code_raw) == 6:
                    sector = str(row[sector_col]).strip()
                    if sector and sector not in ['-', '']:
                        sector_map[code_raw] = sector

        log(f"✅ 最終載入 {len(sector_map)} 個產業對應")
        
        # === 🧪 顯示前 3 個成功映射（驗證正確性）===
        sample_items = list(sector_map.items())[:3]
        for code, sect in sample_items:
            log(f"   🔍 映射範例: {code} → {sect}")

        return sector_map

    except Exception as e:
        log(f"❌ 產業分類抓取失敗: {e}")
        return {}

# ========== 3. 主清單獲取（FDR + KRX Sector 合併）==========
def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader + KRX 產業表 獲取完整清單...")
    
    try:
        # Step 1: 用 FDR 拿基本資料
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 原始資料: {len(df_fdr)} 檔")

        # Step 2: 從 KRX 拿產業映射
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
            
            # 優先用 KRX 產業，其次用 FDR（若存在），否則 Unknown
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

# ========== 4. 批量下載股價（保持不變）==========
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
