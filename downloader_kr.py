# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, re
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

BATCH_SIZE = 40        
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10 
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 從 KRX 公開頁面抓取產業分類 ==========
def fetch_krx_industry_from_html():
    """
    從 KRX 公開頁面抓取公司代碼與產業分類（업종）
    返回 dict: { '005930': '전기전자', ... }
    """
    log("📡 正在從 KRX 公開頁面 (corpList.do) 抓取產業分類...")
    
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do"
    params = {
        'method': 'download',
        'searchType': '13'  # 包含所有市場
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        time.sleep(1)
        r = requests.get(url, params=params, headers=headers, timeout=15)
        r.encoding = 'cp949'  # 韓國編碼
        
        raw_text = r.text.strip()
        if not raw_text or "회사명" not in raw_text or "업종" not in raw_text:
            log("❌ KRX 回應內容異常，無法解析產業資料")
            return {}

        lines = raw_text.split('\n')
        sector_map = {}

        for line in lines[1:]:  # 跳過標題行
            parts = line.split('\t')
            if len(parts) < 4:
                continue

            try:
                # 欄位順序：회사명, 시장구분, 종목코드, 업종, ...
                stock_code_raw = parts[2].strip()
                industry = parts[3].strip()

                # 只保留純數字（移除 A/B 前綴）
                stock_code = re.sub(r'[^0-9]', '', stock_code_raw)
                if len(stock_code) != 6:
                    continue

                if industry and industry not in ['-', '', 'N/A', 'NaN']:
                    sector_map[stock_code] = industry

            except Exception:
                continue

        log(f"✅ 成功載入 {len(sector_map)} 個產業對應（來自 KRX corpList.do）")
        sample_items = list(sector_map.items())[:5]
        for code, ind in sample_items:
            log(f"   🔍 {code} → {ind}")

        return sector_map

    except Exception as e:
        log(f"❌ 抓取 KRX 公開產業表失敗: {e}")
        import traceback
        traceback.print_exc()
        return {}

# ========== 3. 主清單獲取（FDR + KRX 產業）==========
def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader + KRX 公開產業表 獲取完整清單...")
    
    try:
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 原始資料: {len(df_fdr)} 檔")

        # 從 KRX 抓取產業分類
        krx_sector_map = fetch_krx_industry_from_html()

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
            
            # 優先使用 KRX 來源的產業，否則標記為 Unknown
            sector = krx_sector_map.get(code_clean, "Other/Unknown")

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
        if samples:
            for s in samples:
                print(s)
        else:
            log("⚠️ 注意：未找到有效產業分類（可能 KRX 資料未覆蓋全部股票）")

        return items

    except Exception as e:
        log(f"❌ 清單整合失敗: {e}")
        import traceback
        traceback.print_exc()
        return []

# ========== 4. 批量下載股價 ==========
def download_batch(batch_items, mode):
    symbols = [it[0] for it in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    try:
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, threads=False, progress=False, timeout=45)
        if data.empty:
            return 0
        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0
        target_list = symbols if isinstance(symbols, list) else [symbols]
        for symbol in target_list:
            try:
                df = data[symbol].copy() if len(target_list) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty:
                    continue
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                for _, r in df.iterrows():
                    vol = int(r['volume']) if pd.notna(r['volume']) else 0
                    conn.execute("INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)", 
                                 (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], vol))
                success += 1
            except:
                continue
        conn.commit()
        conn.close()
        return success
    except:
        return 0

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
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！有效標的: {total_success} | 費時: {duration:.1f} 分鐘")
    return {"success": total_success, "total": len(items), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
