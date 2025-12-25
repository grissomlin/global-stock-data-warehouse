# -*- coding: utf-8 -*-
"""
韓國股票資料同步器（支援 GitHub Actions）
- 從 FinanceDataReader 獲取完整股票清單
- 從 KRX 官方 Excel 下載產業分類（상장법인목록.xls）
- 使用 yfinance 批量下載歷史股價
- 寫入 SQLite 資料庫
"""

import os
import io
import time
import random
import sqlite3
import re
import pandas as pd
import yfinance as yf
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
    """強制即時輸出（適合 GitHub Actions 監控）"""
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)


# ========== 2. 從 KRX Excel 抓取產業分類 ==========
def fetch_krx_industry_from_excel():
    """
    從 KRX 官方靜態連結下載 상장법인목록.xls 並解析 업종（產業）
    返回 dict: { '005930': '전기전자', ... }
    """
    log("📡 正在從 KRX 下載 상장법인목록.xls (Excel 格式)...")
    
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        time.sleep(1.5)  # 避免被限流
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()

        # 🔥 關鍵：用 BytesIO + read_excel 解析二進位 Excel
        df = pd.read_excel(io.BytesIO(r.content), dtype=str)
        log(f"📥 成功載入 Excel 表格，共 {len(df)} 筆公司")

        # 自動識別欄位（避免硬編索引）
        code_col = None
        sector_col = None
        for col in df.columns:
            col_str = str(col).strip()
            if '종목코드' in col_str:
                code_col = col
            elif '업종' in col_str:
                sector_col = col

        if not code_col or not sector_col:
            log("❌ 無法識別 '종목코드' 或 '업종' 欄位，跳過產業解析")
            return {}

        sector_map = {}
        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            sector = str(row[sector_col]).strip()

            # 清理代碼：只保留 6 位數字
            clean_code = re.sub(r'\D', '', raw_code)
            if len(clean_code) == 6 and sector and sector not in ('', '-', 'N/A', 'nan'):
                sector_map[clean_code] = sector

        log(f"✅ 成功載入 {len(sector_map)} 個產業對應（來自 KRX Excel）")
        sample_items = list(sector_map.items())[:3]
        for code, ind in sample_items:
            log(f"   🔍 {code} → {ind}")

        return sector_map

    except Exception as e:
        log(f"❌ 下載或解析 KRX Excel 失敗: {e}")
        import traceback
        traceback.print_exc()
        return {}


# ========== 3. 主清單獲取（FDR + KRX Excel 產業）==========
def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader + KRX Excel 獲取完整清單...")
    
    try:
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 原始資料: {len(df_fdr)} 檔")

        # 嘗試從 KRX Excel 取得產業
        krx_sector_map = fetch_krx_industry_from_excel()

        conn = sqlite3.connect(DB_PATH)
        items = []
        valid_sector_count = 0

        for _, row in df_fdr.iterrows():
            code_clean = str(row['Code']).strip()
            if not code_clean.isdigit() or len(code_clean) != 6:
                continue

            market = str(row.get('Market', 'Unknown')).strip()
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code_clean}{suffix}"
            name = str(row['Name']).strip()

            # 優先使用 KRX Excel 的產業，其次嘗試 FDR 的 Sector，否則標記 Unknown
            sector = krx_sector_map.get(code_clean)
            if not sector:
                sector = str(row.get('Sector', '')).strip() or "Other/Unknown"
            if sector in ('', 'NaN', 'nan'):
                sector = "Other/Unknown"

            if sector != "Other/Unknown":
                valid_sector_count += 1

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))

        conn.commit()
        conn.close()

        log(f"✅ 韓股清單整合成功: {len(items)} 檔（含有效產業: {valid_sector_count}）")
        if valid_sector_count == 0:
            log("⚠️ 注意：所有股票產業均為 'Other/Unknown'，可能 KRX/FDR 資料異常")

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
                    conn.execute(
                        "INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                        (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], vol)
                    )
                success += 1
            except Exception:
                continue

        conn.commit()
        conn.close()
        return success
    except Exception:
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
