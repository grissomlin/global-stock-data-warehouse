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
import FinanceDataReader as fdr  # ✅ 修正點：補上遺漏的 import
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
    從 KIND 系統下載最新的產業對照表
    """
    log("📡 正在從 KIND 系統下載產業清單 (Excel 格式)...")
    
    # 使用 KIND 的下載接口
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        time.sleep(1.5)  
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()

        # KIND 回傳的是一個偽裝成 XLS 的 HTML 表格，Pandas read_html 處理它最穩定
        dfs = pd.read_html(io.BytesIO(r.content))
        if not dfs:
            log("❌ KIND 回傳內容中找不到表格")
            return {}
        
        df = dfs[0]
        log(f"📥 成功獲取 KIND 清單，共 {len(df)} 筆資料")

        # 欄位映射
        sector_map = {}
        # KIND 下載的欄位通常固定： 회사명, 종목코드, 업종, 주요제품...
        # 我們主要需要 '종목코드' (代碼) 和 '업종' (產業)
        
        # 尋找代碼和產業的正確欄位名稱
        code_col = next((c for c in df.columns if '종목코드' in str(c)), None)
        sector_col = next((c for c in df.columns if '업종' in str(c)), None)

        if code_col is None or sector_col is None:
            log(f"❌ 無法辨識欄位。現有欄位: {df.columns.tolist()}")
            return {}

        for _, row in df.iterrows():
            code = str(row[code_col]).strip().zfill(6)
            sector = str(row[sector_col]).strip()
            if code and sector:
                sector_map[code] = sector

        log(f"✅ 成功載入 {len(sector_map)} 個產業對應")
        return sector_map

    except Exception as e:
        log(f"❌ KIND Excel 解析失敗: {e}")
        return {}


# ========== 3. 主清單獲取（FDR + KIND 產業）==========
def get_kr_stock_list():
    log("📡 正在整合 FinanceDataReader 清單與 KIND 產業資料...")
    
    try:
        # 獲取 FDR 清單
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 獲取到 {len(df_fdr)} 檔標的")

        # 獲取產業映射
        kind_sector_map = fetch_krx_industry_from_excel()

        conn = sqlite3.connect(DB_PATH)
        items = []
        valid_sector_count = 0

        for _, row in df_fdr.iterrows():
            code_clean = str(row['Code']).strip().zfill(6)
            
            # 判斷市場後綴
            market = str(row.get('Market', 'Unknown')).strip()
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code_clean}{suffix}"
            name = str(row['Name']).strip()

            # 優先權：KIND 產業別 > FDR Sector 欄位 > Unknown
            sector = kind_sector_map.get(code_clean)
            if not sector:
                sector = str(row.get('Sector', '')).strip()
            
            if not sector or sector.lower() in ('nan', 'none', ''):
                sector = "Other/Unknown"
            else:
                valid_sector_count += 1

            conn.execute("""
                INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))
            
            items.append((symbol, name))

        conn.commit()
        conn.close()

        log(f"✅ 韓股清單整合成功: {len(items)} 檔（含有效產業: {valid_sector_count}）")
        return items

    except Exception as e:
        log(f"❌ 清單整合失敗: {e}")
        import traceback
        traceback.print_exc()
        return []


# ========== 4. 批量下載股價 (維持原有效率邏輯) ==========
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


# ========== 5. 初始化 & 主流程 ==========
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
