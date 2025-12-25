# -*- coding: utf-8 -*-
import os
import io
import time
import random
import sqlite3
import re
import pandas as pd
import yfinance as yf
import requests
import FinanceDataReader as fdr
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 1. 環境設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# 效能參數
BATCH_SIZE = 40
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg: str):
    """即時印出 Log，方便 GitHub Actions 監控"""
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. KIND 產業資料抓取 (權威來源) ==========

def fetch_kind_industry_map():
    """
    從 KIND (Korea Investor's Network for Disclosure) 下載上市公司名單
    此來源包含最準確的 '業種 (Industry)' 欄位
    """
    url = "http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    log("📡 正在從 KIND 系統獲取權威產業對照表...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        
        # KIND 的 XLS 實際上是 HTML 表格，用 read_html 處理最穩定
        dfs = pd.read_html(io.BytesIO(r.content))
        if not dfs:
            log("❌ KIND 回傳內容中找不到表格")
            return {}
        
        df = dfs[0]
        industry_map = {}
        
        # 遍歷資料建立對照表 (Key: 股票代碼, Value: 產業名稱)
        # 欄位名稱通常為: '종목코드' (代碼), '업종' (產業)
        for _, row in df.iterrows():
            # 💡 關鍵修正：保留原始代碼格式（支援字母），並補足 6 位
            code = str(row['종목코드']).strip().zfill(6)
            sector = str(row['업종']).strip()
            industry_map[code] = sector

        log(f"✅ 成功載入 {len(industry_map)} 筆產業對照數據")
        
        # 隨機抽取 3 筆顯示於 Log 確認
        sample_keys = random.sample(list(industry_map.keys()), 3)
        for k in sample_keys:
            log(f"   🔍 抽樣確認: {k} -> {industry_map[k]}")
            
        return industry_map

    except Exception as e:
        log(f"❌ KIND 產業資料抓取失敗: {e}")
        return {}

# ========== 3. 整合清單與資料庫寫入 ==========

def get_kr_stock_list():
    log("📡 正在透過 FinanceDataReader 獲取完整股票清單...")
    
    try:
        # 獲取 FDR 的 KRX 總表
        df_fdr = fdr.StockListing('KRX')
        log(f"📊 FDR 原始標的總數: {len(df_fdr)}")

        # 獲取 KIND 的產業補丁
        kind_map = fetch_kind_industry_map()

        conn = sqlite3.connect(DB_PATH)
        items = []
        valid_sector_count = 0

        # 確保資料表存在
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, market TEXT, updated_at TEXT)''')

        for _, row in df_fdr.iterrows():
            code = str(row['Code']).strip().zfill(6)
            market = str(row.get('Market', 'Unknown'))
            
            # 判斷市場後綴 (.KS 為 KOSPI, .KQ 為 KOSDAQ/KONEX)
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code}{suffix}"
            name = str(row['Name']).strip()

            # 💡 產業別邏輯：優先使用 KIND 資料，沒有則用 FDR 資料，最後才給 Unknown
            sector = kind_map.get(code)
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

        log(f"✅ 韓股清單整合成功: {len(items)} 檔（產業別覆蓋: {valid_sector_count} 檔）")
        return items

    except Exception as e:
        log(f"❌ 清單獲取與整合過程出錯: {e}")
        return []

# ========== 4. 批量下載股價 (yfinance) ==========

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
        
        # 處理 yfinance 回傳的多級 Index 結構
        for symbol in symbols:
            try:
                df = data[symbol].copy() if len(symbols) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty:
                    continue
                
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                
                # 標準化日期
                date_col = 'date' if 'date' in df.columns else df.columns[0]
                df['date_str'] = pd.to_datetime(df[date_col]).dt.strftime('%Y-%m-%d')
                
                for _, r in df.iterrows():
                    vol = int(r['volume']) if pd.notna(r['volume']) else 0
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_prices (date, symbol, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (r['date_str'], symbol, r['open'], r['high'], r['low'], r['close'], vol))
                success += 1
            except:
                continue

        conn.commit()
        conn.close()
        return success
    except:
        return 0

# ========== 5. 主程序入口 ==========

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
    
    # 1. 獲取名單與產業資料
    items = get_kr_stock_list()
    if not items:
        log("🛑 無法獲取有效清單，同步終止")
        return {"success": 0, "total": 0, "has_changed": False}

    # 2. 切分批次下載
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始分批同步股價 | 總目標: {len(items)} 檔 | 總批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_batch, b, mode): b for b in batches}
        for f in tqdm(as_completed(futures), total=len(batches), desc="KR同步"):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()

    # 3. 完工優化
    log("🧹 執行資料庫重組 (VACUUM)...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！成功下載: {total_success} 檔 | 總耗時: {duration:.1f} 分鐘")
    
    return {"success": total_success, "total": len(items), "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
