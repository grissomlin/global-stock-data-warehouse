# -*- coding: utf-8 -*-
"""
downloader_jp.py
----------------
日股資料下載器（穩定單執行緒版）

✔ 改為單執行緒循環：確保 JPX 大量標的下載時數據 100% 準確
✔ 自動處理 .xls：解決 JPX 官方清單讀取問題
✔ 結構統一：完全支援 Alpha Lab 連動機制
"""

import os, sys, sqlite3, time, random, io, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from tqdm import tqdm
import requests

# =====================================================
# 1. 環境設定
# =====================================================
MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# =====================================================
# 2. Excel 支援與資料庫初始化
# =====================================================
def ensure_excel_tool():
    try:
        import xlrd
    except ImportError:
        log("🔧 安裝 xlrd 以支援 JPX 官方表格...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd"])

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                date TEXT, symbol TEXT, open REAL, high REAL, 
                low REAL, close REAL, volume INTEGER,
                PRIMARY KEY (date, symbol)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, 
                market TEXT, updated_at TEXT
            )
        """)
        conn.commit()
    finally:
        conn.close()

# =====================================================
# 3. 取得 JPX 股票清單
# =====================================================
def get_jp_stock_list():
    ensure_excel_tool()
    url = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html"
    }

    log("📡 正在從 JPX 官網同步最新股票名單...")

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content))
    except Exception as e:
        log(f"❌ 下載失敗: {e}")
        return []

    # JPX Excel 標準欄位定義
    C_CODE = "Local Code"
    C_NAME = "Name (English)"
    C_PROD = "Section/Products"
    C_SECTOR = "33 Sector(name)"

    conn = sqlite3.connect(DB_PATH)
    stock_list = []

    for _, row in df.iterrows():
        raw_code = row.get(C_CODE)
        if pd.isna(raw_code): continue

        # 修正 Excel 代碼格式 (例如 7203.0 -> 7203)
        code = str(raw_code).split(".")[0].strip()

        # 僅保留 4 位數純數字普通股
        if not (len(code) == 4 and code.isdigit()): continue

        product = str(row.get(C_PROD, "")).strip()
        if product.startswith("ETFs"): continue # 排除 ETF

        symbol = f"{code}.T"
        name = str(row.get(C_NAME, "")).strip()
        sector = str(row.get(C_SECTOR, "Unknown")).strip()
        
        conn.execute("""
            INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (symbol, name, sector, product, datetime.now().strftime("%Y-%m-%d")))
        stock_list.append((symbol, name))

    conn.commit()
    conn.close()
    log(f"✅ 日股名單同步完成：共 {len(stock_list)} 檔")
    return stock_list

# =====================================================
# 4. 下載核心 (單執行緒穩定版)
# =====================================================
def download_one_jp(symbol, mode):
    start_date = "2020-01-01" if mode == "hot" else "2000-01-01"
    max_retries = 2
    
    for attempt in range(max_retries + 1):
        try:
            # 💡 核心修正：threads=False 徹底禁止併發，解決資料錯亂
            df = yf.download(symbol, start=start_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=30)

            if df is None or df.empty:
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                return None

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")

            df_final = df[["date", "open", "high", "low", "close", "volume"]].copy()
            df_final["symbol"] = symbol
            return df_final
        except Exception:
            if attempt < max_retries:
                time.sleep(3)
                continue
            return None

# =====================================================
# 5. 主流程
# =====================================================
def run_sync(mode="hot"):
    start_time = time.time()
    init_db()

    items = get_jp_stock_list()
    if not items:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始日股同步 (安全模式) | 目標: {len(items)} 檔")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 單執行緒循環
    pbar = tqdm(items, desc="JP同步")
    for symbol, name in pbar:
        df_res = download_one_jp(symbol, mode)
        
        if df_res is not None:
            # 使用 executemany 批次寫入以增進單執行緒下的效能
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
        
        # 🟢 加入微小延遲防止被 Yahoo 封鎖
        time.sleep(0.05)

    conn.commit()
    
    # 統計
    log("🧹 執行資料庫 VACUUM...")
    conn.execute("VACUUM")
    total_in_db = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 JP 同步完成 | 更新成功: {success_count}/{len(items)} | 費時 {duration:.1f} 分")

    return {
        "success": success_count,
        "total": total_in_db,
        "has_changed": success_count > 0
    }

if __name__ == "__main__":
    run_sync(mode="hot")
