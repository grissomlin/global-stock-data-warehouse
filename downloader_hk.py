# -*- coding: utf-8 -*-
"""
downloader_hk.py
----------------
港股資料下載器（穩定單執行緒版）

✔ 改為單執行緒循環：徹底解決記憶體錯亂與數據污染
✔ 強化判定邏輯：精準對應港股 4 位或 5 位代碼
✔ 支援連動觸發：與 main.py 完全相容
"""

import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from io import StringIO
from datetime import datetime
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}", flush=True)

# ========== 2. 資料庫初始化 ==========
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
    finally:
        conn.close()

# ========== 3. HKEX 清單解析 ==========
def normalize_code_5d(val) -> str:
    digits = re.sub(r"\D", "", str(val))
    if digits.isdigit() and 1 <= int(digits) <= 99999:
        return digits.zfill(5)
    return ""

def get_hk_stock_list():
    url = (
        "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/"
        "Securities/Securities-Lists/"
        "Securities-Using-Standard-Transfer-Form-(including-GEM)-"
        "By-Stock-Code-Order/secstkorder.xls"
    )
    log("📡 正在從港交所下載最新股票清單...")

    try:
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
    except Exception as e:
        log(f"❌ 無法獲取 HKEX 清單: {e}")
        return []

    # 找表頭索引
    header_row = None
    for i in range(min(20, len(df_raw))):
        row_vals = [str(x).replace("\xa0", " ").strip() for x in df_raw.iloc[i].values]
        if any("Stock Code" in v for v in row_vals) and any("Short Name" in v for v in row_vals):
            header_row = i
            break

    if header_row is None:
        log("❌ 無法辨識 HKEX Excel 結構")
        return []

    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = [str(x).replace("\xa0", " ").strip() for x in df_raw.iloc[header_row].values]

    code_col = next(c for c in df.columns if "Stock Code" in c)
    name_col = next(c for c in df.columns if "Short Name" in c)

    conn = sqlite3.connect(DB_PATH)
    stock_list = []

    for _, row in df.iterrows():
        code_5d = normalize_code_5d(row[code_col])
        if not code_5d: continue

        name = str(row[name_col]).strip()
        conn.execute("""
            INSERT OR REPLACE INTO stock_info (symbol, name, sector, market, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (code_5d, name, "HK-Share", "HKEX", datetime.now().strftime("%Y-%m-%d")))
        stock_list.append((code_5d, name))

    conn.commit()
    conn.close()
    return stock_list

# ========== 4. 下載核心邏輯 (單執行緒穩定版) ==========
def download_one_hk(code_5d, mode):
    start_date = "2020-01-01" if mode == "hot" else "2000-01-01"
    
    # 港股代碼嘗試：yfinance 有時接受 0001.HK 有時接受 1.HK
    possible_syms = [f"{code_5d}.HK"]
    if code_5d.startswith("0"):
        possible_syms.append(f"{code_5d.lstrip('0')}.HK")

    for sym in possible_syms:
        try:
            # 💡 核心修正：threads=False 防止併發錯亂
            df = yf.download(sym, start=start_date, progress=False, 
                             auto_adjust=True, threads=False, timeout=20)

            if df is None or df.empty:
                continue

            # 處理可能出現的 MultiIndex
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            # 統一日期格式
            date_col = 'date' if 'date' in df.columns else df.columns[0]
            df['date_str'] = pd.to_datetime(df[date_col]).dt.tz_localize(None).dt.strftime('%Y-%m-%d')

            df_final = df[['date_str', 'open', 'high', 'low', 'close', 'volume']].copy()
            df_final.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df_final['symbol'] = code_5d  # 資料庫存原始 5 位代碼，維持一致性

            return df_final
        except Exception:
            continue
    return None

# ========== 5. 主流程 ==========
def run_sync(mode="hot"):
    start_time = time.time()
    init_db()

    stocks = get_hk_stock_list()
    if not stocks:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始港股同步 (安全模式) | 目標: {len(stocks)} 檔")

    success_count = 0
    conn = sqlite3.connect(DB_PATH, timeout=60)
    
    # 使用單執行緒穩定循環
    pbar = tqdm(stocks, desc="HK同步")
    for code_5d, name in pbar:
        df_res = download_one_hk(code_5d, mode)
        
        if df_res is not None:
            df_res.to_sql('stock_prices', conn, if_exists='append', index=False, 
                          method=lambda table, conn, keys, data_iter: 
                          conn.executemany(f"INSERT OR REPLACE INTO {table.name} ({', '.join(keys)}) VALUES ({', '.join(['?']*len(keys))})", data_iter))
            success_count += 1
            
        # 🟢 控制下載頻率
        time.sleep(0.05)

    conn.commit()
    
    # 統計與優化
    unique_cnt = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices").fetchone()[0]
    log("🧹 執行資料庫 VACUUM...")
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 港股完成 | 更新成功: {success_count} / {len(stocks)} | 資料庫股票總數: {unique_cnt}")

    return {
        "success": success_count,
        "total": len(stocks),
        "has_changed": success_count > 0
    }

if __name__ == "__main__":
    run_sync(mode="hot")
