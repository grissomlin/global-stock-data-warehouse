# -*- coding: utf-8 -*-
"""
downloader_hk.py
----------------
港股資料下載器（最終穩定版）

✔ HKEX 清單 → 僅保留 5 位純數字代碼
✔ Yahoo Finance → 動態嘗試 5 位 / 4 位 .HK
✔ 修復 HKEX Excel 表頭不固定問題
✔ 與 main.py / 全球 pipeline 完全相容
"""

import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 6
BASE_DELAY  = 0.5 if IS_GITHUB_ACTIONS else 0.2


def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")


# ========== 2. DB 初始化 ==========

def init_db():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                date TEXT,
                symbol TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                PRIMARY KEY (date, symbol)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_info (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                sector TEXT,
                market TEXT,
                updated_at TEXT
            )
        """)
    finally:
        conn.close()


# ========== 3. HKEX 股票清單解析（只存 5 位代碼） ==========

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
    log("📡 正在從港交所下載股票清單...")

    try:
        r = requests.get(url, timeout=30, verify=False)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
    except Exception as e:
        log(f"❌ 無法下載 HKEX 清單: {e}")
        return []

    # 找表頭
    header_row = None
    for i in range(20):
        row_vals = [
            str(x).replace("\xa0", " ").strip()
            for x in df_raw.iloc[i].values
        ]
        if any("Stock Code" in v for v in row_vals) and any("Short Name" in v for v in row_vals):
            header_row = i
            break

    if header_row is None:
        log("❌ 無法辨識 HKEX Excel 表頭")
        return []

    df = df_raw.iloc[header_row + 1:].copy()
    df.columns = [
        str(x).replace("\xa0", " ").strip()
        for x in df_raw.iloc[header_row].values
    ]

    code_col = next(c for c in df.columns if "Stock Code" in c)
    name_col = next(c for c in df.columns if "Short Name" in c)

    conn = sqlite3.connect(DB_PATH)
    stock_list = []

    for _, row in df.iterrows():
        code_5d = normalize_code_5d(row[code_col])
        if not code_5d:
            continue

        name = str(row[name_col]).strip()

        conn.execute("""
            INSERT OR REPLACE INTO stock_info
            (symbol, name, sector, market, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            code_5d,
            name,
            "Unknown",
            "HKEX",
            datetime.now().strftime("%Y-%m-%d")
        ))

        stock_list.append((code_5d, name))

    conn.commit()
    conn.close()

    log(f"✅ HKEX 清單解析完成：{len(stock_list)} 檔普通股")
    return stock_list


# ========== 4. Yahoo Finance symbol 嘗試 ==========

def possible_yahoo_symbols(code_5d: str):
    syms = [f"{code_5d}.HK"]
    if code_5d.startswith("0"):
        syms.append(f"{code_5d.lstrip('0')}.HK")
    return syms


def download_one(stock, mode="hot"):
    code_5d, _ = stock
    start_date = "2020-01-01" if mode == "hot" else "2000-01-01"

    for sym in possible_yahoo_symbols(code_5d):
        try:
            time.sleep(random.uniform(BASE_DELAY, BASE_DELAY + 0.3))
            tk = yf.Ticker(sym)
            hist = tk.history(start=start_date, auto_adjust=True, timeout=20)

            if hist is None or hist.empty:
                continue

            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]

            if 'date' in hist.columns:
                hist['date'] = (
                    pd.to_datetime(hist['date'])
                    .dt.tz_localize(None)
                    .dt.strftime('%Y-%m-%d')
                )

            df_final = hist[['date', 'open', 'high', 'low', 'close', 'volume']]
            df_final['symbol'] = sym

            conn = sqlite3.connect(DB_PATH, timeout=30)
            df_final.to_sql(
                "stock_prices",
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=200
            )
            conn.close()
            return True
        except Exception:
            continue

    return False


# ========== 5. 主流程（main.py 會呼叫） ==========

def run_sync(mode="hot"):
    start_time = time.time()
    init_db()

    stocks = get_hk_stock_list()
    if not stocks:
        return {"success": 0, "has_changed": False}

    log(f"🚀 開始港股同步 | 目標: {len(stocks)} 檔")

    success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_one, s, mode) for s in stocks]
        for f in tqdm(as_completed(futures), total=len(futures), desc="HK同步"):
            if f.result():
                success += 1

    conn = sqlite3.connect(DB_PATH)
    unique_cnt = conn.execute(
        "SELECT COUNT(DISTINCT symbol) FROM stock_prices"
    ).fetchone()[0]
    conn.execute("VACUUM")
    conn.close()

    duration = (time.time() - start_time) / 60
    log(f"📊 港股完成 | 成功 {success}/{len(stocks)} | DB 股票數 {unique_cnt} | {duration:.1f} 分")

    return {
        "success": success,
        "total": len(stocks),
        "has_changed": success > 0
    }


if __name__ == "__main__":
    run_sync(mode="hot")
