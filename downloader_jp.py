# -*- coding: utf-8 -*-
import os, sys, time, random, sqlite3, requests, io, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# =====================================================
# 1. 環境設定
# =====================================================

MARKET_CODE = "jp-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "jp_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 8

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# =====================================================
# 2. Excel 讀取支援
# =====================================================

def ensure_excel_tool():
    try:
        import xlrd  # noqa
    except ImportError:
        log("🔧 安裝 xlrd 以支援 .xls")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "xlrd"])

# =====================================================
# 3. DB 初始化（結構對齊全球）
# =====================================================

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
        conn.commit()
    finally:
        conn.close()

# =====================================================
# 4. 取得 JPX 股票清單（完全對齊實際 Excel 欄位）
# =====================================================

def get_jp_stock_list():
    """
    使用 JPX 官方英文版 Excel
    欄位實際為：
    Local Code / Name (English) / Section/Products / 33 Sector(name)
    """
    ensure_excel_tool()

    url = "https://www.jpx.co.jp/english/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_e.xls"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.jpx.co.jp/english/markets/statistics-equities/misc/01.html"
    }

    log("📡 正在從 JPX 下載官方股票清單...")

    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        df = pd.read_excel(io.BytesIO(r.content))
        log("✅ JPX Excel 下載成功")
    except Exception as e:
        log(f"❌ JPX Excel 下載失敗: {e}")
        return []

    # 👉 明確指定欄位（不再猜）
    C_CODE = "Local Code"
    C_NAME = "Name (English)"
    C_PROD = "Section/Products"
    C_SECTOR = "33 Sector(name)"

    conn = sqlite3.connect(DB_PATH)
    stock_list = []

    for _, row in df.iterrows():
        raw_code = row.get(C_CODE)
        if pd.isna(raw_code):
            continue

        # 修正 Excel 將代碼讀成 float 的問題
        code = str(raw_code).split(".")[0].strip()

        # ✅ 普通股：4 位純數字
        if not (len(code) == 4 and code.isdigit()):
            continue

        product = str(row.get(C_PROD, "")).strip()

        # ✅ 明確排除 ETF / ETN
        if product.startswith("ETFs"):
            continue

        symbol = f"{code}.T"
        name = str(row.get(C_NAME, "")).strip()
        sector = str(row.get(C_SECTOR, "Unknown")).strip()
        market = product  # Prime / Standard / Growth

        conn.execute("""
            INSERT OR REPLACE INTO stock_info
            (symbol, name, sector, market, updated_at)
            VALUES (?, ?, ?, ?, ?)
        """, (
            symbol,
            name,
            sector,
            market,
            datetime.now().strftime("%Y-%m-%d")
        ))

        stock_list.append((symbol, name))

    conn.commit()
    conn.close()

    log(f"✅ 日股清單導入完成：{len(stock_list)} 檔普通股")
    return stock_list

# =====================================================
# 5. 下載股價（穩定版）
# =====================================================

def download_one(args):
    symbol, name, mode = args
    start_date = "2020-01-01" if mode == "hot" else "2000-01-01"

    for attempt in range(3):
        try:
            time.sleep(random.uniform(0.1, 0.3))
            tk = yf.Ticker(symbol)
            df = tk.history(start=start_date, auto_adjust=True, timeout=30)

            if df is None or df.empty:
                if attempt < 2:
                    continue
                return {"symbol": symbol, "status": "empty"}

            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]

            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).dt.strftime("%Y-%m-%d")

            out = df[["date", "open", "high", "low", "close", "volume"]].copy()
            out["symbol"] = symbol

            conn = sqlite3.connect(DB_PATH, timeout=60)
            out.to_sql(
                "stock_prices",
                conn,
                if_exists="append",
                index=False,
                method=lambda t, c, k, d: c.executemany(
                    f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d
                )
            )
            conn.close()
            return {"symbol": symbol, "status": "success"}

        except Exception:
            time.sleep(2)

    return {"symbol": symbol, "status": "error"}

# =====================================================
# 6. 主流程
# =====================================================

def run_sync(mode="hot"):
    start = time.time()
    init_db()

    items = get_jp_stock_list()
    if not items:
        log("⚠️ JP 無任何股票，直接結束")
        return {"success": 0, "has_changed": False}

    log(f"🚀 JP 同步開始 | 目標 {len(items)} 檔")

    stats = {"success": 0, "empty": 0, "error": 0}
    fails = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(download_one, (s, n, mode)): s for s, n in items}
        for f in tqdm(as_completed(futures), total=len(futures), desc="JP同步"):
            res = f.result()
            stats[res["status"]] += 1
            if res["status"] == "error":
                fails.append(res["symbol"])

    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    total = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_info").fetchone()[0]
    conn.close()

    mins = (time.time() - start) / 60
    log(f"📊 JP 完成 | 費時 {mins:.1f} 分")
    log(f"📈 股票總數 {total} | 成功 {stats['success']} | 空 {stats['empty']} | 失敗 {stats['error']}")

    return {
        "success": stats["success"],
        "total": total,
        "fail_list": fails,
        "has_changed": stats["success"] > 0
    }

if __name__ == "__main__":
    run_sync(mode="hot")
