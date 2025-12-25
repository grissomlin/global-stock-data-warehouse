# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

BATCH_SIZE = 40
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10
BATCH_DELAY = (4.0, 8.0) if IS_GITHUB_ACTIONS else (0.5, 1.2)

def log(msg):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ---------- DB ----------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            date TEXT, symbol TEXT,
            open REAL, high REAL, low REAL, close REAL, volume INTEGER,
            PRIMARY KEY (date, symbol)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stock_info (
            symbol TEXT PRIMARY KEY,
            name TEXT, sector TEXT, market TEXT, updated_at TEXT
        )
    """)
    conn.close()

# ---------- KRX LIST ----------
def get_kr_stock_list():
    log("📡 從 KRX 取得上市公司名冊 (MDCSTAT01901)...")

    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    dn_url  = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"

    otp_params = {
        "locale": "ko_KR",
        "share": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT01901"
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://data.krx.co.kr/"
    }

    try:
        otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15).text
        r = requests.post(dn_url, data={"code": otp}, headers=headers, timeout=30)
        r.encoding = "cp949"
        df = pd.read_csv(io.StringIO(r.text))
    except Exception as e:
        log(f"❌ KRX 名冊下載失敗: {e}")
        return []

    if df.empty:
        log("❌ KRX 回傳空名冊，直接中止")
        return []

    # 欄位：종목코드 | 종목명 | 시장구분 | 업종명
    conn = sqlite3.connect(DB_PATH)
    items = []

    for _, row in df.iterrows():
        code = re.sub(r"\D", "", str(row["종목코드"])).zfill(6)
        market = str(row["시장구분"])
        suffix = ".KS" if "KOSPI" in market.upper() else ".KQ"
        symbol = f"{code}{suffix}"

        name = str(row["종목명"]).strip()
        sector = str(row.get("업종명", "Unknown")).strip()

        conn.execute("""
            INSERT OR REPLACE INTO stock_info
            VALUES (?,?,?,?,?)
        """, (symbol, name, sector, market, datetime.now().strftime("%Y-%m-%d")))

        items.append((symbol, name))

    conn.commit()
    conn.close()

    log(f"✅ 韓國上市公司導入成功: {len(items)} 檔")
    return items

# ---------- YFINANCE ----------
def download_batch(batch, mode):
    symbols = [x[0] for x in batch]
    start = "2020-01-01" if mode == "hot" else "2010-01-01"

    try:
        data = yf.download(
            tickers=symbols,
            start=start,
            auto_adjust=True,
            threads=False,
            progress=False
        )
        if data.empty:
            return 0

        conn = sqlite3.connect(DB_PATH, timeout=60)
        success = 0

        for sym in symbols:
            try:
                df = data[sym].dropna(how="all")
                if df.empty:
                    continue
                df.reset_index(inplace=True)
                df.columns = [c.lower() for c in df.columns]
                for _, r in df.iterrows():
                    conn.execute(
                        "INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                        (r["date"].strftime("%Y-%m-%d"), sym,
                         r["open"], r["high"], r["low"], r["close"], r["volume"])
                    )
                success += 1
            except:
                continue

        conn.commit()
        conn.close()
        return success
    except:
        return 0

# ---------- MAIN ----------
def run_sync(mode="hot"):
    init_db()
    items = get_kr_stock_list()

    if not items:
        log("⛔ 韓國名單為 0，跳過後續流程")
        return {"success": 0, "has_changed": False}

    batches = [items[i:i+BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 KR 同步開始 | 股票數 {len(items)}")

    total = 0
    with ThreadPoolExecutor(MAX_WORKERS) as exe:
        futures = [exe.submit(download_batch, b, mode) for b in batches]
        for f in tqdm(as_completed(futures), total=len(futures), desc="KR同步"):
            time.sleep(random.uniform(*BATCH_DELAY))
            total += f.result()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    conn.close()

    log(f"📊 KR 完成 | 成功 {total}/{len(items)}")
    return {"success": total, "total": len(items), "has_changed": total > 0}

if __name__ == "__main__":
    run_sync()
