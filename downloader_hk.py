# -*- coding: utf-8 -*-
import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from notifier import StockNotifier # 假設你的通知工具類在此

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

# ✅ 批次加速設定
BATCH_SIZE = 40 
MAX_WORKERS = 4 if IS_GITHUB_ACTIONS else 10
BATCH_DELAY = (3.0, 6.0) if IS_GITHUB_ACTIONS else (0.5, 1.0)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 偵察任務：抽樣獲取 Info 並送往 Telegram ==========

def scout_hk_info_to_telegram(stock_list):
    """抽樣偵察香港股票的產業資訊"""
    notifier = StockNotifier()
    log("🔍 啟動偵察任務：抽樣獲取產業資訊...")
    
    # 隨機挑選 10 檔進行測試
    sample_stocks = random.sample(stock_list, min(10, len(stock_list)))
    report_items = []

    for code_5d in sample_stocks:
        sym = f"{code_5d}.HK"
        try:
            tk = yf.Ticker(sym)
            info = tk.info
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            report_items.append(f"🔹 {sym} | {sector} | {industry}")
        except Exception as e:
            report_items.append(f"🔸 {sym} | 獲取失敗: {str(e)[:20]}")
    
    msg = (
        f"🇭🇰 <b>港股產業偵察報告</b>\n"
        f"抽樣總數: {len(report_items)}\n"
        f"--------------------------\n" + 
        "\n".join(report_items) + 
        f"\n--------------------------\n"
        f"<i>如果上面顯示 N/A，代表 Yahoo API 沒給資料。</i>"
    )
    notifier.send_telegram(msg)
    log("✅ 偵察報告已送往 Telegram。")

# ========== 3. 獲取名單與批次下載 ==========

def get_hk_stock_list():
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(url, timeout=30, verify=False, headers=headers)
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 尋找表頭 (簡化邏輯)
        df = df_raw.iloc[3:].copy() # 通常前幾行是廢棄的
        stock_list = []
        for val in df[0].dropna():
            digits = re.sub(r"\D", "", str(val))
            if digits: stock_list.append(digits.zfill(5))
        return list(set(stock_list))
    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        return []

def download_batch(codes_batch, mode):
    yahoo_map = {f"{c}.HK": c for c in codes_batch}
    symbols = list(yahoo_map.keys())
    start_date = "2020-01-01" if mode == "hot" else "2010-01-01"
    
    try:
        # 批次下載價格 (不含 info)
        data = yf.download(tickers=symbols, start=start_date, group_by='ticker', 
                           auto_adjust=True, progress=False, timeout=45)
        
        if data.empty: return 0
        
        conn = sqlite3.connect(DB_PATH)
        success = 0
        for sym, code_5d in yahoo_map.items():
            try:
                df = data[sym].copy() if len(symbols) > 1 else data.copy()
                df.dropna(how='all', inplace=True)
                if df.empty: continue
                
                # 儲存價格邏輯 (略過具體 SQL 以維持簡潔，同你之前的版本)
                success += 1
            except: continue
        conn.close()
        return success
    except: return 0

# ========== 4. 主流程 ==========

def run_sync(mode="hot"):
    start_time = time.time()
    
    # 1. 獲取名單
    codes = get_hk_stock_list()
    if not codes: return {"success": 0, "has_changed": False}

    # 2. 🔥 先執行 Telegram 偵察報告 (不影響主流程)
    scout_hk_info_to_telegram(codes)

    # 3. 執行批次加速下載
    batches = [codes[i:i + BATCH_SIZE] for i in range(0, len(codes), BATCH_SIZE)]
    log(f"🚀 開始港股批次同步 | 目標: {len(codes)} 檔 | 批次: {len(batches)}")

    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {executor.submit(download_batch, b, mode): b for b in batches}
        for f in tqdm(as_completed(future_to_batch), total=len(batches), desc="HK同步"):
            time.sleep(random.uniform(*BATCH_DELAY))
            total_success += f.result()

    duration = (time.time() - start_time) / 60
    log(f"📊 港股批次同步完成 | 費時: {duration:.1f} 分")
    return {"success": total_success, "has_changed": total_success > 0}

if __name__ == "__main__":
    run_sync()
