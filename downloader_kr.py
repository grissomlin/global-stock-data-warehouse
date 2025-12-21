# -*- coding: utf-8 -*-
import os, sys, time, random, subprocess
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        print(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

ensure_pkg("pykrx")
from pykrx import stock as krx

# ========== 核心參數設定 ==========
MAX_WORKERS = 3  # 韓國市場對頻繁請求較敏感，維持低執行緒

def get_full_stock_list():
    """從 KRX 獲取最新 KOSPI/KOSDAQ 清單"""
    threshold = 2000  
    print("📡 正在獲取韓股 (KOSPI/KOSDAQ) 完整清單...")
    try:
        today = pd.Timestamp.today().strftime("%Y%m%d")
        lst = []
        # KS = KOSPI (主要板塊), KQ = KOSDAQ (創業板)
        for mk, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            tickers = krx.get_market_ticker_list(today, market=mk)
            for t in tickers:
                lst.append(f"{t.zfill(6)}{suffix}")
        
        if len(lst) >= threshold:
            print(f"✅ 成功獲取 {len(lst)} 檔韓股代號")
            return list(set(lst))
    except Exception as e:
        print(f"❌ 獲取韓股清單失敗: {e}")
    
    # 保底：若失敗則回傳三星電子
    return ["005930.KS"]

def fetch_single_stock(symbol, period):
    """單檔下載：具備隨機延遲保護"""
    try:
        time.sleep(random.uniform(0.5, 1.2))
        tk = yf.Ticker(symbol)
        # 根據主程式需求抓取 10y 或 7d
        hist = tk.history(period=period, interval="1d", auto_adjust=True)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            
            # 移除時區轉換為純淨日期字串
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    except:
        return None
    return None

def fetch_kr_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    # 💡 初次下載 10 年，日常更新 7 天
    period = "10y" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 韓股任務啟動: {'全量(10y)' if is_first_time else '增量(7d)'}, 目標: {len(items)} 檔")
    
    all_dfs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_stock, tkr, period): tkr for tkr in items}
        
        count = 0
        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                all_dfs.append(res)
            
            count += 1
            if count % 100 == 0:
                print(f"📊 已處理 {count}/{len(items)} 檔韓股...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 韓股處理完成，共獲取 {len(final_df)} 筆記錄")
        return final_df
    return pd.DataFrame()