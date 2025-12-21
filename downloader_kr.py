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
MAX_WORKERS = 3  # 韓國市場對頻繁請求極度敏感，建議維持 3 以免被封 IP

def get_full_stock_list():
    """從 KRX 獲取最新 KOSPI/KOSDAQ 清單"""
    threshold = 2000  
    print("📡 正在獲取韓股 (KOSPI/KOSDAQ) 完整清單...")
    try:
        # 使用當前日期獲取清單
        today = pd.Timestamp.today().strftime("%Y%m%d")
        lst = []
        # KS = KOSPI (主板), KQ = KOSDAQ (創業板)
        for mk, suffix in [("KOSPI", ".KS"), ("KOSDAQ", ".KQ")]:
            tickers = krx.get_market_ticker_list(today, market=mk)
            for t in tickers:
                lst.append(f"{t.zfill(6)}{suffix}")
        
        if len(lst) >= threshold:
            print(f"✅ 成功獲取 {len(lst)} 檔韓股代號")
            return list(set(lst))
    except Exception as e:
        print(f"❌ 獲取韓股清單失敗: {e}")
    
    # 保底標的：三星電子 (Samsung Electronics)
    return ["005930.KS"]

def fetch_single_stock(symbol, period):
    """單檔下載：具備隨機延遲保護與長時間歷史支援"""
    try:
        # 下載 max 歷史數據量較大，隨機休眠 0.7 ~ 1.5 秒以求穩定
        time.sleep(random.uniform(0.7, 1.5))
        tk = yf.Ticker(symbol)
        
        # 增加 timeout 到 30 秒，並確保 auto_adjust 為 True 處理除權息
        hist = tk.history(period=period, interval="1d", auto_adjust=True, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            
            # 日期標準化：移除時區並轉為 YYYY-MM-DD
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                # 回傳資料庫所需的標準欄位
                return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    except Exception:
        return None
    return None

def fetch_kr_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    # ✨ 修改點：初次下載由 10y 改為 max
    period = "max" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 韓股任務啟動: {'全量歷史(max)' if is_first_time else '增量更新(7d)'}, 目標: {len(items)} 檔")
    
    all_dfs = []
    # 使用線程池平行下載，注意 MAX_WORKERS 不宜過高
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
        print(f"✨ 韓股處理完成，共獲取 {len(final_df)} 筆交易記錄")
        return final_df
    return pd.DataFrame()
