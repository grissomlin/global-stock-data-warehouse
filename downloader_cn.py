# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg: str):
    try:
        __import__(pkg)
    except ImportError:
        print(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

# ========== 核心參數設定 ==========
MAX_WORKERS = 4  # A 股建議維持 4-8，避免被 Yahoo 封鎖

def get_full_stock_list():
    """使用 akshare 獲取 A 股完整清單"""
    ensure_pkg("akshare")
    import akshare as ak
    
    threshold = 4000  
    print("📡 正在透過 akshare 獲取 A 股 (SH/SZ) 清單...")
    try:
        # 嘗試從東方財富獲取，這在本地執行較穩定
        df = ak.stock_info_a_code_name()
        df['code'] = df['code'].astype(str).str.zfill(6)
        
        # 只過濾核心板塊標的
        valid_prefixes = ('000','001','002','300','600','601','603','605')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        res = []
        for code in df['code']:
            symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
            res.append(symbol)
            
        final_list = list(set(res))
        if len(final_list) >= threshold:
            print(f"✅ 成功獲取 {len(final_list)} 檔 A 股代號")
            return final_list
    except Exception as e:
        print(f"❌ A 股清單抓取失敗: {e}")
    
    return ["600519.SS"] # 保底：貴州茅台

def fetch_single_stock(symbol, period):
    """單檔下載：加入更長的 timeout 以應對 max 模式"""
    try:
        # 下載 max 歷史需要更多緩衝時間
        time.sleep(random.uniform(0.5, 1.0))
        tk = yf.Ticker(symbol)
        
        # 增加 timeout 到 30 秒，因為 max 數據包較大
        hist = tk.history(period=period, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            
            if 'date' in hist.columns:
                # 標準化日期格式
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                # 確保只回傳核心欄位
                cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
                return hist[cols]
    except Exception:
        return None
    return None

def fetch_cn_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    # ✨ 已修正：初次抓取改為 max，之後每日更新 7 天
    period = "max" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 中國 A 股任務啟動: {'全量(max)' if is_first_time else '增量(7d)'}, 目標: {len(items)} 檔")
    
    all_dfs = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_stock, tkr, period): tkr for tkr in items}
        
        count = 0
        for future in as_completed(futures):
            res = future.result()
            if res is not None:
                all_dfs.append(res)
            
            count += 1
            if count % 200 == 0:
                print(f"📊 進度: {count}/{len(items)} 檔 A 股處理中...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ A 股處理完成，共獲取 {len(final_df)} 筆交易記錄")
        return final_df
    return pd.DataFrame()
