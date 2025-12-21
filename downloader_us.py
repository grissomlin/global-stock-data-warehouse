# -*- coding: utf-8 -*-
import os, time, random, requests, json
import pandas as pd
import yfinance as yf
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 核心參數設定 ==========
MAX_WORKERS = 5  # 美股檔數多，執行緒稍高，但配合 Jitter 隨機延遲
LIST_THRESHOLD = 3000

def classify_security(name: str, is_etf: bool) -> str:
    """過濾掉權證、優先股、ADR 等非普通股標的"""
    if is_etf: return "Exclude"
    n_upper = str(name).upper()
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """獲取美股普通股清單 (NASDAQ & NYSE)"""
    all_tickers = []
    print("📡 正在從 Nasdaq 官網獲取最新美股清單...")
    
    for site in ["nasdaqlisted.txt", "otherlisted.txt"]:
        try:
            url = f"https://www.nasdaqtrader.com/dynamic/symdir/{site}"
            r = requests.get(url, timeout=15)
            df = pd.read_csv(StringIO(r.text), sep="|")
            df = df[df["Test Issue"] == "N"]
            
            # 校正欄位名稱
            sym_col = "Symbol" if site == "nasdaqlisted.txt" else "NASDAQ Symbol"
            
            # 執行安全分類過濾
            df["Category"] = df.apply(lambda row: classify_security(row["Security Name"], row["ETF"] == "Y"), axis=1)
            valid_df = df[df["Category"] == "Common Stock"]
            
            for _, row in valid_df.iterrows():
                ticker = str(row[sym_col]).strip().replace('$', '-')
                all_tickers.append(ticker)
            time.sleep(1) 
        except Exception as e:
            print(f"⚠️ {site} 清單抓取失敗: {e}")

    final_list = list(set(all_tickers))
    if len(final_list) < LIST_THRESHOLD:
        print(f"❌ 警告：美股清單數量異常 ({len(final_list)})")
    return final_list

def fetch_single_stock(tkr, period):
    """具備抗封鎖機制的單檔下載"""
    # 🚀 Jitter：隨機等待防止被 Yahoo 辨識為機器人
    time.sleep(random.uniform(0.3, 0.8))
    
    try:
        tk = yf.Ticker(tkr)
        # 根據 main.py 傳入的參數決定抓 10y 或 7d
        hist = tk.history(period=period, timeout=15)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            
            # ✅ 重要：美股時區與格式處理
            if 'date' in hist.columns:
                # 移除時區資訊 (tz_localize(None))，避免 SQLite 格式混亂
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = tkr
                # 統一數據結構
                return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    except Exception:
        return None
    return None

def fetch_us_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    # 💡 初次抓取 10 年 (支援千日新高回測)，日常抓取 7 天
    period = "10y" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 美股任務啟動: {'深度歷史(10y)' if is_first_time else '增量更新(7d)'}, 目標: {len(items)} 檔")
    
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
                print(f"📊 已處理 {count}/{len(items)} 檔美股...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 美股處理完成，共獲取 {len(final_df)} 筆交易記錄。")
        return final_df
    return pd.DataFrame()