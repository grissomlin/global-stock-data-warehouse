# -*- coding: utf-8 -*-
import os, time, random, requests, json
import pandas as pd
import yfinance as yf
from datetime import datetime
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 核心參數設定 ==========
MAX_WORKERS = 5  # 美股標的多，稍微調高執行緒，但配合較長的 Jitter 隨機延遲
LIST_THRESHOLD = 3000

def classify_security(name: str, is_etf: bool) -> str:
    """過濾掉權證、優先股、ADR 等非普通股標的，確保資料庫純淨"""
    if is_etf: return "Exclude"
    n_upper = str(name).upper()
    # 排除常見非普通股關鍵字
    exclude_keywords = ["WARRANT", "RIGHTS", "UNIT", "PREFERRED", "DEPOSITARY", "ADR", "FOREIGN", "DEBENTURE"]
    if any(kw in n_upper for kw in exclude_keywords): return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """獲取美股普通股清單 (NASDAQ & NYSE)"""
    all_tickers = []
    print("📡 正在從 Nasdaq 官網獲取最新美股清單...")
    
    # 抓取 Nasdaq 本身與 NYSE/AMEX 等其他市場
    for site in ["nasdaqlisted.txt", "otherlisted.txt"]:
        try:
            url = f"https://www.nasdaqtrader.com/dynamic/symdir/{site}"
            r = requests.get(url, timeout=15)
            # 官網是以 | 分隔的文本檔
            df = pd.read_csv(StringIO(r.text), sep="|")
            # 排除測試標的
            df = df[df["Test Issue"] == "N"]
            
            # 校正欄位名稱
            sym_col = "Symbol" if site == "nasdaqlisted.txt" else "NASDAQ Symbol"
            
            # 執行安全分類過濾 (排除 ETF 與衍生品)
            df["Category"] = df.apply(lambda row: classify_security(row["Security Name"], row["ETF"] == "Y"), axis=1)
            valid_df = df[df["Category"] == "Common Stock"]
            
            for _, row in valid_df.iterrows():
                # 處理符號中的特殊字元 (例如 BRK.B)
                ticker = str(row[sym_col]).strip().replace('$', '-')
                all_tickers.append(ticker)
            time.sleep(1) 
        except Exception as e:
            print(f"⚠️ {site} 清單抓取失敗: {e}")

    final_list = list(set(all_tickers))
    if len(final_list) < LIST_THRESHOLD:
        print(f"❌ 警告：美股清單數量異常 ({len(final_list)})")
    else:
        print(f"✅ 成功獲取 {len(final_list)} 檔美股普通股代號")
    return final_list

def fetch_single_stock(tkr, period):
    """具備抗封鎖機制的單檔下載"""
    # 🚀 Jitter：隨機等待，下載 max 歷史時建議稍微拉長，避免被 Yahoo 偵測
    time.sleep(random.uniform(0.5, 1.2))
    
    try:
        tk = yf.Ticker(tkr)
        # 增加 timeout 到 30 秒，因為 max 模式的數據包通常很大
        hist = tk.history(period=period, auto_adjust=True, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            
            # ✅ 重要：美股時區與格式處理
            if 'date' in hist.columns:
                # 標準化日期：移除時區並轉為 YYYY-MM-DD 字串
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = tkr
                # 返回資料庫所需的核心欄位
                cols = ['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']
                return hist[cols]
    except Exception:
        # 下載失敗不報錯，直接跳過該股
        return None
    return None

def fetch_us_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    # ✨ 已修正：初次抓取由 10y 改為 max
    period = "max" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 美股任務啟動: {'全量歷史(max)' if is_first_time else '增量更新(7d)'}, 目標: {len(items)} 檔")
    
    all_dfs = []
    # 使用線程池平行下載
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
