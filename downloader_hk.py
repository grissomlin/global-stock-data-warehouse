# -*- coding: utf-8 -*-
import os, io, re, time, random, requests
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 核心參數設定 ==========
MAX_WORKERS = 4  # 港股建議維持在此數量以防觸發 Yahoo 限流

def normalize_code5(s: str) -> str:
    """確保為 5 位數補零格式"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-5:].zfill(5) if digits else ""

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (4 位數.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品 (牛熊、權證等)"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """從 HKEX 獲取證券名單"""
    print("📡 正在從港交所 (HKEX) 獲取最新普通股清單...")
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
        
        # 定位表頭位置
        hdr_idx = 0
        for row_i in range(20):
            row_str = "".join([str(x) for x in df_raw.iloc[row_i]]).lower()
            if "stock code" in row_str and "short name" in row_str:
                hdr_idx = row_i
                break
        
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = df_raw.iloc[hdr_idx].tolist()
        
        col_code = [c for c in df.columns if "Stock Code" in str(c)][0]
        col_name = [c for c in df.columns if "Short Name" in str(c)][0]
        
        res = []
        for _, row in df.iterrows():
            name = str(row[col_name])
            if classify_security(name) == "Common Stock":
                yf_sym = to_symbol_yf(row[col_code])
                if yf_sym:
                    res.append(yf_sym)
        
        final_list = list(set(res))
        print(f"✅ 成功獲取港股清單: {len(final_list)} 檔")
        return final_list
    except Exception as e:
        print(f"❌ 港股清單抓取失敗: {e}")
        return ["0700.HK", "9988.HK", "3690.HK"] # 保底核心股

def fetch_single_stock(symbol, period):
    """單檔下載：具備隨機延遲與時區處理"""
    try:
        time.sleep(random.uniform(0.5, 1.2))
        tk = yf.Ticker(symbol)
        hist = tk.history(period=period, timeout=20)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            
            # 標準化日期格式與時區處理
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    except:
        return None
    return None

def fetch_hk_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    period = "10y" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 港股任務啟動: {'深度歷史(10y)' if is_first_time else '增量更新(7d)'}, 目標: {len(items)} 檔")
    
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
                print(f"📊 已處理 {count}/{len(items)} 檔港股...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 港股處理完成，共獲取 {len(final_df)} 筆交易記錄")
        return final_df
    return pd.DataFrame()