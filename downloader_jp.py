# -*- coding: utf-8 -*-
import os, sys, time, random, subprocess
import pandas as pd
import yfinance as yf
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== 自動安裝必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ========== 核心參數設定 ==========
MAX_WORKERS = 4  # 日股檔數極多，建議維持 4 以避免觸發 Yahoo API 頻率限制

def get_full_stock_list():
    """獲取日股完整清單 (TSE)"""
    threshold = 3000
    print("📡 正在從 TSE 資料庫獲取日股清單...")
    try:
        df = pd.read_csv(tse.csv_file_path)
        
        # 識別代碼欄位 (日文/英文通用相容)
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        
        res = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            # 日本股代碼通常為 4 位數字，Yahoo 格式為 1234.T
            if len(code) >= 4 and code[:4].isdigit():
                res.append(f"{code[:4]}.T")
        
        final_list = list(set(res))
        
        if len(final_list) >= threshold:
            print(f"✅ 成功獲取 {len(final_list)} 檔日股代號")
            return final_list
        else:
            print(f"⚠️ 獲取清單數量異常 ({len(final_list)} 檔)")
    except Exception as e:
        print(f"❌ 日股清單獲取失敗: {e}")
    
    # 保底標的 (豐田汽車 7203.T)
    return ["7203.T"]

def fetch_single_stock(symbol, period):
    """單檔下載：加入隨機延遲與長歷史下載支援"""
    try:
        # 下載 max 歷史數據量大，隨機休眠 0.5 ~ 1.2 秒
        time.sleep(random.uniform(0.5, 1.2))
        
        tk = yf.Ticker(symbol)
        # 增加 timeout 至 30 秒，因為 max 模式的數據包通常較大
        hist = tk.history(period=period, interval="1d", auto_adjust=True, timeout=30)
        
        if hist is not None and not hist.empty:
            hist = hist.reset_index()
            hist.columns = [c.lower() for c in hist.columns]
            
            # 標準化日期格式與時區處理
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                hist['symbol'] = symbol
                # 確保回傳標準欄位，避開不需要的資料 (如 Dividends, Stock Splits)
                return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
    except Exception:
        return None
    return None

def fetch_jp_market_data(is_first_time=False):
    """主進入點：回傳給 main.py 的數據集"""
    # ✨ 修改點：初次抓取由 10y 改為 max
    period = "max" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 日股任務啟動: {'全量歷史(max)' if is_first_time else '增量更新(7d)'}, 目標: {len(items)} 檔")
    
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
                print(f"📊 已處理 {count}/{len(items)} 檔日股...")

    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 日股處理完成，共獲取 {len(final_df)} 筆交易記錄")
        return final_df
    return pd.DataFrame()
