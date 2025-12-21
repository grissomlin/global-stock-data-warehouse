# -*- coding: utf-8 -*-
import time, random, requests
import pandas as pd
import yfinance as yf
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

# ========== 核心參數設定 ==========
MAX_WORKERS = 3  # 維持低執行緒以防 Yahoo 封鎖 IP

def get_full_stock_list():
    """獲取台股全市場清單 (包含上市、上櫃、ETF、興櫃、創新板、存託憑證)"""
    url_configs = [
        # 1. 上市普通股
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        # 2. 上市存託憑證 (DR)
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        # 3. 上櫃普通股
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        # 4. 指數股票型基金 (ETF)
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        # 5. 興櫃股票
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        # 6. 臺灣創新板
        {'name': 'tw_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        # 7. 上櫃創新板
        {'name': 'otc_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    all_items = []
    print("📡 正在從證交所獲取 7 大類市場完整清單...")
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            # 使用 pandas 讀取 HTML 表格
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                # 排除標頭重複字眼
                if code and '有價證券' not in code:
                    all_items.append(f"{code}{cfg['suffix']}")
        except Exception as e:
            print(f"⚠️ 獲取 {cfg['name']} 失敗: {e}")
            continue
            
    # 去除重複項
    unique_items = list(set(all_items))
    print(f"✅ 台股清單獲取完成，總計標的: {len(unique_items)} 檔")
    return unique_items

def fetch_single_stock(yf_tkr, period):
    """單檔下載邏輯：支援長時間歷史與抗封鎖重試"""
    try:
        time.sleep(random.uniform(0.6, 1.2))
        tk = yf.Ticker(yf_tkr)
        # 增加重試機制
        for attempt in range(2):
            try:
                # 抓取 max 資料包較大，增加 timeout
                hist = tk.history(period=period, auto_adjust=True, timeout=25)
                if hist is not None and not hist.empty:
                    hist = hist.reset_index()
                    hist.columns = [c.lower() for c in hist.columns]
                    # 日期標準化
                    if 'date' in hist.columns:
                        hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
                        hist['symbol'] = yf_tkr
                        return hist[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
                break # 成功則跳出重試
            except Exception as e:
                if "Rate limited" in str(e): 
                    print(f"🛑 {yf_tkr} 觸發限流，等待中...")
                    time.sleep(random.uniform(20, 40))
                time.sleep(random.uniform(2, 5))
    except: 
        return None
    return None

def fetch_tw_market_data(is_first_time=False):
    """主進入點：由 main.py 呼叫"""
    # ✨ 修改點：將 10y 改為 max 以獲取最完整歷史
    period = "max" if is_first_time else "7d"
    items = get_full_stock_list()
    
    print(f"🚀 台股任務啟動: {'全量歷史(max)' if is_first_time else '增量更新(7d)'}, 總數: {len(items)}")
    
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
                print(f"📊 已處理 {count}/{len(items)} 檔台股...")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"✨ 台股處理完成，共獲取 {len(final_df)} 筆交易記錄")
        return final_df
    return pd.DataFrame()
