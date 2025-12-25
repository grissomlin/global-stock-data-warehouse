# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
from datetime import datetime
from notifier import StockNotifier

# ========== 1. 環境設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 數據偵察函式 ==========

def debug_to_telegram(df, url_code):
    """將抓到的檔案結構送往 Telegram 診斷"""
    try:
        notifier = StockNotifier()
        cols = " | ".join(df.columns.tolist())
        
        sample_rows = ""
        for i in range(min(3, len(df))):
            row_values = [str(v)[:15] for v in df.iloc[i].values]
            sample_rows += f"📍 樣本 {i+1}:\n{' | '.join(row_values)}\n\n"
        
        msg = (
            f"🇰🇷 <b>KRX 數據偵察 (接口: {url_code})</b>\n\n"
            f"<b>【標題欄位】</b>\n<code>{cols}</code>\n\n"
            f"<b>【數據內容】</b>\n<pre>{sample_rows}</pre>\n"
            f"<i>請確認是否有「업종명」(業種名) 或「Sector」字眼。</i>"
        )
        log(f"📤 正在發送 {url_code} 偵察數據至 Telegram...")
        notifier.send_telegram(msg)
    except Exception as e:
        log(f"⚠️ Telegram 發送失敗: {e}")

# ========== 3. 偵察任務執行 ==========

def get_kr_stock_list():
    log("📡 正在向 KRX 請求業種分類清單 (MDCSTAT00201)...")
    
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT00201' # 專門的業種接口
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/'
    }
    
    try:
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        otp_code = r_otp.text
        
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949'
        
        df = pd.read_csv(io.StringIO(r_csv.text))
        
        # 🔥 執行診斷
        debug_to_telegram(df, "MDCSTAT00201")
        
        log(f"✅ 檔案讀取成功，欄位數: {len(df.columns)}，已送出 Telegram。")
        return [] 
    except Exception as e:
        log(f"❌ 偵察失敗: {e}")
        return []

# ========== 4. 解決 AttributeError 的入口 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    get_kr_stock_list()
    log("🏁 診斷任務完成。")
    return {"success": 0, "total": 0, "has_changed": False}

if __name__ == "__main__":
    run_sync()
