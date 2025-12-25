# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
from datetime import datetime
# 💡 確保這行能正確導入你的通知工具
from notifier import StockNotifier

# ========== 1. 環境設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 數據偵察：將 CSV 內容傳給 Telegram ==========

def debug_krx_content_to_telegram(df):
    """診斷專用：把抓到的欄位名稱與資料傳到手機"""
    try:
        notifier = StockNotifier()
        
        # A. 取得所有欄位標題
        all_columns = " | ".join(df.columns.tolist())
        
        # B. 取得前 3 筆範例資料
        samples = ""
        for i in range(min(3, len(df))):
            row_data = " | ".join([str(x) for x in df.iloc[i].values])
            samples += f"📍 樣本 {i+1}:\n{row_data}\n\n"
        
        msg = (
            f"🇰🇷 <b>KRX 數據偵察報告</b>\n\n"
            f"<b>【所有欄位標題】</b>\n<code>{all_columns}</code>\n\n"
            f"<b>【數據內容範例】</b>\n<pre>{samples}</pre>\n"
            f"<i>請檢查上述內容是否有「業種」、「產業」或 Industry 字眼。</i>"
        )
        
        log("📤 正在發送偵察數據至 Telegram...")
        notifier.send_telegram(msg)
    except Exception as e:
        log(f"⚠️ Telegram 偵察發送失敗: {e}")

# ========== 3. 獲取名單 (含偵察邏輯) ==========

def get_kr_stock_list():
    log("📡 正在向 KRX 請求原始清單以進行結構分析...")
    
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    # 先用原本的網址診斷，看看是不是這個網址本身就沒產業資料
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/'
    }
    
    try:
        # 1. 取得 OTP
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        otp_code = r_otp.text
        
        # 2. 下載 CSV
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949'
        
        df = pd.read_csv(io.StringIO(r_csv.text))
        
        # 🚀 執行偵察：送資料去 Telegram
        debug_krx_content_to_telegram(df)
        
        log(f"✅ 檔案讀取成功，共 {len(df)} 筆資料，已送出診斷訊息。")
        
        # 為了不讓程式空轉，回傳空清單，這樣 main.py 會顯示「數據無變動」並結束
        return []

    except Exception as e:
        log(f"❌ 偵察過程出錯: {e}")
        return []

# ========== 4. 必備的 run_sync 接口 (解決 AttributeError) ==========

def run_sync(mode='hot'):
    """主程序入口"""
    start_time = time.time()
    
    # 執行偵察獲取名單
    get_kr_stock_list()
    
    # 因為是診斷模式，我們直接回報 0 變動
    log("🏁 診斷模式執行完畢，請查看 Telegram。")
    return {
        "success": 0,
        "total": 0,
        "has_changed": False,
        "duration_minutes": f"{(time.time() - start_time)/60:.2f}"
    }

if __name__ == "__main__":
    run_sync()
