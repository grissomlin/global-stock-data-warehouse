# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re
import pandas as pd
from datetime import datetime
from notifier import StockNotifier

# ========== 1. 偵察發送器 ==========

def scout_and_report(url_code, description):
    """抓取特定接口並發送 Telegram 報告"""
    notifier = StockNotifier()
    print(f"📡 正在偵察接口: {url_code} ({description})...")
    
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'trdDd': datetime.now().strftime("%Y%m%d"), # 針對部分需要日期的接口
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': f'dbms/MDC/STAT/standard/{url_code}'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'http://data.krx.co.kr/'
    }
    
    try:
        # 1. 獲取 OTP
        r_otp = requests.post(otp_url, data=otp_params, headers=headers, timeout=15)
        otp_code = r_otp.text
        
        # 2. 下載 CSV
        dn_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
        r_csv = requests.post(dn_url, data={'code': otp_code}, headers=headers, timeout=30)
        r_csv.encoding = 'cp949'
        
        df = pd.read_csv(io.StringIO(r_csv.text))
        
        # 3. 格式化報告
        cols = " | ".join(df.columns.tolist())
        samples = ""
        for i in range(min(2, len(df))):
            row_data = " | ".join([str(x)[:12] for x in df.iloc[i].values])
            samples += f"📍 樣本 {i+1}: {row_data}\n\n"
            
        msg = (
            f"🇰🇷 <b>KRX 偵察報告 - {url_code}</b>\n"
            f"描述: {description}\n\n"
            f"<b>【欄位】</b>\n<code>{cols}</code>\n\n"
            f"<b>【數據】</b>\n<pre>{samples}</pre>"
        )
        notifier.send_telegram(msg)
        print(f"✅ {url_code} 報告已送出。")
        
    except Exception as e:
        print(f"❌ {url_code} 偵察失敗: {e}")

# ========== 2. 主任務入口 ==========

def run_sync(mode='hot'):
    start_time = time.time()
    
    # 一次掃描三個最有潛力的接口
    targets = [
        ("MDCSTAT02101", "個股產業分類表"),
        ("MDCSTAT03402", "上市公司詳細基本資料"),
        ("MDCSTAT03501", "業種別構成股票")
    ]
    
    for code, desc in targets:
        scout_and_report(code, desc)
        time.sleep(2) # 稍微間隔以免被封
        
    print("🏁 全部偵察任務已完成，請檢查 Telegram。")
    return {"success": 0, "total": 0, "has_changed": False}

if __name__ == "__main__":
    run_sync()
