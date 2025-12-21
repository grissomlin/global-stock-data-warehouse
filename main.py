# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 匯入各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心設定 ==========
DB_FILE = 'global_stock_warehouse.db'
# 本地金鑰檔案路徑
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'
# Google Drive 資料夾 ID
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 

def init_db():
    """初始化資料庫與索引"""
    conn = sqlite3.connect(DB_FILE)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
        date TEXT, symbol TEXT, market TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, updated_at TEXT,
        PRIMARY KEY (date, symbol, market))''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date_market ON stock_prices (date, market)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stock_prices (symbol)')
    conn.close()

def check_is_first_time(market):
    """偵測該市場是否已有資料"""
    if not os.path.exists(DB_FILE): return True
    conn = sqlite3.connect(DB_FILE)
    try:
        count = conn.execute("SELECT COUNT(*) FROM stock_prices WHERE market = ?", (market,)).fetchone()[0]
        return count == 0
    except:
        return True
    finally:
        conn.close()

def update_database(market, df):
    """資料寫入 SQLite"""
    if df is None or df.empty: return
    df['market'] = market
    df['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_FILE)
    try:
        df.to_sql('stock_prices', conn, if_exists='append', index=False)
        print(f"✅ {market.upper()}: 資料庫寫入成功")
    except Exception:
        print(f"⚠️ {market.upper()}: 部分重複日期資料已跳過")
    finally:
        conn.close()

def upload_to_drive():
    """雲端同步邏輯：包含深度診斷模式"""
    if not os.path.exists(DB_FILE):
        print("❌ 錯誤：找不到資料庫檔案，上傳中止。")
        return

    # 1. 嘗試讀取金鑰
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    
    try:
        if env_json:
            print("☁️ [診斷] 偵測到 GitHub Secrets 環境變數，開始解析...")
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"💻 [診斷] 偵測到本地金鑰檔案: {SERVICE_ACCOUNT_FILE}")
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else:
            print("⚠️ [診斷] 找不到任何金鑰來源，跳過雲端同步。")
            return
            
        # 2. 建立 Google Drive 服務
        print("📡 [診斷] 正在建立 Google Drive API 連線...")
        service = build('drive', 'v3', credentials=creds)
        
        # 3. 檢查檔案大小
        file_size_mb = os.path.getsize(DB_FILE) / (1024 * 1024)
        print(f"📦 [診斷] 本地資料庫大小: {file_size_mb:.2f} MB")

        # 4. 搜尋雲端現有檔案
        print(f"🔍 [診斷] 正在雲端搜尋檔案: {DB_FILE}")
        query = f"name = '{DB_FILE}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        files = service.files().list(q=query, fields="files(id)", supportsAllDrives=True).execute().get('files', [])
        
        # 5. 準備媒體上傳 (開啟 resumable 以支援大檔案)
        media = MediaFileUpload(DB_FILE, mimetype='application/x-sqlite3', resumable=True)
        
        if files:
            file_id = files[0]['id']
            print(f"🔄 [診斷] 發現現有檔案 (ID: {file_id})，啟動覆蓋上傳...")
            service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        else:
            print("🆕 [診斷] 雲端無現有檔案，啟動全新上傳...")
            file_metadata = {'name': DB_FILE, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
            
        print("🚀 [成功] 全球數據倉庫已同步至雲端。")

    except Exception as e:
        print("\n💥 [崩潰診斷] 雲端同步過程中發生嚴重錯誤！")
        print("-" * 50)
        import traceback
        traceback.print_exc() # 這會印出最詳細的報錯行數與原因
        print("-" * 50)

def main():
    init_db()
    
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    
    modules = {
        'tw': downloader_tw.fetch_tw_market_data,
        'us': downloader_us.fetch_us_market_data,
        'cn': downloader_cn.fetch_cn_market_data,
        'hk': downloader_hk.fetch_hk_market_data,
        'jp': downloader_jp.fetch_jp_market_data,
        'kr': downloader_kr.fetch_kr_market_data
    }

    markets_to_run = [target_market] if target_market in modules else modules.keys()

    for m in markets_to_run:
        print(f"\n🌍 市場任務開始: {m.upper()}")
        is_first = check_is_first_time(m)
        df = modules[m](is_first)
        update_database(m, df)
    
    print("\n🏁 所有市場抓取完成，準備進入同步階段...")
    upload_to_drive()
    print("\n✨ 任務結束。")

if __name__ == "__main__":
    main()
