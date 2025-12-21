# -*- coding: utf-8 -*-
import os, sys, sqlite3, json
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 匯入各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心設定 ==========
DB_FILE = 'global_stock_warehouse.db'
# 本地金鑰檔案路徑 (若環境變數不存在時會使用)
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'
# Google Drive 資料夾 ID
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 

def init_db():
    """初始化資料庫與索引"""
    conn = sqlite3.connect(DB_FILE)
    # 建立主表：PRIMARY KEY 確保資料不重複 (日期+代號+市場)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
        date TEXT, symbol TEXT, market TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, updated_at TEXT,
        PRIMARY KEY (date, symbol, market))''')
    # 建立索引：大幅提升「千日新高」回測查詢速度
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date_market ON stock_prices (date, market)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stock_prices (symbol)')
    conn.close()

def check_is_first_time(market):
    """偵測資料庫中該市場是否已有資料"""
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
    """將下載的資料寫入 SQLite"""
    if df is None or df.empty: return
    df['market'] = market
    df['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_FILE)
    try:
        # if_exists='append' 搭配 PRIMARY KEY 會自動處理重複資料
        df.to_sql('stock_prices', conn, if_exists='append', index=False)
        print(f"✅ {market.upper()}: 資料庫寫入成功")
    except Exception:
        print(f"⚠️ {market.upper()}: 部分重複日期資料已跳過")
    finally:
        conn.close()

def upload_to_drive():
    """雲端同步邏輯：優先讀取環境變數，次之讀取本地檔案"""
    if not os.path.exists(DB_FILE):
        print("❌ 找不到資料庫檔案，停止上傳")
        return

    # 1. 嘗試從環境變數讀取 JSON (GitHub Actions 模式)
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    
    try:
        if env_json:
            print("☁️ 偵測到環境變數，使用 GitHub Secrets 金鑰...")
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            print("💻 偵測到本地檔案，使用 JSON 金鑰檔案...")
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else:
            print("⚠️ 找不到任何金鑰來源 (環境變數或 JSON 檔案)，跳過雲端同步")
            return
            
        service = build('drive', 'v3', credentials=creds)
        
        # 2. 檢查雲端是否已存在檔案
        query = f"name = '{DB_FILE}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        files = service.files().list(q=query, fields="files(id)", supportsAllDrives=True).execute().get('files', [])
        
        media = MediaFileUpload(DB_FILE, mimetype='application/x-sqlite3', resumable=True)
        
        if files:
            # 更新現有檔案
            file_id = files[0]['id']
            service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
            print(f"🚀 雲端更新成功 (ID: {file_id})")
        else:
            # 建立新檔案
            file_metadata = {'name': DB_FILE, 'parents': [GDRIVE_FOLDER_ID]}
            new_file = service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
            print(f"🚀 雲端建立成功 (ID: {new_file.get('id')})")
            
    except Exception as e:
        print(f"❌ 雲端同步失敗: {e}")

def main():
    """主執行程序"""
    init_db()
    
    # 支援指令參數，如: python main.py tw
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    
    modules = {
        'tw': downloader_tw.fetch_tw_market_data,
        'us': downloader_us.fetch_us_market_data,
        'cn': downloader_cn.fetch_cn_market_data,
        'hk': downloader_hk.fetch_hk_market_data,
        'jp': downloader_jp.fetch_jp_market_data,
        'kr': downloader_kr.fetch_kr_market_data
    }

    # 決定要執行的市場清單
    markets_to_run = [target_market] if target_market in modules else modules.keys()

    for m in markets_to_run:
        print(f"\n--- 正在處理市場: {m.upper()} ---")
        is_first = check_is_first_time(m)
        # 呼叫對應模組，傳入 is_first 決定 period (max/10y 或 7d)
        df = modules[m](is_first)
        update_database(m, df)
    
    # 全部執行完後再上傳雲端
    upload_to_drive()
    print("\n✨ 全球數據倉庫任務執行完畢")

if __name__ == "__main__":
    main()