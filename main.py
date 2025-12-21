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
# Google Drive 資料夾 ID
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
# 本地測試用的金鑰路徑 (GitHub Actions 環境會優先讀取 Secrets)
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

def get_db_name(market):
    """根據市場代碼動態生成檔案名稱，實現分國存儲"""
    return f"{market}_stock_warehouse.db"

def init_db(db_file):
    """初始化 SQLite，確保表結構與索引存在"""
    conn = sqlite3.connect(db_file)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
            date TEXT, symbol TEXT, market TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, updated_at TEXT,
            PRIMARY KEY (date, symbol, market))''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_date_market ON stock_prices (date, market)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stock_prices (symbol)')
        conn.commit()
    finally:
        conn.close()

def check_is_first_time(db_file, market):
    """偵測本地資料庫狀態，決定下載長度"""
    if not os.path.exists(db_file): 
        return True
    
    conn = sqlite3.connect(db_file)
    try:
        # 檢查該市場是否已有任何交易紀錄
        cursor = conn.execute("SELECT COUNT(*) FROM stock_prices WHERE market = ?", (market,))
        count = cursor.fetchone()[0]
        return count == 0
    except Exception as e:
        print(f"⚠️ 偵測資料庫時發生錯誤: {e}")
        return True
    finally:
        conn.close() # 已修正：補上括號

def update_database(db_file, market, df):
    """高效能寫入，支援 WAL 模式與大數據緩衝"""
    if df is None or df.empty: 
        return
    
    df['market'] = market
    df['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    conn = sqlite3.connect(db_file)
    try:
        # ⚡ 針對大量數據寫入的 PRAGMA 優化
        conn.execute("PRAGMA journal_mode = WAL;")  
        conn.execute("PRAGMA synchronous = OFF;")   
        conn.execute("PRAGMA cache_size = -1000000;") # 1GB 記憶體快取
        
        df.to_sql('stock_prices', conn, if_exists='append', index=False)
        conn.commit()
        print(f"✅ {market.upper()}: 成功寫入 {len(df)} 筆資料至 {db_file}")
    except Exception as e:
        print(f"⚠️ {market.upper()}: 寫入提醒 (可能是重複日期): {e}")
    finally:
        conn.close()

def upload_to_drive(db_file):
    """雲端同步：自動切換更新(Update)或建立(Create)邏輯"""
    if not os.path.exists(db_file): 
        return

    # 優先從環境變數讀取 GitHub Secrets
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else:
            print(f"⚠️ 找不到金鑰來源，中止 {db_file} 同步。")
            return
            
        service = build('drive', 'v3', credentials=creds)
        file_size_mb = os.path.getsize(db_file) / (1024 * 1024)
        print(f"📦 同步診斷: {db_file} 目前大小 {file_size_mb:.2f} MB")

        # 搜尋雲端是否存在此市場的專屬 DB
        query = f"name = '{db_file}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        files = service.files().list(q=query, fields="files(id)", supportsAllDrives=True).execute().get('files', [])
        
        # 使用可續傳上傳 (Resumable Upload) 應對大檔案
        media = MediaFileUpload(db_file, mimetype='application/x-sqlite3', resumable=True, chunksize=10*1024*1024)
        
        if files:
            file_id = files[0]['id']
            print(f"🔄 更新現有檔案 (ID: {file_id})...")
            service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        else:
            print(f"🆕 建立全新檔案 (注意 Quota 限制)...")
            file_metadata = {'name': db_file, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
            
        print(f"🚀 {db_file} 雲端同步成功")
    except Exception:
        print(f"❌ {db_file} 同步失敗！")
        import traceback
        traceback.print_exc()

def main():
    # 接收 GitHub Actions 傳入的市場參數
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    
    modules = {
        'tw': downloader_tw.fetch_tw_market_data,
        'us': downloader_us.fetch_us_market_data,
        'cn': downloader_cn.fetch_cn_market_data,
        'hk': downloader_hk.fetch_hk_market_data,
        'jp': downloader_jp.fetch_jp_market_data,
        'kr': downloader_kr.fetch_kr_market_data
    }

    # 決定執行的市場清單
    markets_to_run = [target_market] if target_market in modules else modules.keys()

    for m in markets_to_run:
        db_file = get_db_name(m)
        print(f"\n--- 🌍 市場任務啟動: {m.upper()} ---")
        
        # 1. 初始化資料庫
        init_db(db_file)
        
        # 2. 偵測是否為第一次下載 (基於下載下來的種子檔案是否為空)
        is_first = check_is_first_time(db_file, m)
        print(f"ℹ️ 模式偵測: {'首次抓取 (MAX)' if is_first else '增量更新 (7D)'}")
        
        # 3. 執行抓取
        df = modules[m](is_first)
        
        # 4. 寫入並同步
        if df is not None and not df.empty:
            update_database(db_file, m, df)
            upload_to_drive(db_file)
        else:
            print(f"📭 {m.upper()} 無新數據，跳過更新。")
    
    print("\n✨ 任務圓滿結束")

if __name__ == "__main__":
    main()
