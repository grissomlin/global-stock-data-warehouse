# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# 💡 增加連線逾時，確保大檔案傳輸穩定
socket.setdefaulttimeout(600)
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except ImportError:
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

EXPECTED_MIN_STOCKS = {
    'tw': 900, 'us': 4000, 'cn': 4500, 'hk': 1500, 'jp': 3000, 'kr': 2000
}

# ========== 1. 強韌的 Google Drive 函式 (加入重試機制) ==========

def get_drive_service():
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else:
            return None
        return build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ 無法初始化 Drive 服務: {e}")
        return None

def download_db_from_drive(service, file_name, retries=3):
    """直接下載 .db 檔案，失敗會自動重試"""
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            items = results.get('files', [])
            if not items:
                print(f"ℹ️ 雲端尚無備份檔: {file_name}")
                return False

            file_id = items[0]['id']
            print(f"📡 正在下載雲端數據 ({attempt+1}/{retries}): {file_name}...")
            
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO(file_name, 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            print(f"✅ 下載完成: {file_name}")
            return True
        except Exception as e:
            print(f"⚠️ 下載嘗試 {attempt+1} 失敗: {e}")
            time.sleep(5)
    return False

def upload_db_to_drive(service, file_path, retries=3):
    """直接上傳 .db 檔案，並覆蓋舊版"""
    file_name = os.path.basename(file_path)
    # 使用 resumable=True 處理較大檔案
    media = MediaFileUpload(file_path, mimetype='application/x-sqlite3', resumable=True)
    
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            items = results.get('files', [])

            if items:
                file_id = items[0]['id']
                print(f"🔄 正在更新雲端檔案 ({attempt+1}/{retries}): {file_name}")
                service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
            else:
                print(f"🆕 正在建立雲端檔案 ({attempt+1}/{retries}): {file_name}")
                file_metadata = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
                service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
            print(f"✅ 上傳完成: {file_name}")
            return True
        except Exception as e:
            print(f"⚠️ 上傳嘗試 {attempt+1} 失敗: {e}")
            time.sleep(5)
    return False

# ========== 2. 數據倉庫維護與統計 ==========

def optimize_db(db_file):
    """僅執行 VACUUM，不壓縮"""
    try:
        print(f"🧹 正在優化資料庫結構: {db_file}")
        conn = sqlite3.connect(db_file)
        conn.execute("VACUUM")
        conn.close()
    except Exception as e:
        print(f"⚠️ 優化失敗: {e}")

def get_db_summary(db_path):
    try:
        conn = sqlite3.connect(db_path)
        df_stats = pd.read_sql("SELECT COUNT(DISTINCT symbol) as s, MAX(date) as d2, COUNT(*) as t FROM stock_prices", conn)
        info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        conn.close()
        return {
            "stocks": df_stats['s'][0], "end": df_stats['d2'][0],
            "total": df_stats['t'][0], "names": info_count, "file": os.path.basename(db_path)
        }
    except: return None

# ========== 3. 主程式執行邏輯 ==========

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    markets_to_run = [target_market] if target_market in module_map else module_map.keys()

    service = get_drive_service()
    if not service: return

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        print(f"\n--- 🚀 [Warehouse] 市場啟動: {m.upper()} ---")

        # A. 直接下載 .db (無解壓)
        if not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        # B. 下載新數據
        target_module = module_map.get(m)
        target_module.run_sync(mode='hot') 

        # C. 數據統計
        summary = get_db_summary(db_file)
        if summary and notifier:
            health = "✅" if summary['stocks'] >= EXPECTED_MIN_STOCKS.get(m, 0) else "⚠️"
            msg = (f"📈 <b>{m.upper()} 倉庫監控</b>\n"
                   f"狀態: {health} | 最新日期: {summary['end']}\n"
                   f"股票數: {summary['stocks']} | 總筆數: {summary['total']}\n"
                   f"名稱同步: {summary['names']}")
            notifier.send_telegram(msg)

        # D. 優化並直接上傳 (無壓縮)
        optimize_db(db_file)
        upload_db_to_drive(service, db_file)
        
        # 💡 如果是在 GitHub Actions 跑，可以考慮刪除本地 db 釋放空間，但 db 很小不刪也行
        # os.remove(db_file)

    print("\n✨ 全球數據倉庫同步任務圓滿結束")

if __name__ == "__main__":
    main()
