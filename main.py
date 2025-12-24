# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# 💡 確保傳輸穩定
socket.setdefaulttimeout(600)
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except ImportError:
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# 📊 應收標的門檻 (這是計算覆蓋率的基準)
EXPECTED_MIN_STOCKS = {
    'tw': 900, 'us': 4000, 'cn': 5496, 'hk': 1500, 'jp': 3000, 'kr': 2000
}

# ========== 1. Google Drive 傳輸核心 (無壓縮版) ==========

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
        print(f"❌ 無法初始化 Drive: {e}")
        return None

def download_db_from_drive(service, file_name, retries=3):
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            items = results.get('files', [])
            if not items: return False
            file_id = items[0]['id']
            print(f"📡 下載雲端數據 ({attempt+1}/{retries}): {file_name}")
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO(file_name, 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            return True
        except Exception as e:
            print(f"⚠️ 下載失敗: {e}")
            time.sleep(5)
    return False

def upload_db_to_drive(service, file_path, retries=3):
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='application/x-sqlite3', resumable=True)
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            items = results.get('files', [])
            if items:
                service.files().update(fileId=items[0]['id'], media_body=media, supportsAllDrives=True).execute()
            else:
                meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
                service.files().create(body=meta, media_body=media, supportsAllDrives=True).execute()
            print(f"✅ 上傳完成: {file_name}")
            return True
        except Exception as e:
            print(f"⚠️ 上傳失敗: {e}")
            time.sleep(5)
    return False

# ========== 2. 數據維護與精確統計 ==========

def optimize_db(db_file):
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("VACUUM")
        conn.close()
    except: pass

def get_db_summary(db_path, market_id):
    """計算包含 覆蓋率、總筆數、名稱同步 的完整統計"""
    try:
        conn = sqlite3.connect(db_path)
        # 統計日K
        df_stats = pd.read_sql("SELECT COUNT(DISTINCT symbol) as s, MAX(date) as d2, COUNT(*) as t FROM stock_prices", conn)
        # 統計名稱
        info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        conn.close()

        success_count = int(df_stats['s'][0])
        expected = EXPECTED_MIN_STOCKS.get(market_id, 1)
        coverage = (success_count / expected) * 100

        return {
            "market": market_id.upper(),
            "expected": expected,
            "success": success_count,
            "coverage": f"{coverage:.1f}%",
            "end_date": df_stats['d2'][0],
            "total_rows": df_stats['t'][0],
            "names_synced": info_count,
            "status": "✅" if coverage >= 90 else "⚠️"
        }
    except Exception as e:
        print(f"⚠️ 統計出錯: {e}")
        return None

# ========== 3. 主執行流程 ==========

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    markets_to_run = [target_market] if target_market in module_map else module_map.keys()

    service = get_drive_service()
    if not service: return

    all_summaries = []

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        print(f"\n--- 🚀 市場啟動: {m.upper()} ---")

        if not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        # 下載新數據 (熱更新模式)
        target_module = module_map.get(m)
        target_module.run_sync(mode='hot') 

        # 收集統計數據
        summary = get_db_summary(db_file, m)
        if summary:
            all_summaries.append(summary)

        # 優化並回傳雲端
        optimize_db(db_file)
        upload_db_to_drive(service, db_file)

    # 彙整完所有市場後一次性發送 Email
    if notifier and all_summaries:
        notifier.send_stock_report_email(all_summaries)

if __name__ == "__main__":
    main()
