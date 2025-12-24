# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, gzip, shutil, socket
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

# 💡 增加全域連線逾時，確保大檔案傳輸穩定
socket.setdefaulttimeout(600)

# 導入通知與環境變數載入工具
from notifier import StockNotifier
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 匯入各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心參數設定 ==========
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'
AUDIT_DB_PATH = "data_warehouse_audit.db"

# 📊 數量門檻預警
EXPECTED_MIN_ROWS = {
    'tw': 900, 'us': 4000, 'cn': 4500, 'hk': 1500, 'jp': 3000, 'kr': 2000
}

notifier = StockNotifier()

def get_drive_service():
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    if env_json:
        info = json.loads(env_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
    elif os.path.exists(SERVICE_ACCOUNT_FILE):
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
    else:
        return None
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

# ========== 雲端與磁碟維護函式 ==========

def download_backup_from_drive(service, file_name):
    """從雲端下載 .db.gz 檔案"""
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute(num_retries=3)
    items = results.get('files', [])
    
    if not items:
        return False

    file_id = items[0]['id']
    print(f"📡 發現雲端備份: {file_name}, 正在下載...")
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_name, 'wb')
    downloader = MediaIoBaseDownload(fh, request, chunksize=10*1024*1024)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk(num_retries=5)
        if status:
            print(f"📥 下載進度: {int(status.progress() * 100)}%")
    return True

def decompress_db(gz_file):
    """解壓縮並移除壓縮檔以節省空間"""
    db_file = gz_file.replace('.gz', '')
    try:
        print(f"🔓 正在解壓 {gz_file}...")
        with gzip.open(gz_file, 'rb') as f_in:
            with open(db_file, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz_file) # 💡 關鍵：立即刪除壓縮檔釋放空間
        return True
    except Exception as e:
        print(f"❌ 解壓失敗: {e}")
        return False

def optimize_and_compress(db_file):
    """優化 SQLite 並壓縮，隨後移除原始檔"""
    gz_file = f"{db_file}.gz"
    try:
        print(f"🧹 執行 VACUUM 優化 {db_file}...")
        conn = sqlite3.connect(db_file)
        conn.execute("VACUUM")
        conn.close()
        
        print(f"📦 正在壓縮為 {gz_file}...")
        with open(db_file, 'rb') as f_in:
            with gzip.open(gz_file, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # 💡 關鍵：壓縮成功後立即刪除原始 .db 檔案，為上傳流程騰出空間
        os.remove(db_file) 
        return gz_file
    except Exception as e:
        print(f"❌ 壓縮失敗: {e}")
        return None

def upload_to_drive(service, file_path):
    """上傳 .db.gz 到雲端"""
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='application/octet-stream', resumable=True, chunksize=10*1024*1024)
    
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    results = service.files().list(q=query, fields="files(id)").execute(num_retries=3)
    items = results.get('files', [])
    
    if items:
        request = service.files().update(fileId=items[0]['id'], media_body=media, supportsAllDrives=True)
    else:
        file_metadata = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
        request = service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True)

    response = None
    while response is None:
        status, response = request.next_chunk(num_retries=5)
        if status:
            print(f"📤 上傳進度: {int(status.progress() * 100)}%")
    return True

# ========== 主程式邏輯 ==========

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    markets_to_run = [target_market] if target_market in module_map else module_map.keys()

    service = get_drive_service()
    if not service:
        print("❌ 無法啟動 Google Drive 服務")
        return

    for m in markets_to_run:
        try:
            db_file = f"{m}_stock_warehouse.db"
            gz_file = f"{db_file}.gz"
            print(f"\n--- 🌍 市場任務啟動: {m.upper()} ---")

            # 1. 嘗試恢復備份
            if not os.path.exists(db_file):
                if download_backup_from_drive(service, gz_file):
                    decompress_db(gz_file)
                else:
                    print(f"🆕 雲端無備份，建立全新資料庫...")
                    conn = sqlite3.connect(db_file)
                    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                        date TEXT, symbol TEXT, market TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, updated_at TEXT,
                        PRIMARY KEY (date, symbol, market))''')
                    conn.close()

            # 2. 執行增量下載
            target_module = module_map.get(m)
            stats = target_module.main() 
            
            # 3. 處理完成後的封裝與上傳
            success_count = stats.get('success', 0)
            if success_count > 0:
                final_gz = optimize_and_compress(db_file)
                if final_gz:
                    upload_to_drive(service, final_gz)
                    os.remove(final_gz) # 💡 最終清理
                
                health_note = "✅ 數據完整度良好。"
                if m in EXPECTED_MIN_ROWS and success_count < EXPECTED_MIN_ROWS[m]:
                    health_note = f"⚠️ <b>[警告]</b> 數量 ({success_count}) 低於門檻!"
                
                notifier.send_stock_report(m.upper(), None, pd.DataFrame(), health_note, stats)
            else:
                notifier.send_telegram(f"❌ {m.upper()} 今日無更新。")
                if os.path.exists(db_file): os.remove(db_file)

        except Exception as e:
            notifier.send_telegram(f"❌ {m.upper()} 崩潰: {str(e)}")
            if os.path.exists(db_file): os.remove(db_file)
    
    print("\n✨ 任務結束")

if __name__ == "__main__":
    main()
