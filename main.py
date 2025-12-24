# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, gzip, shutil, socket
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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

EXPECTED_MIN_ROWS = {
    'tw': 900, 'us': 4000, 'cn': 4500, 'hk': 1500, 'jp': 3000, 'kr': 2000
}

notifier = StockNotifier()

def get_db_name(market):
    return f"{market}_stock_warehouse.db"

def init_db(db_file):
    """初始化數據存儲結構"""
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

def optimize_and_compress(db_file):
    """
    1. 執行 VACUUM 優化 SQLite 空間
    2. 壓縮成 .gz 檔案
    """
    gz_file = f"{db_file}.gz"
    print(f"🧹 正在優化資料庫: {db_file}...")
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("VACUUM")
        conn.close()
        
        print(f"📦 正在壓縮檔案為 {gz_file}...")
        with open(db_file, 'rb') as f_in:
            with gzip.open(gz_file, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        orig_size = os.path.getsize(db_file) / (1024**2)
        gz_size = os.path.getsize(gz_file) / (1024**2)
        print(f"✅ 壓縮完成! {orig_size:.1f}MB -> {gz_size:.1f}MB")
        return gz_file
    except Exception as e:
        print(f"❌ 優化或壓縮失敗: {e}")
        return None

def record_audit_log(market_id, stats, task_type="DOWNLOAD"):
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS sync_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execution_time TEXT, market_id TEXT, task_type TEXT,
            total_count INTEGER, success_count INTEGER, fail_count INTEGER, success_rate REAL
        )''')
        total = stats.get('total', 0)
        success = stats.get('success', 0)
        fail = stats.get('fail', 0)
        rate = round((success / total * 100), 2) if total > 0 else 0
        now_ts = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute('INSERT INTO sync_audit (execution_time, market_id, task_type, total_count, success_count, fail_count, success_rate) VALUES (?,?,?,?,?,?,?)',
                     (now_ts, market_id, task_type, total, success, fail, rate))
        conn.commit()
    except Exception as e:
        print(f"📋 Audit Log 失敗: {e}")
    finally:
        conn.close()

def upload_to_drive(file_path):
    """同步檔案(支援 .gz) 至 Google Drive，具備分段上傳與重試機制"""
    if not os.path.exists(file_path): return False
    file_name = os.path.basename(file_path)
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else: return False
        
        service = build('drive', 'v3', credentials=creds, cache_discovery=False)
        # 💡 使用更大的 chunksize (10MB) 處理大檔案上傳
        media = MediaFileUpload(file_path, mimetype='application/octet-stream', resumable=True, chunksize=10*1024*1024)
        
        query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        files = service.files().list(q=query, fields="files(id)").execute(num_retries=3)
        files_list = files.get('files', [])
        
        if files_list:
            file_id = files_list[0]['id']
            print(f"🔄 正在更新雲端檔案: {file_name}")
            request = service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True)
        else:
            print(f"🆕 正在建立新雲端檔案: {file_name}")
            file_metadata = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
            request = service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True)

        response = None
        while response is None:
            status, response = request.next_chunk(num_retries=5)
            if status:
                print(f"📤 上傳進度: {int(status.progress() * 100)}%")
        
        return True
    except Exception as e:
        print(f"❌ 上傳失敗: {e}")
        return False

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    markets_to_run = [target_market] if target_market in module_map else module_map.keys()

    for m in markets_to_run:
        try:
            db_file = get_db_name(m)
            print(f"\n--- 🌍 市場任務啟動: {m.upper()} ---")
            target_module = module_map.get(m)
            if not hasattr(target_module, 'main'): continue

            init_db(db_file)
            
            # 1. 執行下載
            stats = target_module.main() 
            
            # 2. 資料完整性檢查
            success_count = stats.get('success', 0)
            health_note = "✅ 數據完整度正常。"
            if m in EXPECTED_MIN_ROWS and success_count < EXPECTED_MIN_ROWS[m]:
                health_note = f"⚠️ <b>[資料異常預警]</b> 更新家數 ({success_count}) 低於門檻!"

            # 3. 壓縮與雲端同步
            if success_count > 0:
                # 💡 重點：先壓縮再上傳
                gz_file = optimize_and_compress(db_file)
                if gz_file:
                    upload_to_drive(gz_file)
                    # 結束後移除 gz 節省本地空間 (可選)
                    # os.remove(gz_file) 
                
                notifier.send_stock_report(
                    market_name=m.upper(), 
                    img_data=None, 
                    report_df=pd.DataFrame(), 
                    text_reports=health_note, 
                    stats=stats
                )
                record_audit_log(m, stats, task_type="DOWNLOAD")
            else:
                notifier.send_telegram(f"❌ {m.upper()} 下載完全失敗。")
        
        except Exception as e:
            notifier.send_telegram(f"❌ {m.upper()} 系統崩潰: {str(e)}")
    
    print("\n✨ 任務圓滿結束")

if __name__ == "__main__":
    main()
