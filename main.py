# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time
import pandas as pd
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 導入通知與環境變數載入工具
from notifier import StockNotifier
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 匯入各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心設定 ==========
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'
AUDIT_DB_PATH = "data_warehouse_audit.db"

# 初始化通知器
notifier = StockNotifier()

def get_db_name(market):
    return f"{market}_stock_warehouse.db"

def init_db(db_file):
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

def record_audit_log(market_id, stats):
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS sync_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execution_time TEXT,
            market_id TEXT,
            total_count INTEGER,
            success_count INTEGER,
            fail_count INTEGER,
            success_rate REAL
        )''')
        total = stats.get('total', 0)
        success = stats.get('success', 0)
        fail = stats.get('fail', 0)
        rate = round((success / total * 100), 2) if total > 0 else 0
        now_ts = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        conn.execute('''INSERT INTO sync_audit (execution_time, market_id, total_count, success_count, fail_count, success_rate)
                        VALUES (?, ?, ?, ?, ?, ?)''', (now_ts, market_id, total, success, fail, rate))
        conn.commit()
    except Exception as e:
        print(f"⚠️ Audit Log 記錄失敗: {e}")
    finally:
        conn.close()

def upload_to_drive(db_file):
    if not os.path.exists(db_file): return False
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else: return False
        service = build('drive', 'v3', credentials=creds)
        media = MediaFileUpload(db_file, mimetype='application/x-sqlite3', resumable=True)
        query = f"name = '{db_file}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
        files = service.files().list(q=query, fields="files(id)").execute().get('files', [])
        if files:
            service.files().update(fileId=files[0]['id'], media_body=media, supportsAllDrives=True).execute()
        else:
            file_metadata = {'name': db_file, 'parents': [GDRIVE_FOLDER_ID]}
            service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()
        return True
    except: return False

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    
    # 💡 修改點：僅存儲模組物件，不要在此時呼叫 .main
    module_map = {
        'tw': downloader_tw,
        'us': downloader_us,
        'cn': downloader_cn,
        'hk': downloader_hk,
        'jp': downloader_jp,
        'kr': downloader_kr
    }

    markets_to_run = [target_market] if target_market in module_map else module_map.keys()

    for m in markets_to_run:
        try:
            db_file = get_db_name(m)
            print(f"\n--- 🌍 市場任務啟動: {m.upper()} ---")
            
            # 💡 修改點：動態檢查該模組是否有 main 函式
            target_module = module_map.get(m)
            if not hasattr(target_module, 'main'):
                err = f"❌ 錯誤: {m.upper()} 的下載器檔案 (downloader_{m}.py) 缺少 main() 函式，請更新該檔案內容。"
                print(err)
                notifier.send_telegram(err)
                continue

            init_db(db_file)
            
            # 執行抓取
            stats = target_module.main() 
            
            if stats and stats.get('success', 0) > 0:
                upload_to_drive(db_file)
                notifier.send_stock_report(market_name=m.upper(), img_data=None, report_df=pd.DataFrame(), text_reports="", stats=stats)
                record_audit_log(m, stats)
            else:
                msg = f"❌ {m.upper()} 抓取結果為空。"
                print(msg)
                notifier.send_telegram(msg)
        
        except Exception as e:
            err_detail = f"❌ {m.upper()} 執行異常: {str(e)}"
            print(err_detail)
            notifier.send_telegram(err_detail)
    
    print("\n✨ 任務圓滿結束")

if __name__ == "__main__":
    main()
