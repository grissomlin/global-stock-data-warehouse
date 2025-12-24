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

# 📊 [資料完整性] 各市場每日更新數量預期門檻 (低於此數將發送警報)
EXPECTED_MIN_ROWS = {
    'tw': 900,    # 台灣上市櫃約 1800+，設定 900 為基本門檻
    'us': 4000,   # 美國普通股約 5000-8000
    'cn': 4500,   # 中國 A 股約 5000+
    'hk': 1500,   # 香港普通股約 2500+
    'jp': 3000,   # 日本東證約 3800+
    'kr': 2000    # 韓國 KOSPI/KOSDAQ 約 2500+
}

# 初始化通知器
notifier = StockNotifier()

def get_db_name(market):
    return f"{market}_stock_warehouse.db"

def init_db(db_file):
    """初始化 SQLite 數據表與索引"""
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

def record_audit_log(market_id, stats, task_type="DOWNLOAD"):
    """
    紀錄審計日誌：支援下載 (DOWNLOAD) 與 轉換 (CONVERSION) 兩種類型
    """
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS sync_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execution_time TEXT,
            market_id TEXT,
            task_type TEXT,
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
        
        conn.execute('''INSERT INTO sync_audit 
            (execution_time, market_id, task_type, total_count, success_count, fail_count, success_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', 
            (now_ts, market_id, task_type, total, success, fail, rate))
        conn.commit()
    except Exception as e:
        print(f"📋 Audit Log 寫入失敗: {e}")
    finally:
        conn.close()

def upload_to_drive(db_file):
    """雲端同步邏輯 (Resumable Upload)"""
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
            if not hasattr(target_module, 'main'):
                print(f"❌ {m.upper()} 缺少 main() 進入點")
                continue

            init_db(db_file)
            
            # 1. 執行抓取並取得統計
            stats = target_module.main() 
            
            # 2. 資料完整性驗證 (Threshold Check)
            success_count = stats.get('success', 0)
            if m in EXPECTED_MIN_ROWS and success_count < EXPECTED_MIN_ROWS[m]:
                alert_msg = f"⚠️ <b>{m.upper()} 資料完整性警告</b>\n更新數量 ({success_count}) 低於門檻 ({EXPECTED_MIN_ROWS[m]})，請檢查數據源或連線狀態。"
                notifier.send_telegram(alert_msg)

            # 3. 雲端同步與報表發送
            if success_count > 0:
                upload_to_drive(db_file)
                notifier.send_stock_report(market_name=m.upper(), img_data=None, report_df=pd.DataFrame(), text_reports="", stats=stats)
                record_audit_log(m, stats, task_type="DOWNLOAD")
            else:
                notifier.send_telegram(f"❌ {m.upper()} 今日抓取失敗，無數據可更新。")
        
        except Exception as e:
            err_msg = f"❌ {m.upper()} 系統崩潰: {str(e)}"
            print(err_msg)
            notifier.send_telegram(err_msg)
    
    print("\n✨ 任務完成")

if __name__ == "__main__":
    main()
