# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from dotenv import load_dotenv

# 💡 1. 環境設定
load_dotenv() 
socket.setdefaulttimeout(600)

# 強制日期設定 (根據你的要求鎖定年份)
FORCE_START_DATE = "2024-01-01"
FORCE_END_DATE = "2025-12-31"

GDRIVE_FOLDER_ID = os.environ.get('GDRIVE_FOLDER_ID')
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 💡 2. 導入特徵加工模組
try:
    from processor import process_market_data
except ImportError:
    print("⚠️ 系統提示：找不到 processor.py")
    process_market_data = None

# 💡 3. 導入通知模組 (保留 notifier 功能)
try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except Exception as e:
    print(f"❌ Notifier 初始化失敗: {e}")
    notifier = None

import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# 📊 門檻門檻設定
EXPECTED_MIN_STOCKS = {
    'tw': 2500, 'us': 5684, 'cn': 5496, 'hk': 2689, 'jp': 4315, 'kr': 2000
}

# ========== [Google Drive 相關函式 - 原封不動保留] ==========

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
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)").execute()
            items = results.get('files', [])
            if not items: return False
            file_id = items[0]['id']
            print(f"📡 正在從雲端下載數據庫: {file_name}")
            request = service.files().get_media(fileId=file_id)
            with io.FileIO(file_name, 'wb') as fh:
                downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
            return True
        except Exception as e:
            print(f"⚠️ 下載失敗 ({attempt+1}/3): {e}")
            time.sleep(5)
    return False

def upload_db_to_drive(service, file_path, retries=3):
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype='application/x-sqlite3', resumable=True)
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)").execute()
            items = results.get('files', [])
            if items:
                service.files().update(fileId=items[0]['id'], media_body=media).execute()
            else:
                meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
                service.files().create(body=meta, media_body=media).execute()
            print(f"✅ 上傳成功: {file_name} ({os.path.getsize(file_path)/1024/1024:.2f} MB)")
            return True
        except Exception as e:
            print(f"⚠️ 上傳失敗 ({attempt+1}/3): {e}")
            time.sleep(5)
    return False

def check_needs_update(db_path):
    if not os.path.exists(db_path): return True
    try:
        conn = sqlite3.connect(db_path)
        res = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()
        conn.close()
        db_latest_date = res[0] if res and res[0] else None
        if not db_latest_date: return True
        # 只要最新日期小於當前日期，就更新（但在 run_sync 會被限制在 2025）
        today_str = datetime.now().strftime('%Y-%m-%d')
        return db_latest_date < today_str
    except: return True

def get_db_summary(db_path, market_id, fail_list=None):
    if not os.path.exists(db_path): return None
    try:
        conn = sqlite3.connect(db_path)
        df_stats = pd.read_sql("SELECT COUNT(DISTINCT symbol) as s, MAX(date) as d2, COUNT(*) as t FROM stock_prices", conn)
        info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        conn.close()

        success_count = int(df_stats['s'][0]) if df_stats['s'][0] else 0
        latest_date = df_stats['d2'][0] if df_stats['d2'][0] else "N/A"
        total_rows = int(df_stats['t'][0]) if df_stats['t'][0] else 0
        
        expected = EXPECTED_MIN_STOCKS.get(market_id, 1)
        coverage = (success_count / expected) * 100
        
        return {
            "market": market_id.upper(), "expected": expected, "success": success_count,
            "coverage": f"{coverage:.1f}%", "end_date": latest_date, "total_rows": total_rows,
            "names_synced": info_count, "fail_list": fail_list if fail_list else [],
            "status": "✅" if coverage >= 80 else "⚠️"
        }
    except Exception as e:
        print(f"⚠️ {market_id.upper()} 摘要失敗: {e}")
        return None

# ========== [主流程] ==========

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    
    markets_to_run = [target_market] if target_market in module_map else list(module_map.keys())
    service = get_drive_service()
    all_summaries = []

    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        print(f"\n--- 🌍 市場啟動: {m.upper()} ---")

        if service and not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        needs_update = check_needs_update(db_file)
        execution_results = {"has_changed": False, "fail_list": []}
        
        if needs_update:
            target_module = module_map.get(m)
            # 💡 關鍵修正：傳送強制日期給下載模組
            print(f"📡 執行同步: 範圍設定為 {FORCE_START_DATE} ~ {FORCE_END_DATE}")
            execution_results = target_module.run_sync(
                start_date=FORCE_START_DATE, 
                end_date=FORCE_END_DATE,
                max_workers=8
            ) 
        
        # 執行特徵加工
        if process_market_data and os.path.exists(db_file):
            print(f"🧪 執行特徵工程加工...")
            try:
                process_market_data(db_file)
            except Exception as e:
                print(f"❌ 特徵加工出錯: {e}")

        has_changed = execution_results.get('has_changed', False) if isinstance(execution_results, dict) else False
        current_fails = execution_results.get('fail_list', []) if isinstance(execution_results, dict) else []
        
        summary = get_db_summary(db_file, m, fail_list=current_fails)
        if summary:
            all_summaries.append(summary)

        # 只要有執行過更新或資料變動就同步雲端
        if service and (has_changed or needs_update):
            print(f"🔄 執行雲端同步中...")
            try:
                conn = sqlite3.connect(db_file)
                conn.execute("VACUUM")
                conn.close()
                upload_db_to_drive(service, db_file)
            except Exception as e:
                print(f"❌ 優化或上傳失敗: {e}")

    # 💡 保留 Notifier 發送功能
    if notifier and all_summaries:
        print("📨 發送報告中...")
        notifier.send_stock_report_email(all_summaries)

if __name__ == "__main__":
    main()
