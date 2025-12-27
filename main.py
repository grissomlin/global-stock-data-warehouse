# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from dotenv import load_dotenv

# 💡 核心修正：在本機跑時，必須手動載入 .env 檔案
load_dotenv() 

# 💡 全域逾時設定
socket.setdefaulttimeout(600)
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 初始化通知模組
try:
    from notifier import StockNotifier
    notifier = StockNotifier()
    if not os.getenv("TELEGRAM_BOT_TOKEN"):
        print("⚠️ 警告：環境變數 TELEGRAM_BOT_TOKEN 為空，通知功能將受限。")
except Exception as e:
    print(f"❌ Notifier 初始化失敗: {e}")
    notifier = None

# 匯入各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# 📊 應收標的門檻
EXPECTED_MIN_STOCKS = {
    'tw': 900, 'us': 5684, 'cn': 5496, 'hk': 2689, 'jp': 4315, 'kr': 2000
}

# [Google Drive 相關函式保持不變]
def get_drive_service():
    env_json = os.environ.get('GDRIVE_SERVICE_ACCOUNT')
    try:
        if env_json:
            info = json.loads(env_json)
            creds = service_account.Credentials.from_service_account_info(info, scopes=['https://www.googleapis.com/auth/drive'])
        elif os.path.exists(SERVICE_ACCOUNT_FILE):
            creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=['https://www.googleapis.com/auth/drive'])
        else:
            print("❌ 找不到 Google Drive 憑證金鑰")
            return None
        return build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"❌ 無法初始化 Drive 服務: {e}")
        return None

def download_db_from_drive(service, file_name, retries=3):
    query = f"name = '{file_name}' and '{GDRIVE_FOLDER_ID}' in parents and trashed = false"
    for attempt in range(retries):
        try:
            results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            items = results.get('files', [])
            if not items: return False
            file_id = items[0]['id']
            print(f"📡 正在從雲端下載數據庫: {file_name}")
            request = service.files().get_media(fileId=file_id)
            fh = io.FileIO(file_name, 'wb')
            downloader = MediaIoBaseDownload(fh, request, chunksize=5*1024*1024)
            done = False
            while not done:
                status, done = downloader.next_chunk()
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
            results = service.files().list(q=query, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            items = results.get('files', [])
            if items:
                service.files().update(fileId=items[0]['id'], media_body=media, supportsAllDrives=True).execute()
            else:
                meta = {'name': file_name, 'parents': [GDRIVE_FOLDER_ID]}
                service.files().create(body=meta, media_body=media, supportsAllDrives=True).execute()
            print(f"✅ 上傳成功: {file_name}")
            return True
        except Exception as e:
            print(f"⚠️ 上傳失敗 ({attempt+1}/3): {e}")
            time.sleep(5)
    return False

# 💡 新增：檢查資料庫最新日期的功能
def check_needs_update(db_path, market_id):
    """檢查資料庫最新交易日，判斷是否需要更新"""
    if not os.path.exists(db_path):
        print(f"🆕 {market_id.upper()} 資料庫檔案不存在，準備全新同步。")
        return True
    
    try:
        conn = sqlite3.connect(db_path)
        # 抓取原始價格表中的最新日期
        res = conn.execute("SELECT MAX(date) FROM stock_prices").fetchone()
        conn.close()
        
        db_latest_date = res[0] if res and res[0] else None
        if not db_latest_date:
            print(f"⚠️ {market_id.upper()} 資料庫內無價格數據，需要更新。")
            return True
            
        # 取得今天日期
        today_str = datetime.now().strftime('%Y-%m-%d')
        print(f"📅 [{market_id.upper()}] 資料庫最新紀錄: {db_latest_date} | 執行當前日期: {today_str}")
        
        # 如果最新日期已經等於或大於今天，就跳過 (例如週六跑發現週五已更新過)
        if db_latest_date >= today_str:
            print(f"✅ {market_id.upper()} 數據已是最新，略過下載流程。")
            return False
            
        return True
    except Exception as e:
        print(f"⚠️ 檢查日期時出錯: {e}，預設執行更新。")
        return True

def get_db_summary(db_path, market_id, fail_list=None):
    if not os.path.exists(db_path):
        return None
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
            "market": market_id.upper(),
            "expected": expected,
            "success": success_count,
            "coverage": f"{coverage:.1f}%",
            "end_date": latest_date,
            "total_rows": total_rows,
            "names_synced": info_count,
            "fail_list": fail_list if fail_list else [],
            "status": "✅" if coverage >= 90 else "⚠️"
        }
    except Exception as e:
        print(f"⚠️ {market_id.upper()} 摘要撈取失敗: {e}")
        return None

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

        # 1. 下載雲端備份
        if service and not os.path.exists(db_file):
            download_db_from_drive(service, db_file)

        # 💡 核心修正：判斷是否需要執行 run_sync
        needs_update = check_needs_update(db_file, m)
        
        execution_results = {"has_changed": False, "fail_list": []}
        
        if needs_update:
            target_module = module_map.get(m)
            execution_results = target_module.run_sync(mode='hot') 
        
        # 2. 獲取摘要（不論有無更新都讀取目前資料庫狀態）
        current_fails = execution_results.get('fail_list', []) if isinstance(execution_results, dict) else []
        has_changed = execution_results.get('has_changed', False) if isinstance(execution_results, dict) else False
        
        summary = get_db_summary(db_file, m, fail_list=current_fails)
        if summary:
            all_summaries.append(summary)
            print(f"📊 摘要已生成: {m.upper()} (最新日期: {summary['end_date']} | 覆蓋率: {summary['coverage']})")

        # 3. 只有真的有變動才上傳雲端
        if service and has_changed:
            print(f"🔄 偵測到數據變動，正在優化並同步至雲端...")
            conn = sqlite3.connect(db_file)
            conn.execute("VACUUM")
            conn.close()
            upload_db_to_drive(service, db_file)
        else:
            print(f"⏭️ {m.upper()} 無變動或略過更新，跳過雲端上傳。")

    print(f"\n🏁 任務全部結束。收集到摘要: {len(all_summaries)} 份")
    
    if notifier is not None:
        if len(all_summaries) > 0:
            print("📨 正在發送監控報告 (Email & Telegram)...")
            success = notifier.send_stock_report_email(all_summaries)
            if success:
                print("✨ 通報成功發送。")
            else:
                print("❌ 通報發送失敗。")
        else:
            print("⚠️ 摘要清單為空，跳過發送。")
    else:
        print("❌ Notifier 物件為空，跳過通報階段。")

if __name__ == "__main__":
    main()
