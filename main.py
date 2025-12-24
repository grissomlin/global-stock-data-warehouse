# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, gzip, shutil, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# 💡 增加連線逾時，確保大檔案傳輸穩定
socket.setdefaulttimeout(600)

# 導入通知工具 (假設您已準備好 notifier.py)
try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except ImportError:
    notifier = None

# 匯入您剛剛重寫過的各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心參數設定 ==========
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 📊 數量門檻預警設定 (依據各國熱數據規模調整)
EXPECTED_MIN_STOCKS = {
    'tw': 900, 'us': 4000, 'cn': 4500, 'hk': 1500, 'jp': 3000, 'kr': 2000
}

def get_drive_service():
    """初始化 Google Drive API"""
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

# ========== 概況統計邏輯 ==========

def get_db_summary(db_path):
    """產出您要求的概況統計資料"""
    db_file = os.path.basename(db_path)
    try:
        conn = sqlite3.connect(db_path)
        # 統計行情表
        df_stats = pd.read_sql("""
            SELECT 
                COUNT(DISTINCT symbol) as stock_count, 
                MIN(date) as start_date, 
                MAX(date) as end_date, 
                COUNT(*) as total_rows 
            FROM stock_prices
        """, conn)
        
        # 額外統計：公司名稱覆蓋率 (來自新表 stock_info)
        info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        conn.close()

        summary = {
            "file": db_file,
            "stocks": df_stats['stock_count'][0],
            "start": df_stats['start_date'][0],
            "end": df_stats['end_date'][0],
            "total": df_stats['total_rows'][0],
            "names_synced": info_count
        }
        return summary
    except Exception as e:
        print(f"⚠️ 統計失敗 {db_file}: {e}")
        return None

def format_summary_table(summary):
    """格式化成您要求的文字表格樣式"""
    if not summary: return "統計失敗"
    table = (
        f"================================================================================\n"
        f"📈 各國股票資料庫概況統計表\n"
        f"================================================================================\n"
        f"檔案名稱: {summary['file']}\n"
        f"股票數量: {summary['stocks']}\n"
        f"最早日期: {summary['start']}\n"
        f"最新日期: {summary['end']}\n"
        f"總筆數  : {summary['total']}\n"
        f"名稱同步: {summary['names_synced']} 檔\n"
    )
    return table

# ========== 雲端與維護邏輯 (省略重複的 upload/download 函數，與您原本一致) ==========
# ... [保留您原本的 download_backup_from_drive, decompress_db, optimize_and_compress, upload_to_drive] ...

# ========== 主程式執行區塊 ==========

def main():
    # 決定跑哪一國 (python main.py tw) 或全跑
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    module_map = {
        'tw': downloader_tw, 'us': downloader_us, 'cn': downloader_cn,
        'hk': downloader_hk, 'jp': downloader_jp, 'kr': downloader_kr
    }
    markets_to_run = [target_market] if target_market in module_map else module_map.keys()

    service = get_drive_service()
    
    for m in markets_to_run:
        db_file = f"{m}_stock_warehouse.db"
        gz_file = f"{db_file}.gz"
        
        print(f"\n--- 🌍 市場啟動: {m.upper()} ---")

        # 1. 嘗試恢復熱數據備份 (從雲端抓回 2020 至今的 DB)
        if not os.path.exists(db_file):
            if not download_backup_from_drive(service, gz_file):
                print(f"🆕 建立全新 {m} 資料庫結構...")
                # 這裡調用各國的 init_db()

        # 2. 執行下載模組 (熱數據模式)
        # 此時各國下載器會同步更新行情與名稱表
        target_module = module_map.get(m)
        target_module.run_sync(mode='hot') 
        
        # 3. 產出統計報表
        summary = get_db_summary(db_file)
        report_text = format_summary_table(summary)
        print(report_text)

        # 4. 優化、壓縮並回傳雲端
        final_gz = optimize_and_compress(db_file)
        if final_gz and service:
            upload_to_drive(service, final_gz)
            os.remove(final_gz)
            
        # 5. 發送通知
        if notifier:
            # 加入數量警示邏輯
            health_status = "✅ 正常"
            if summary and summary['stocks'] < EXPECTED_MIN_STOCKS.get(m, 0):
                health_status = f"⚠️ 異常 (數量低於預期 {EXPECTED_MIN_STOCKS[m]})"
            
            notifier.send_telegram(f"市場: {m.upper()}\n狀態: {health_status}\n{report_text}")

    print("\n✨ 全球數據同步任務圓滿結束")

if __name__ == "__main__":
    main()
