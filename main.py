# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time, gzip, shutil, socket, io
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# 💡 增加連線逾時，確保大檔案傳輸穩定
socket.setdefaulttimeout(600)

# 導入通知工具
try:
    from notifier import StockNotifier
    notifier = StockNotifier()
except ImportError:
    notifier = None

# 匯入各國下載模組 (只負責下載日K與同步Info)
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心參數設定 ==========
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

# 📊 數量門檻預警 (數據倉庫完整性指標)
EXPECTED_MIN_STOCKS = {
    'tw': 900, 'us': 4000, 'cn': 4500, 'hk': 1500, 'jp': 3000, 'kr': 2000
}

# ========== 雲端與基礎維護邏輯 (此處省略 upload/download 具體實現) ==========
# ... [保留原本的 download_backup_from_drive, decompress_db, optimize_and_compress, upload_to_drive] ...

def get_db_summary(db_path):
    """獲取數據倉庫當前狀態 (日K)"""
    db_file = os.path.basename(db_path)
    try:
        conn = sqlite3.connect(db_path)
        # 統計行情表：我們現在只關心日K的入庫情況
        df_stats = pd.read_sql("""
            SELECT 
                COUNT(DISTINCT symbol) as stock_count, 
                MIN(date) as start_date, 
                MAX(date) as end_date, 
                COUNT(*) as total_rows 
            FROM stock_prices
        """, conn)
        
        info_count = conn.execute("SELECT COUNT(*) FROM stock_info").fetchone()[0]
        conn.close()

        return {
            "file": db_file,
            "stocks": df_stats['stock_count'][0],
            "start": df_stats['start_date'][0],
            "end": df_stats['end_date'][0],
            "total": df_stats['total_rows'][0],
            "names_synced": info_count
        }
    except Exception as e:
        print(f"⚠️ 統計失敗 {db_file}: {e}")
        return None

def main():
    """日K數據倉庫同步任務"""
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
        gz_file = f"{db_file}.gz"
        
        print(f"\n--- 🌍 [數據入庫] 市場啟動: {m.upper()} ---")

        # 1. 雲端同步與備份恢復
        if not os.path.exists(db_file):
            download_backup_from_drive(service, gz_file)
            decompress_db(gz_file)

        # 2. 執行各國下載模組 (核心：只抓日K與名稱)
        # 💡 注意：此處不再呼叫任何 WMY 轉換
        target_module = module_map.get(m)
        target_module.run_sync(mode='hot') 
        
        # 3. 數據倉庫完整性統計
        summary = get_db_summary(db_file)
        
        # 4. 優化與上傳 (保持資料庫健康)
        final_gz = optimize_and_compress(db_file)
        if final_gz and service:
            upload_to_drive(service, final_gz)
            os.remove(final_gz)
            
        # 5. 發送監控報表
        if notifier and summary:
            health_icon = "✅"
            if summary['stocks'] < EXPECTED_MIN_STOCKS.get(m, 0):
                health_icon = "⚠️"
            
            msg = (
                f"📊 <b>{m.upper()} 倉庫監控報告</b>\n"
                f"狀態: {health_icon} 資料規模良好\n"
                f"股票數: {summary['stocks']} | 總行數: {summary['total']}\n"
                f"名稱覆蓋: {summary['names_synced']}\n"
                f"最新日期: {summary['end']}\n"
                f"--------------------------------"
            )
            notifier.send_telegram(msg)

    print("\n✨ [Warehouse] 六國原始數據同步任務圓滿結束")

if __name__ == "__main__":
    main()
