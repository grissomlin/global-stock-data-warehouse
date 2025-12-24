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
    """根據市場代碼動態生成檔案名稱"""
    return f"{market}_stock_warehouse.db"

def init_db(db_file):
    """初始化數據存儲 SQLite"""
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
    """
    ✨ 新增：紀錄審計日誌至 data_warehouse_audit.db
    """
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
        
        # 獲取台北時間
        now_ts = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        
        conn.execute('''INSERT INTO sync_audit 
            (execution_time, market_id, total_count, success_count, fail_count, success_rate)
            VALUES (?, ?, ?, ?, ?, ?)''', 
            (now_ts, market_id, total, success, fail, rate))
        conn.commit()
        print(f"📋 Audit Log 已記錄至 {AUDIT_DB_PATH}")
    except Exception as e:
        print(f"⚠️ Audit Log 記錄失敗: {e}")
    finally:
        conn.close()

def check_is_first_time(db_file, market):
    if not os.path.exists(db_file): return True
    conn = sqlite3.connect(db_file)
    try:
        cursor = conn.execute("SELECT COUNT(*) FROM stock_prices WHERE market = ?", (market,))
        return cursor.fetchone()[0] == 0
    except: return True
    finally: conn.close()

def update_database(db_file, market, df):
    """將抓取的數據存入資料庫，並標註 UTC+8 更新時間"""
    if df is None or df.empty: return
    
    df['market'] = market
    # 強制使用台北時間標記
    taipei_now = datetime.utcnow() + timedelta(hours=8)
    df['updated_at'] = taipei_now.strftime("%Y-%m-%d %H:%M:%S")
    
    conn = sqlite3.connect(db_file)
    try:
        conn.execute("PRAGMA journal_mode = WAL;")  
        conn.execute("PRAGMA synchronous = OFF;")   
        conn.execute("PRAGMA cache_size = -1000000;")
        df.to_sql('stock_prices', conn, if_exists='append', index=False)
        conn.commit()
        print(f"✅ {market.upper()}: 成功寫入 {len(df)} 筆交易記錄")
    except Exception as e:
        print(f"⚠️ {market.upper()}: 寫入提醒: {e}")
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
        print(f"🚀 {db_file} 雲端同步成功")
        return True
    except Exception:
        print(f"❌ {db_file} 同步失敗！")
        return False

def main():
    target_market = sys.argv[1].lower() if len(sys.argv) > 1 else None
    
    modules = {
        'tw': downloader_tw.main,
        'us': downloader_us.main,
        'cn': downloader_cn.main,
        'hk': downloader_hk.main,
        'jp': downloader_jp.main,
        'kr': downloader_kr.main
    }

    markets_to_run = [target_market] if target_market in modules else modules.keys()

    for m in markets_to_run:
        try:
            db_file = get_db_name(m)
            print(f"\n--- 🌍 市場任務啟動: {m.upper()} ---")
            
            init_db(db_file)
            is_first = check_is_first_time(db_file, m)
            
            # 1. 執行抓取並接收詳細統計 (stats)
            # 假設各模組已修改為 return {"total": x, "success": y, "fail": z, "fail_list": [...]}
            stats = modules[m]() 
            
            # 2. 判斷是否成功
            if stats and stats.get('success', 0) > 0:
                # 這裡假設下載器會順便存好 CSV，main.py 負責後續同步或入庫
                # 如果你的下載器直接回傳 DF，則需在此調用 update_database
                
                upload_status = upload_to_drive(db_file)
                
                # 3. 發送詳細報告
                notifier.send_stock_report(
                    market_name=m.upper(),
                    img_data=None, # 若有圖表可傳入路劇
                    report_df=pd.DataFrame(), # 這裡可傳入分析後的結果
                    text_reports="",
                    stats=stats
                )
                
                # 4. 紀錄審計日誌
                record_audit_log(m, stats)
            else:
                error_msg = f"❌ {m.upper()} 無數據更新或抓取完全失敗。"
                print(error_msg)
                notifier.send_telegram(error_msg)
        
        except Exception as e:
            err_detail = f"❌ {m.upper()} 執行異常: {str(e)}"
            print(err_detail)
            notifier.send_telegram(err_detail)
    
    print("\n✨ 任務圓滿結束")

if __name__ == "__main__":
    main()
