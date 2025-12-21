# -*- coding: utf-8 -*-
import os, sys, sqlite3, json, time
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 匯入各國下載模組
import downloader_tw, downloader_us, downloader_cn, downloader_hk, downloader_jp, downloader_kr

# ========== 核心設定 ==========
# Google Drive 資料夾 ID
GDRIVE_FOLDER_ID = '1ltKCQ209k9MFuWV6FIxQ1coinV2fxSyl' 
# 本地測試用的金鑰路徑 (GitHub Actions 會自動讀取 Secrets)
SERVICE_ACCOUNT_FILE = 'citric-biplane-319514-75fead53b0f5.json'

def get_db_name(market):
    """根據市場代碼動態生成檔案名稱"""
    return f"{market}_stock_warehouse.db"

def init_db(db_file):
    """初始化 SQLite，建立表結構與索引"""
    conn = sqlite3.connect(db_file)
    conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
        date TEXT, symbol TEXT, market TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, updated_at TEXT,
        PRIMARY KEY (date, symbol, market))''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_date_market ON stock_prices (date, market)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON stock_prices (symbol)')
    conn.close()

def check_is_first_time(db_file, market):
    """判斷本地是否有資料，決定抓取長度 (max 或 7d)"""
    if not os.path.exists(db_file): return True
    conn = sqlite3.connect(db_file)
    try:
        # 檢查該市場的資料量
        count = conn.execute("SELECT COUNT(*) FROM stock_prices WHERE market = ?", (market,)).fetchone()[0]
        return count == 0
    except:
        return True
    finally:
        conn.close
