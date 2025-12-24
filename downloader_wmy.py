# -*- coding: utf-8 -*-
import os, re, glob, time, warnings, random, sqlite3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shutil import copy2

# [參數設定省略，與您提供的一致]
AUDIT_DB_PATH = "data_warehouse_audit.db"

def record_conversion_audit(market_key, total, success, skip_records):
    """將轉換結果寫入審計資料庫"""
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS wmy_conversion_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execution_time TEXT,
            market_id TEXT,
            total_files INTEGER,
            success_count INTEGER,
            skip_count INTEGER,
            success_rate REAL
        )''')
        now = (pd.Timestamp.now()).strftime("%Y-%m-%d %H:%M:%S")
        skip = len(skip_records)
        rate = round((success / total * 100), 2) if total > 0 else 0
        conn.execute('INSERT INTO wmy_conversion_audit (execution_time, market_id, total_files, success_count, skip_count, success_rate) VALUES (?,?,?,?,?,?)',
                     (now, market_key, total, success, skip, rate))
        conn.commit()
    finally:
        conn.close()

def _process_one_file(path: str):
    """整合資料異常分析的核心邏輯"""
    try:
        stem = Path(path).stem
        raw_id, _ = _parse_id_name(stem)
        cid = _canonical_id(raw_id)

        # 1. 載入與基礎清洗
        df_day = _load_day_clean_full(path)
        
        # 2. 異常檢查：價格必須 > 0
        if (df_day['收盤'] <= 0).any():
            return "SKIP", {'StockID': cid, 'reason': 'invalid_price'}, "價格含負值或零"

        # 3. 斷層偵測：檢查 2024 年後是否有超過 14 天的資料斷層
        df_check = df_day[df_day['日期'] >= '2024-01-01'].sort_values('日期')
        if not df_check.empty:
            gaps = df_check['日期'].diff().dt.days
            if gaps.max() > 14:
                return "SKIP", {'StockID': cid, 'reason': f'gap_{int(gaps.max())}d'}, "存在長日期斷層"

        # 4. 轉換週/月/年 K
        dfw = _resample_ohlc_with_flags(df_day, 'W'); dfw = _add_period_returns(dfw, 'W'); dfw['StockID'] = cid
        dfm = _resample_ohlc_with_flags(df_day, 'M'); dfm = _add_period_returns(dfm, 'M'); dfm['StockID'] = cid
        dfy = _resample_ohlc_with_flags(df_day, 'Y'); dfy = _add_period_returns(dfy, 'Y'); dfy['StockID'] = cid

        # 5. OHLC 邏輯驗證：收盤價必須在最高與最低價之間
        for _df in [dfw, dfm, dfy]:
            if ((_df['收盤'] > _df['最高']) | (_df['收盤'] < _df['最低'])).any():
                return "SKIP", {'StockID': cid, 'reason': 'ohlc_logic_error'}, "OHLC 邏輯錯誤"

        return True, (dfw, dfm, dfy), None
    except Exception as e:
        return False, None, str(e)

# [其餘 build_wmy_parquets 邏輯整合 record_conversion_audit 即可]