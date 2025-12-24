# -*- coding: utf-8 -*-
import os, re, glob, time, sqlite3, gzip
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ========== 核心路徑設定 ==========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")
OUTPUT_BASE = os.path.join(BASE_DIR, "data", "processed_wmy")

# ========== 內部工具函式 ==========

def _parse_id_name(stem):
    """解析檔名，提取代號"""
    return stem.split('_')[0], stem

def _canonical_id(raw_id):
    """標準化代號格式"""
    return str(raw_id).strip().upper()

def _load_day_clean_full(path):
    """載入 CSV 並進行基礎清洗與格式化"""
    df = pd.read_csv(path)
    # 統一欄位名稱為中文以利後續邏輯一致
    rename_map = {
        'date': '日期', 'open': '開盤', 'high': '最高', 
        'low': '最低', 'close': '收盤', 'volume': '成交量'
    }
    df = df.rename(columns=rename_map)
    df['日期'] = pd.to_datetime(df['日期'])
    # 移除任何空值
    df = df.dropna(subset=['開盤', '最高', '最低', '收盤'])
    return df

def _resample_ohlc_with_flags(df, period):
    """
    執行 OHLC 採樣轉換
    W=週, M=月, Y=年
    """
    resampler = df.set_index('日期').resample(period)
    df_res = resampler.agg({
        '開盤': 'first',
        '最高': 'max',
        '最低': 'min',
        '收盤': 'last',
        '成交量': 'sum'
    }).dropna()
    return df_res.reset_index()

def _add_period_returns(df, period):
    """計算週期漲跌幅"""
    if not df.empty:
        df['漲跌幅'] = df['收盤'].pct_change().round(4)
    return df

# ========== 審計與處理核心 ==========

def record_conversion_audit(market_key, total, success, skip_records):
    """將轉換結果寫入審計資料庫 (修正為 UTC+8)"""
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
        # 修正時區
        now = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
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
        if df_day.empty:
            return "SKIP", {'StockID': cid, 'reason': 'empty_file'}, "空檔案"
        
        # 2. 異常檢查：價格必須 > 0
        if (df_day['收盤'] <= 0).any():
            return "SKIP", {'StockID': cid, 'reason': 'invalid_price'}, "價格含負值或零"

        # 3. 斷層偵測：檢查 2024 年後是否有超過 14 天的資料斷層
        # 注意：對於日股/美股，國定假日可能導致超過 14 天的斷層，此處門檻可視情況調整
        df_check = df_day[df_day['日期'] >= '2024-01-01'].sort_values('日期')
        if not df_check.empty and len(df_check) > 1:
            gaps = df_check['日期'].diff().dt.days
            if gaps.max() > 14:
                return "SKIP", {'StockID': cid, 'reason': f'gap_{int(gaps.max())}d'}, "存在長日期斷層"

        # 4. 執行轉換
        dfw = _resample_ohlc_with_flags(df_day, 'W-FRI') # 週五為結束
        dfw = _add_period_returns(dfw, 'W'); dfw['StockID'] = cid
        
        dfm = _resample_ohlc_with_flags(df_day, 'M')
        dfm = _add_period_returns(dfm, 'M'); dfm['StockID'] = cid
        
        dfy = _resample_ohlc_with_flags(df_day, 'Y')
        dfy = _add_period_returns(dfy, 'Y'); dfy['StockID'] = cid

        # 5. OHLC 邏輯驗證
        for _df in [dfw, dfm, dfy]:
            if ((_df['收盤'] > _df['最高']) | (_df['收盤'] < _df['最低'])).any():
                return "SKIP", {'StockID': cid, 'reason': 'ohlc_logic_error'}, "OHLC 邏輯錯誤"

        return True, (dfw, dfm, dfy), None
    except Exception as e:
        return False, None, str(e)

# ========== 主入口函式 ==========

def main(market_id, input_dir):
    """
    主進入點：將指定目錄的 CSV 轉為 WMY Parquet
    """
    print(f"🛠️ 開始執行 {market_id} 週期轉換任務...")
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    total = len(csv_files)
    
    if total == 0:
        return {"total": 0, "success": 0, "fail": 0, "fail_list": []}

    success_count = 0
    fail_list = []
    
    all_w, all_m, all_y = [], [], []

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = {executor.submit(_process_one_file, f): f for f in csv_files}
        
        for future in as_completed(futures):
            res_type, data, reason = future.result()
            if res_type is True:
                success_count += 1
                w, m, y = data
                all_w.append(w); all_m.append(m); all_y.append(y)
            elif res_type == "SKIP":
                fail_list.append(f"{data['StockID']}({data['reason']})")
            else:
                fail_list.append(f"Error_{reason}")

    # 儲存結果為 Parquet (高效能壓縮格式)
    if all_w:
        market_out = os.path.join(OUTPUT_BASE, market_id)
        os.makedirs(market_out, exist_ok=True)
        
        pd.concat(all_w).to_parquet(os.path.join(market_out, "weekly.parquet"), index=False)
        pd.concat(all_m).to_parquet(os.path.join(market_out, "monthly.parquet"), index=False)
        pd.concat(all_y).to_parquet(os.path.join(market_out, "yearly.parquet"), index=False)

    # 紀錄審計
    record_conversion_audit(market_id, total, success_count, fail_list)

    print(f"✅ {market_id} 轉換完成：成功 {success_count} / 總量 {total}")
    
    return {
        "total": total,
        "success": success_count,
        "fail": len(fail_list),
        "fail_list": fail_list[:10] # 只回傳前 10 筆給 Email 報表
    }

if __name__ == "__main__":
    # 測試用
    # main("tw-share", "./data/tw-share/dayK")
    pass
