# -*- coding: utf-8 -*-
"""
downloader_hk.py - 港股數據下載器 (優化兼容版)
修復 run_sync 缺失錯誤，整合5位代碼處理與批次下載優化。
"""
import os, io, re, time, random, sqlite3, requests, urllib3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境與參數設定 ==========
MARKET_CODE = "hk-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hk_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# 下載設定：動態調整並發數與延遲
MAX_WORKERS = 2 if IS_GITHUB_ACTIONS else 4
BASE_DELAY = 0.5 if IS_GITHUB_ACTIONS else 0.2  # 基礎延遲大幅縮短

def log(msg: str):
    """統一日誌格式"""
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 2. 核心工具函數 ==========
def normalize_code_any(s: str, length: int = 5) -> str:
    """正規化股票代碼為指定長度 (預設5位)"""
    digits = re.sub(r"\D", "", str(s or ""))
    if digits and digits.isdigit():
        return digits.zfill(length)[-length:]
    return ""

def get_possible_symbols(code_5d: str):
    """為5位數代碼生成Yahoo Finance可能使用的符號列表"""
    symbols = []
    symbols.append(f"{code_5d}.HK")           # 格式1: 完整5位數
    if code_5d.startswith("0"):
        symbols.append(f"{code_5d[1:]}.HK")   # 格式2: 去首零的4位數
        if code_5d.startswith("00"):
            symbols.append(f"{code_5d[2:]}.HK") # 格式3: 去兩零的3位數(極少數)
    return symbols

def init_db():
    """初始化數據庫表格"""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, 
                            sector TEXT, market TEXT, updated_at TEXT)''')
        # 檢查並升級market欄位
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        if 'market' not in [col[1] for col in cursor.fetchall()]:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()
    log("✅ 數據庫初始化/檢查完成")

# ========== 3. 港股清單獲取 (5位數代碼版) ==========
def download_hkex_xls(url: str) -> pd.DataFrame:
    """下載港交所Excel文件"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, headers=headers, timeout=30, verify=False)
    r.raise_for_status()
    return pd.read_excel(io.BytesIO(r.content), header=None)

def parse_hkex_list():
    """解析港交所官方清單，返回(5位代碼, 名稱)列表"""
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    log("📡 正在從港交所同步最新名單...")
    
    try:
        df_raw = download_hkex_xls(url)
        # 尋找表頭行
        hdr_idx = None
        for i in range(min(20, len(df_raw))):
            row_vals = [str(x).replace('\xa0', ' ').strip() for x in df_raw.iloc[i].values]
            if any("Stock Code" in val for val in row_vals) and any("Short Name" in val for val in row_vals):
                hdr_idx = i
                break
        
        if hdr_idx is None:
            raise ValueError("無法定位Excel表頭")
        
        # 解析數據
        df = df_raw.iloc[hdr_idx+1:].copy()
        df.columns = [str(x).replace('\xa0', ' ').strip() for x in df_raw.iloc[hdr_idx].values]
        
        # 識別關鍵欄位
        code_col = next((c for c in df.columns if "Stock Code" in c), None)
        name_col = next((c for c in df.columns if "Short Name" in c), None)
        
        if not code_col or not name_col:
            raise ValueError("找不到必要的代碼或名稱欄位")
        
        stock_list = []
        conn = sqlite3.connect(DB_PATH)
        
        for _, row in df.iterrows():
            raw_code = str(row[code_col]).strip()
            name = str(row[name_col]).strip()
            code_5d = normalize_code_any(raw_code, 5)
            
            # 過濾：僅處理有效5位數代碼且數值>=100
            if code_5d and code_5d.isdigit() and 100 <= int(code_5d) <= 99999:
                # 將5位代碼存入stock_info的symbol欄位
                conn.execute("""INSERT OR REPLACE INTO stock_info 
                                (symbol, name, sector, market, updated_at) 
                                VALUES (?, ?, ?, ?, ?)""",
                           (code_5d, name, "Unknown", "HKEX", 
                            datetime.now().strftime("%Y-%m-%d")))
                stock_list.append((code_5d, name))
        
        conn.commit()
        conn.close()
        log(f"✅ 清單解析成功: {len(stock_list)} 檔股票 (使用5位代碼)")
        return stock_list
        
    except Exception as e:
        log(f"⚠️ 清單獲取失敗 {e}，使用保底清單")
        # 保底清單也使用5位代碼
        return [("00700", "TENCENT"), ("00005", "HSBC"), 
                ("00941", "CHINA MOBILE"), ("00001", "CK HUTCHISON")]

# ========== 4. 優化下載邏輯 ==========
def safe_history_multi(symbols_list, start_date, max_retries=2):
    """嘗試多種符號格式獲取歷史數據"""
    for symbol in symbols_list:
        for attempt in range(max_retries):
            try:
                tk = yf.Ticker(symbol)
                # 先嘗試短期數據驗證股票有效
                df = tk.history(period="5d", interval="1d", 
                               auto_adjust=True, timeout=15)
                if df is not None and not df.empty:
                    # 有效則獲取完整數據
                    df_full = tk.history(start=start_date, auto_adjust=True, timeout=20)
                    return df_full, symbol
                time.sleep(0.3)
            except Exception:
                time.sleep(0.5 * (attempt + 1))
                continue
    return None, None

def download_one_stock(stock_info, mode='hot'):
    """下載單一股票數據 (核心優化函數)"""
    code_5d, name = stock_info
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    
    # 1. 生成可能的Yahoo符號
    possible_symbols = get_possible_symbols(code_5d)
    
    # 2. 智能延遲：GitHub環境稍長，本地較短
    delay = random.uniform(BASE_DELAY, BASE_DELAY * 1.5)
    time.sleep(delay)
    
    # 3. 嘗試下載數據
    hist, used_symbol = safe_history_multi(possible_symbols, start_date)
    
    if hist is None or hist.empty:
        return False  # 靜默失敗，減少日誌噪音
    
    # 4. 處理並寫入數據
    try:
        hist = hist.reset_index()
        hist.columns = [c.lower() for c in hist.columns]
        
        # 處理MultiIndex
        if isinstance(hist.columns, pd.MultiIndex):
            hist.columns = hist.columns.get_level_values(0)
        
        if 'date' in hist.columns:
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None)
            hist['date'] = hist['date'].dt.strftime('%Y-%m-%d')
        
        # 選取所需欄位
        required_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
        if all(col in hist.columns for col in required_cols):
            df_final = hist[required_cols].copy()
            df_final['symbol'] = used_symbol  # 使用成功的Yahoo符號
            
            # 高效批量寫入
            conn = sqlite3.connect(DB_PATH, timeout=30)
            df_final.to_sql('stock_prices', conn, if_exists='append', 
                           index=False, method='multi', chunksize=100)
            conn.close()
            return True
    except Exception:
        pass
    
    return False

# ========== 5. 主同步函數 (兼容性關鍵) ==========
def run_sync(mode='hot'):
    """
    主同步函數 - 必須存在以被main.py調用
    整合批次處理與進度顯示
    """
    start_time = time.time()
    init_db()
    
    # 獲取股票清單
    stock_items = parse_hkex_list()
    if not stock_items:
        log("❌ 無法獲取股票清單")
        return {"success": 0, "has_changed": False}
    
    total_count = len(stock_items)
    log(f"🚀 開始同步港股 | 執行緒: {MAX_WORKERS} | 總數: {total_count}")
    
    # 批次處理避免資源耗盡
    batch_size = 50
    success_count = 0
    
    for batch_idx in range(0, total_count, batch_size):
        batch = stock_items[batch_idx:batch_idx + batch_size]
        batch_num = batch_idx // batch_size + 1
        total_batches = (total_count + batch_size - 1) // batch_size
        
        log(f"📦 處理批次 {batch_num}/{total_batches} ({len(batch)}檔)")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_one_stock, item, mode): item 
                      for item in batch}
            
            # 進度條
            pbar = tqdm(as_completed(futures), total=len(batch), 
                       desc=f"批次{batch_num}下載", leave=False)
            for future in pbar:
                if future.result():
                    success_count += 1
            pbar.close()
        
        # 批次間隔
        if batch_idx + batch_size < total_count:
            time.sleep(random.uniform(1, 2))
    
    # 數據庫維護
    log("🧹 優化數據庫...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    
    # 統計信息
    unique_symbols = conn.execute(
        "SELECT COUNT(DISTINCT symbol) FROM stock_prices"
    ).fetchone()[0]
    conn.close()
    
    # 計算耗時
    duration_min = (time.time() - start_time) / 60
    coverage_pct = (success_count / total_count * 100) if total_count > 0 else 0
    
    log(f"📊 同步完成！耗時: {duration_min:.1f}分鐘")
    log(f"✅ 成功: {success_count}/{total_count}檔 | 覆蓋率: {coverage_pct:.1f}%")
    log(f"📈 數據庫唯一股票數: {unique_symbols}")
    
    return {
        "success": success_count,
        "total": total_count,
        "has_changed": success_count > 0,
        "coverage": f"{coverage_pct:.1f}%",
        "duration_minutes": f"{duration_min:.1f}"
    }

def run_sync_optimized(mode='hot'):
    """優化版本的別名，可選調用"""
    return run_sync(mode)

# ========== 6. 直接執行測試 ==========
if __name__ == "__main__":
    log("=" * 50)
    log("🟢 港股下載器獨立測試啟動")
    log("=" * 50)
    
    result = run_sync(mode='hot')
    
    log("=" * 50)
    log("🏁 測試結果摘要")
    log(f"   成功下載: {result['success']}/{result['total']}檔")
    log(f"   數據覆蓋: {result['coverage']}")
    log(f"   耗時: {result['duration_minutes']}分鐘")
    log("=" * 50)
