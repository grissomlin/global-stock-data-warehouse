# -*- coding: utf-8 -*-
import os, io, time, random, sqlite3, requests, re, json, urllib3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ========== 1. 環境設定 ==========
MARKET_CODE = "kr-share"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kr_stock_warehouse.db")
IS_GITHUB_ACTIONS = os.getenv('GITHUB_ACTIONS') == 'true'

# 優化批次設定
BATCH_SIZE = 30 if IS_GITHUB_ACTIONS else 50
MAX_WORKERS = 3 if IS_GITHUB_ACTIONS else 8
BATCH_DELAY = (1.5, 3.0) if IS_GITHUB_ACTIONS else (0.3, 0.8)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def init_db():
    """初始化数据库，确保表结构正确"""
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                            date TEXT, symbol TEXT, open REAL, high REAL, 
                            low REAL, close REAL, volume INTEGER,
                            PRIMARY KEY (date, symbol))''')
        conn.execute('''CREATE TABLE IF NOT EXISTS stock_info (
                            symbol TEXT PRIMARY KEY, name TEXT, sector TEXT, 
                            market TEXT, updated_at TEXT)''')
        cursor = conn.execute("PRAGMA table_info(stock_info)")
        if 'market' not in [col[1] for col in cursor.fetchall()]:
            conn.execute("ALTER TABLE stock_info ADD COLUMN market TEXT")
            conn.commit()
    finally:
        conn.close()

# ========== 2. 多來源韓國股票清單獲取 ==========

def normalize_kr_code(code: str) -> str:
    """正規化韓國股票代碼為6位數字"""
    if not code:
        return ""
    # 移除所有非數字字符
    digits = re.sub(r'[^0-9]', '', str(code))
    # 補零至6位
    return digits.zfill(6) if digits else ""

def fetch_krx_list_primary():
    """
    主要方法：使用 KRX 的公開API獲取股票清單
    嘗試多個可能的API端點
    """
    endpoints = [
        {
            'name': 'MDCSTAT04301',  # 上市證券現狀
            'url': 'dbms/MDC/STAT/standard/MDCSTAT04301',
            'params': {
                'locale': 'ko_KR',
                'mktId': 'STK',  # STK=股票, KSQ=KOSDAQ
                'trdDd': datetime.now().strftime('%Y%m%d'),
                'share': '1',
                'money': '1',
                'csvxls_isNo': 'false'
            }
        },
        {
            'name': 'MDCSTAT03402',  # 股票市場現狀（備用）
            'url': 'dbms/MDC/STAT/standard/MDCSTAT03402',
            'params': {
                'locale': 'ko_KR',
                'mktId': 'ALL',
                'share': '1',
                'csvxls_isNo': 'false'
            }
        }
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
        'Accept': 'text/csv,application/csv,text/plain'
    }
    
    for endpoint in endpoints:
        try:
            log(f"嘗試 KRX API: {endpoint['name']}")
            
            # 1. 獲取 OTP
            otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
            otp_response = requests.post(
                otp_url, 
                data=endpoint['params'], 
                headers=headers, 
                timeout=15,
                verify=False
            )
            
            if otp_response.status_code != 200:
                log(f"  OTP獲取失敗: {otp_response.status_code}")
                continue
                
            otp_code = otp_response.text.strip()
            if not otp_code:
                log("  OTP為空")
                continue
            
            # 2. 下載 CSV
            download_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
            csv_response = requests.post(
                download_url,
                data={'code': otp_code},
                headers=headers,
                timeout=30,
                verify=False
            )
            
            if csv_response.status_code != 200:
                log(f"  CSV下載失敗: {csv_response.status_code}")
                continue
            
            # 處理編碼 (KRX 使用 cp949)
            csv_response.encoding = 'cp949'
            content = csv_response.text
            
            if not content or len(content.strip()) < 100:
                log(f"  回傳內容過短: {len(content)} 字符")
                continue
            
            # 3. 解析 CSV
            df = pd.read_csv(io.StringIO(content))
            
            if df.empty or len(df) < 10:
                log(f"  數據為空或過少: {len(df)} 行")
                continue
            
            log(f"✅ 成功從 {endpoint['name']} 獲取 {len(df)} 筆數據")
            return df
            
        except Exception as e:
            log(f"  {endpoint['name']} 失敗: {str(e)[:100]}")
            continue
    
    return None

def fetch_fallback_list():
    """
    備用方法：當KRX API失敗時，使用已知的大型股列表
    來源：韓國市值前100大公司 + 主要ETF
    """
    fallback_stocks = [
        # 格式: (代碼, 名稱, 產業, 市場)
        ("005930", "삼성전자", "전기전자", "KOSPI"),
        ("000660", "SK하이닉스", "전기전자", "KOSPI"),
        ("035420", "NAVER", "서비스업", "KOSPI"),
        ("051910", "LG화학", "화학", "KOSPI"),
        ("005380", "현대차", "운수장비", "KOSPI"),
        ("035720", "카카오", "서비스업", "KOSPI"),
        ("000270", "기아", "운수장비", "KOSPI"),
        ("068270", "셀트리온", "의약품", "KOSPI"),
        ("028260", "삼성물산", "건설업", "KOSPI"),
        ("012330", "현대모비스", "운수장비", "KOSPI"),
        ("105560", "KB금융", "금융업", "KOSPI"),
        ("055550", "신한지주", "금융업", "KOSPI"),
        ("009150", "삼성전기", "전기전자", "KOSPI"),
        ("032830", "삼성생명", "보험업", "KOSPI"),
        ("034730", "SK", "화학", "KOSPI"),
        ("086790", "하나금융지주", "금융업", "KOSPI"),
        ("247540", "에코프로비엠", "전기전자", "KOSDAQ"),
        ("066570", "LG전자", "전기전자", "KOSPI"),
        ("003550", "LG", "화학", "KOSPI"),
        ("096770", "SK이노베이션", "화학", "KOSPI"),
        ("352820", "하이브", "서비스업", "KOSPI"),
        ("259960", "크래프톤", "서비스업", "KOSPI"),
        ("207940", "삼성바이오로직스", "의약품", "KOSPI"),
        ("036570", "엔씨소프트", "서비스업", "KOSPI"),
        ("017670", "SK텔레콤", "통신업", "KOSPI"),
    ]
    
    # 轉換為DataFrame格式
    data = []
    for code, name, sector, market in fallback_stocks:
        data.append({
            '종목코드': code,
            '종목명': name,
            '시장구분': market,
            '업종명': sector
        })
    
    return pd.DataFrame(data)

def parse_krx_data(df):
    """解析KRX數據，提取股票信息"""
    if df is None or df.empty:
        log("❌ 無有效數據可解析")
        return []
    
    # 嘗試識別欄位名稱 (韓文)
    code_col = None
    name_col = None
    market_col = None
    sector_col = None
    
    for col in df.columns:
        col_str = str(col).strip()
        if '종목코드' in col_str or '단축코드' in col_str:
            code_col = col
        elif '종목명' in col_str or '기업명' in col_str:
            name_col = col
        elif '시장구분' in col_str or '시장' in col_str:
            market_col = col
        elif '업종명' in col_str or '업종' in col_str:
            sector_col = col
    
    # 如果自動識別失敗，使用前幾列
    if not code_col and len(df.columns) > 0:
        code_col = df.columns[0]
    if not name_col and len(df.columns) > 1:
        name_col = df.columns[1]
    
    if not code_col or not name_col:
        log(f"⚠️ 無法識別必要欄位，可用欄位: {list(df.columns)}")
        return []
    
    stocks = []
    samples = []
    
    for idx, row in df.head(3000).iterrows():  # 限制3000支避免過多
        try:
            # 獲取股票代碼
            raw_code = str(row[code_col]).strip()
            code_6d = normalize_kr_code(raw_code)
            
            if not code_6d or code_6d == '000000':
                continue
            
            # 獲取股票名稱
            name = str(row[name_col]).strip() if name_col else f"KR_{code_6d}"
            
            # 獲取市場信息
            market = "Unknown"
            if market_col and market_col in row:
                market_val = str(row[market_col]).strip().upper()
                if 'KOSPI' in market_val:
                    market = 'KOSPI'
                elif 'KOSDAQ' in market_val:
                    market = 'KOSDAQ'
                else:
                    market = market_val
            
            # 獲取產業分類
            sector = "Other"
            if sector_col and sector_col in row:
                sector = str(row[sector_col]).strip()
                if pd.isna(sector) or sector == 'nan':
                    sector = "Other"
            
            # 決定後綴
            suffix = ".KS" if market == "KOSPI" else ".KQ"
            symbol = f"{code_6d}{suffix}"
            
            stocks.append({
                'symbol': symbol,
                'name': name,
                'code_6d': code_6d,
                'market': market,
                'sector': sector,
                'raw_data': row.to_dict() if hasattr(row, 'to_dict') else {}
            })
            
            if len(samples) < 5:
                samples.append(f"   ✓ {symbol} | {name[:12]:<12} | {market} | {sector[:15]}")
                
        except Exception as e:
            if idx < 5:  # 只顯示前幾個錯誤
                log(f"  解析第{idx}行時出錯: {e}")
            continue
    
    if samples:
        log("🔍 數據樣本:")
        for sample in samples:
            log(sample)
    
    return stocks

def get_kr_stock_list():
    """主函數：獲取韓國股票清單"""
    log("📡 正在從 KRX 獲取最新股票清單...")
    
    # 嘗試主要API
    df = fetch_krx_list_primary()
    
    # 如果主要API失敗，使用備用列表
    if df is None:
        log("⚠️ KRX API 失敗，使用備用清單")
        df = fetch_fallback_list()
    
    # 解析數據
    stocks = parse_krx_data(df)
    
    if not stocks:
        log("❌ 無法獲取任何股票數據")
        return []
    
    # 寫入數據庫
    conn = sqlite3.connect(DB_PATH)
    try:
        for stock in stocks:
            conn.execute("""
                INSERT OR REPLACE INTO stock_info 
                (symbol, name, sector, market, updated_at) 
                VALUES (?, ?, ?, ?, ?)
            """, (
                stock['symbol'], 
                stock['name'], 
                stock['sector'], 
                stock['market'], 
                datetime.now().strftime("%Y-%m-%d")
            ))
        conn.commit()
        log(f"✅ 韓國清單導入成功: {len(stocks)} 檔")
        
    except Exception as e:
        log(f"❌ 寫入數據庫失敗: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    # 返回給下載流程的格式
    return [(stock['symbol'], stock['name']) for stock in stocks]

# ========== 3. 優化的批量下載邏輯 ==========

def safe_yfinance_download(symbols, start_date, max_retries=2):
    """安全的yfinance批量下載，帶重試機制"""
    for attempt in range(max_retries):
        try:
            # 適當延遲避免被限制
            if attempt > 0:
                delay = 2 + random.uniform(0, 2)
                time.sleep(delay)
            
            data = yf.download(
                tickers=symbols,
                start=start_date,
                group_by='ticker',
                auto_adjust=True,
                threads=False,
                progress=False,
                timeout=30 + attempt * 10
            )
            
            return data
        except Exception as e:
            if attempt == max_retries - 1:
                log(f"❌ 下載失敗 ({symbols[0] if symbols else 'N/A'}): {str(e)[:80]}")
            continue
    
    return None

def download_batch(batch_items, mode):
    """下載一批股票數據"""
    symbols = [item[0] for item in batch_items]
    start_date = "2020-01-01" if mode == 'hot' else "2010-01-01"
    
    if not symbols:
        return 0
    
    log(f"  批次下載 {len(symbols)} 檔: {symbols[0]}..{symbols[-1] if len(symbols)>1 else ''}")
    
    data = safe_yfinance_download(symbols, start_date)
    if data is None or data.empty:
        return 0
    
    conn = sqlite3.connect(DB_PATH, timeout=60)
    success_count = 0
    
    try:
        # 處理單一股票或多股票的情況
        is_multi_symbol = len(symbols) > 1
        
        for symbol in symbols:
            try:
                # 提取該股票的數據
                if is_multi_symbol and symbol in data.columns.get_level_values(0):
                    df_symbol = data[symbol].copy()
                elif not is_multi_symbol:
                    df_symbol = data.copy()
                else:
                    continue  # 股票不在下載的數據中
                
                # 清理數據
                df_symbol = df_symbol.dropna(how='all')
                if df_symbol.empty:
                    continue
                
                # 重置索引並標準化
                df_symbol = df_symbol.reset_index()
                df_symbol.columns = [col.lower().replace(' ', '_') for col in df_symbol.columns]
                
                # 識別日期欄位
                date_col = None
                for col in df_symbol.columns:
                    if 'date' in col.lower() or col.lower() == 'index':
                        date_col = col
                        break
                
                if not date_col:
                    continue
                
                # 準備插入數據
                rows_to_insert = []
                for _, row in df_symbol.iterrows():
                    try:
                        date_val = row[date_col]
                        if pd.isna(date_val):
                            continue
                        
                        # 轉換日期格式
                        if hasattr(date_val, 'strftime'):
                            date_str = date_val.strftime('%Y-%m-%d')
                        else:
                            date_str = pd.to_datetime(date_val).strftime('%Y-%m-%d')
                        
                        rows_to_insert.append((
                            date_str,
                            symbol,
                            float(row.get('open', 0)) if not pd.isna(row.get('open')) else 0,
                            float(row.get('high', 0)) if not pd.isna(row.get('high')) else 0,
                            float(row.get('low', 0)) if not pd.isna(row.get('low')) else 0,
                            float(row.get('close', 0)) if not pd.isna(row.get('close')) else 0,
                            int(row.get('volume', 0)) if not pd.isna(row.get('volume')) else 0
                        ))
                    except Exception:
                        continue
                
                if rows_to_insert:
                    # 批量插入
                    conn.executemany(
                        "INSERT OR REPLACE INTO stock_prices VALUES (?,?,?,?,?,?,?)",
                        rows_to_insert
                    )
                    success_count += 1
                    
            except Exception as e:
                if random.random() < 0.1:  # 只記錄10%的錯誤避免日誌過多
                    log(f"   處理 {symbol} 時出錯: {str(e)[:50]}")
                continue
        
        conn.commit()
        
    except Exception as e:
        log(f"❌ 批次處理失敗: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    return success_count

def run_sync(mode='hot'):
    """主同步函數"""
    start_time = time.time()
    init_db()
    
    # 獲取股票清單
    items = get_kr_stock_list()
    if not items:
        log("❌ 無法獲取股票清單，使用最小備用集")
        # 極小備用集，確保至少有數據
        items = [("005930.KS", "Samsung Electronics"), ("000660.KS", "SK Hynix")]
    
    log(f"📊 獲取到 {len(items)} 檔韓國股票")
    
    # 分批處理
    batches = [items[i:i + BATCH_SIZE] for i in range(0, len(items), BATCH_SIZE)]
    log(f"🚀 開始韓股同步 | 批次: {len(batches)} | 每批: {BATCH_SIZE}檔")
    
    total_success = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_batch = {
            executor.submit(download_batch, batch, mode): batch 
            for batch in batches
        }
        
        pbar = tqdm(total=len(items), desc="KR同步", unit="檔")
        
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                success_in_batch = future.result()
                total_success += success_in_batch
                log(f"  批次完成: {success_in_batch}/{len(batch)} 成功")
            except Exception as e:
                log(f"  批次異常: {e}")
            
            # 批次間延遲
            time.sleep(random.uniform(*BATCH_DELAY))
            pbar.update(len(batch))
        
        pbar.close()
    
    # 數據庫優化
    log("🧹 優化數據庫...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    
    # 統計信息
    cursor = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices")
    unique_symbols = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM stock_info")
    total_listed = cursor.fetchone()[0]
    
    conn.close()
    
    duration = (time.time() - start_time) / 60
    
    # 計算覆蓋率（避免除以零）
    coverage = 0
    if total_listed > 0:
        coverage = (unique_symbols / total_listed * 100)
    
    log(f"📊 同步完成！耗時: {duration:.1f}分鐘")
    log(f"✅ 成功下載: {total_success}/{len(items)} 檔")
    log(f"📈 數據庫統計: {unique_symbols} 檔有價格數據 / {total_listed} 檔在清單中")
    log(f"🎯 覆蓋率: {coverage:.1f}%")
    
    return {
        "success": total_success,
        "total": len(items),
        "has_changed": total_success > 0,
        "coverage": f"{coverage:.1f}%",
        "duration_minutes": f"{duration:.1f}"
    }

# ========== 4. 直接執行測試 ==========
if __name__ == "__main__":
    log("=" * 50)
    log("🟢 韓國股票下載器啟動")
    log("=" * 50)
    
    result = run_sync(mode='hot')
    
    log("=" * 50)
    log("🏁 最終結果")
    log(f"   成功: {result['success']}/{result['total']}")
    log(f"   覆蓋率: {result['coverage']}")
    log(f"   耗時: {result['duration_minutes']}分鐘")
    log("=" * 50)
