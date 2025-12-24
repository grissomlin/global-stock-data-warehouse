# -*- coding: utf-8 -*-
import os, sys, time, random, json, subprocess, sqlite3
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑設定 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 資料存放路徑
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
# 清單快取與審計資料庫路徑
CACHE_LIST_PATH = os.path.join(LIST_DIR, "cn_stock_list_cache.json")
AUDIT_DB_PATH = os.path.join(BASE_DIR, "data_warehouse_audit.db")

# 🛡️ 穩定性設定：保持 4 執行緒避開封鎖
THREADS_CN = 4 
# 💡 數據效期：1 小時 (3600秒) 內抓過就不再重複請求 Yahoo
DATA_EXPIRY_SECONDS = 3600

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def ensure_pkg(pkg: str):
    """確保必要套件已安裝"""
    try:
        __import__(pkg)
    except ImportError:
        log(f"🔧 正在安裝 {pkg}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg])

def init_audit_db():
    """初始化審計資料庫紀錄表"""
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
        conn.commit()
    finally:
        conn.close()

def get_cn_list():
    """獲取 A 股清單：整合接口與快取"""
    ensure_pkg("akshare")
    import akshare as ak
    threshold = 4500  
    
    # 1. 檢查今日清單快取
    if os.path.exists(CACHE_LIST_PATH):
        try:
            file_mtime = os.path.getmtime(CACHE_LIST_PATH)
            if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
                with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) >= threshold:
                        log(f"📦 載入今日清單快取 (共 {len(data)} 檔)")
                        return data
        except: pass

    log("📡 嘗試從 Akshare EM 接口更新清單...")
    try:
        df_sh = ak.stock_sh_a_spot_em()
        df_sz = ak.stock_sz_a_spot_em()
        df = pd.concat([df_sh, df_sz], ignore_index=True)
        
        df['code'] = df['代码'].astype(str).str.zfill(6)
        valid_prefixes = ('000','001','002','003','300','301','600','601','603','605','688')
        df = df[df['code'].str.startswith(valid_prefixes)]
        
        name_col = '名称' if '名称' in df.columns else '名稱'
        res = [f"{row['code']}&{row[name_col]}" for _, row in df.iterrows()]
        
        if len(res) >= threshold:
            with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False)
            return res
    except Exception as e:
        log(f"⚠️ 接口失敗: {e}")

    if os.path.exists(CACHE_LIST_PATH):
        with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return ["600519&貴州茅台", "000001&平安銀行"]

def download_one(item):
    """單檔下載邏輯：智慧快取 + 重試"""
    try:
        code, name = item.split('&', 1)
        symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

        # 💡 智慧快取檢查 (抓過且在效期內則跳過)
        if os.path.exists(out_path):
            file_age = time.time() - os.path.getmtime(out_path)
            if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
                return {"status": "exists", "code": code}

        time.sleep(random.uniform(0.7, 1.5)) 
        tk = yf.Ticker(symbol)
        # 下載 2 年歷史作為增量依據
        hist = tk.history(period="2y", timeout=25)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None)
            
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "code": code}
        return {"status": "empty", "code": code}
    except Exception:
        return {"status": "error", "code": code}

def main():
    start_time = time.time()
    init_audit_db()
    log("🇨🇳 中國 A 股數據同步器 (Audit & Cache 強化版)")
    
    items = get_cn_list()
    log(f"🚀 目標總數: {len(items)} 檔")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    fail_list = [] # 收集失敗名單

    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futures = {executor.submit(download_one, it): it for it in items}
        pbar = tqdm(total=len(items), desc="下載進度")
        
        for f in as_completed(futures):
            res = f.result()
            s = res.get("status", "error")
            stats[s] += 1
            if s in ["error", "empty"]:
                fail_list.append(res.get("code", "Unknown"))
            pbar.update(1)
        pbar.close()

    total = len(items)
    success = stats['success'] + stats['exists']
    fail = stats['error'] + stats['empty']
    rate = round((success / total * 100), 2) if total > 0 else 0

    # 🚀 寫入 Audit DB
    conn = sqlite3.connect(AUDIT_DB_PATH)
    try:
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute('''INSERT INTO sync_audit 
            (execution_time, market_id, total_count, success_count, fail_count, success_rate)
            VALUES (?, ?, ?, ?, ?, ?)''', (now_ts, MARKET_CODE, total, success, fail, rate))
        conn.commit()
    finally:
        conn.close()

    download_stats = {
        "total": total,
        "success": success,
        "fail": fail,
        "fail_list": fail_list  # 回傳給 notifier 顯示
    }

    duration = (time.time() - start_time) / 60
    log(f"📊 執行報告: 成功={success}, 失敗={fail}, 成功率={rate}%")
    
    return download_stats

if __name__ == "__main__":
    main()
