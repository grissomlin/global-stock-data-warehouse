# ========== 3. 優化下載邏輯 (高效版) ==========

def download_one_optimized(symbol_info, mode):
    """優化版單檔下載：多重代碼格式 + 智能延遲"""
    code_5d, name = symbol_info
    start_date = "2020-01-01" if mode == 'hot' else "2000-01-01"
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 生成可能的符號格式
    possible_symbols = get_possible_symbols(code_5d)
    
    for attempt in range(2):  # 減少重試次數
        try:
            # 智能延遲：根據執行環境和嘗試次數調整
            delay = random.uniform(0.3, 0.8) if IS_GITHUB_ACTIONS else random.uniform(0.1, 0.3)
            time.sleep(delay)
            
            # 嘗試多種符號格式
            hist, used_symbol = try_download_with_symbols(possible_symbols, start_date, end_date)
            
            if hist is None or hist.empty:
                if attempt == 1:  # 最後一次嘗試
                    log(f"⚠️  跳過 {code_5d} ({name}): 無有效數據")
                continue
            
            # 標準化並寫入數據庫 (使用成功的符號)
            df_final = standardize_df(hist, used_symbol)
            if df_final.empty:
                continue
                
            conn = sqlite3.connect(DB_PATH, timeout=30)
            # 使用更高效的批量插入
            df_final.to_sql('stock_prices', conn, if_exists='append', index=False,
                          method=lambda t, c, k, d: c.executemany(
                              f"INSERT OR REPLACE INTO {t.name} ({', '.join(k)}) VALUES ({', '.join(['?']*len(k))})", d))
            conn.close()
            return True
            
        except Exception as e:
            if attempt == 1:
                log(f"⏭️  {code_5d} 下載失敗: {e}")
            time.sleep(1)  # 錯誤後短暫等待
    
    return False

# ========== 4. 批次處理與進度優化 ==========

def run_sync_optimized(mode='hot'):
    start_time = time.time()
    init_db()
    
    # 獲取股票清單 (返回5位數代碼和名稱)
    items = get_hk_stock_list()
    if not items:
        log("❌ 無法獲取股票清單")
        return {"success": 0, "has_changed": False}
    
    # 直接使用5位數代碼，無需預篩
    log(f"🚀 開始同步港股 | 執行緒: {MAX_WORKERS} | 股票數: {len(items)}")
    
    total_success = 0
    # 分批處理，避免內存溢出
    batch_size = 100
    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(download_one_optimized, it, mode): it[0] for it in batch}
            
            # 使用tqdm顯示批次進度
            pbar = tqdm(as_completed(futures), total=len(batch), 
                       desc=f"批次 {i//batch_size+1}/{(len(items)+batch_size-1)//batch_size}")
            for f in pbar:
                if f.result():
                    total_success += 1
            pbar.close()
        
        # 批次之間稍作休息
        if i + batch_size < len(items):
            time.sleep(random.uniform(1, 2))
    
    # 資料庫維護
    log("🧹 執行資料庫優化...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("VACUUM")
    
    # 統計
    cursor = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices")
    actual_count = cursor.fetchone()[0]
    conn.close()
    
    duration = (time.time() - start_time) / 60
    log(f"📊 同步完成！成功: {total_success}/{len(items)} 檔 | 費時: {duration:.1f} 分鐘")
    log(f"📈 資料庫現有股票: {actual_count} 檔")
    
    return {
        "success": total_success,
        "total": len(items),
        "coverage": f"{(total_success/len(items)*100):.1f}%",
        "duration_minutes": f"{duration:.1f}"
    }

# 替換主函數
if __name__ == "__main__":
    result = run_sync_optimized(mode='hot')
    print(f"\n🏁 最終結果:")
    print(f"   成功下載: {result['success']}/{result['total']} 檔")
    print(f"   覆蓋率: {result['coverage']}")
    print(f"   總用時: {result['duration_minutes']} 分鐘")
