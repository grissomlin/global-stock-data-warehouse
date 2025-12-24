# -*- coding: utf-8 -*-
import os
import requests
import resend
import pandas as pd
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        """
        初始化通知模組，自動從環境變數載入 API Key
        """
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        """獲取台北時間 (UTC+8)"""
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """發送 Telegram 即時通知"""
        if not self.tg_token or not self.tg_chat_id:
            return False
        
        ts = self.get_now_time_str().split(" ")[1]
        # 使用 HTML 模式，支援加粗等格式
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id, 
            "text": f"{message}\n\n🕒 <i>Sent at {ts} (UTC+8)</i>", 
            "parse_mode": "HTML"
        }
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except:
            return False

    def send_stock_report(self, market_name, text_reports, stats=None):
        """
        發送 HTML 專業監控報表 (新增資料庫概況統計支援)
        """
        if not self.resend_api_key:
            return False

        report_time = self.get_now_time_str()
        
        # 1. 取得基本統計 (下載器產出)
        stats = stats or {}
        total_stocks = stats.get('total', 'N/A')
        success_stocks = stats.get('success', 0)
        fail_stocks = stats.get('fail', 0)
        fail_list = stats.get('fail_list', [])

        # 2. 💡 取得資料庫概況 (從 main.py 傳入的 summary 資料)
        # 假設我們將資料庫統計放在 stats['summary'] 裡
        db_summary = stats.get('summary', {})
        db_total_rows = db_summary.get('total', 'N/A')
        db_names_synced = db_summary.get('names_synced', 'N/A')
        db_end_date = db_summary.get('end', 'N/A')
        
        success_rate = "0%"
        if isinstance(total_stocks, int) and total_stocks > 0:
            success_rate = f"{(success_stocks / total_stocks * 100):.1f}%"

        # 顏色邏輯
        health_color = "#dc3545" if "⚠️" in text_reports else "#28a745"
        health_bg = "#fff4f4" if "⚠️" in text_reports else "#f6fff8"

        # 失敗名單
        fail_html = ""
        if fail_list:
            display_fails = fail_list[:20]
            fail_html = f"""
            <div style="margin-top: 20px; padding: 15px; background-color: #fff4f4; border-left: 5px solid #dc3545; border-radius: 4px;">
                <strong style="color: #dc3545;">⚠️ 失敗/異常名單摘要 (前 20 筆):</strong><br>
                <code style="word-break: break-all;">{", ".join(map(str, display_fails))}</code>
            </div>
            """

        subject = f"📊 {market_name} 數據倉儲報告 - {report_time.split(' ')[0]}"
        
        html_content = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', sans-serif; color: #333;">
            <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; border-top: 10px solid {health_color}; padding: 20px; border-radius: 8px;">
                <h2 style="margin-top: 0;">{market_name} 市場同步報告</h2>
                <div style="padding: 12px; background-color: {health_bg}; border-radius: 4px; color: {health_color}; font-weight: bold; margin-bottom: 20px;">
                    {text_reports}
                </div>

                <h3 style="border-left: 4px solid #007bff; padding-left: 10px;">📈 下載進度統計</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">應收股票總數</td><td style="text-align: right;">{total_stocks} 檔</td></tr>
                    <tr><td style="padding: 8px; border-bottom: 1px solid #eee; color: #28a745;">成功下載數</td><td style="text-align: right;">{success_stocks} 檔</td></tr>
                    <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">下載成功率</td><td style="text-align: right;"><b>{success_rate}</b></td></tr>
                </table>

                <h3 style="border-left: 4px solid #17a2b8; padding-left: 10px; margin-top: 25px;">🗄️ 資料庫現況 (2020-2025 熱數據)</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">資料庫最新日期</td><td style="text-align: right;">{db_end_date}</td></tr>
                    <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">總行情筆數</td><td style="text-align: right;">{db_total_rows} 筆</td></tr>
                    <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">已同步公司名稱</td><td style="text-align: right;">{db_names_synced} 檔</td></tr>
                </table>

                {fail_html}

                <div style="font-size: 12px; color: #999; margin-top: 30px; text-align: center; border-top: 1px solid #eee; padding-top: 15px;">
                    💾 熱數據庫已優化並同步至 Google Drive<br>
                    此為系統自動發送，請勿直接回覆
                </div>
            </div>
        </body>
        </html>
        """

        try:
            # 1. 發送 Email (使用 Resend)
            resend.Emails.send({
                "from": "StockMatrix <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": subject,
                "html": html_content
            })
            
            # 2. 發送 Telegram 摘要
            status_icon = "✅" if "✅" in text_reports else "⚠️"
            tg_msg = (
                f"{status_icon} <b>{market_name} 同步完成</b>\n"
                f"成功率: {success_rate} ({success_stocks}/{total_stocks})\n"
                f"最新日期: {db_end_date}\n"
                f"總筆數: {db_total_rows}"
            )
            self.send_telegram(tg_msg)
            return True
        except Exception as e:
            print(f"❌ 報表發送失敗: {e}")
            return False
