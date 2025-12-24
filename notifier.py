# -*- coding: utf-8 -*-
import os
import requests
import resend
import pandas as pd
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        if not self.tg_token or not self.tg_chat_id:
            return False
        
        ts = self.get_now_time_str().split(" ")[1]
        full_message = f"{message}\n\n🕒 <i>Sent at {ts} (UTC+8)</i>"
        
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id, 
            "text": full_message, 
            "parse_mode": "HTML"
        }
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except:
            return False

    def send_stock_report(self, market_name, img_data, report_df, text_reports, stats=None):
        if not self.resend_api_key:
            return False

        report_time = self.get_now_time_str()
        
        stats = stats or {}
        total = stats.get('total', 'N/A')
        success = stats.get('success', 0)
        fail = stats.get('fail', 0)
        fail_list = stats.get('fail_list', [])
        
        success_rate = "0%"
        if isinstance(total, int) and total > 0:
            success_rate = f"{(success / total * 100):.1f}%"

        fail_html = ""
        if fail_list:
            display_fails = fail_list[:20]
            fail_html = f"""
            <div style="margin-top: 20px; padding: 15px; background-color: #fff4f4; border-left: 5px solid #dc3545;">
                <strong style="color: #dc3545;">⚠️ 失敗名單摘要 (前 20 筆):</strong><br>
                <code>{", ".join(map(str, display_fails))}</code>
                {"<br>...等其餘股票請查看日誌" if len(fail_list) > 20 else ""}
            </div>
            """

        subject = f"📊 {market_name} 監控報表 - {report_time.split(' ')[0]}"
        html_content = f"""
        <html>
        <body style="font-family: sans-serif; color: #333;">
            <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; border-top: 10px solid #28a745; padding: 20px;">
                <h2>{market_name} 市場監控報告</h2>
                <p>生成時間: {report_time} (UTC+8)</p>
                <hr>
                <table style="width: 100%;">
                    <tr><td><b>應收總數:</b></td><td>{total} 檔</td></tr>
                    <tr><td><b>成功更新:</b></td><td style="color: #28a745;">{success} 檔</td></tr>
                    <tr><td><b>失敗/斷層:</b></td><td style="color: #dc3545;">{fail} 檔</td></tr>
                    <tr><td><b>成功率:</b></td><td><b>{success_rate}</b></td></tr>
                </table>
                {fail_html}
                <p style="font-size: 12px; color: #999; margin-top: 30px; text-align: center;">
                    數據庫已寫入並同步至 Google Drive
                </p>
            </div>
        </body>
        </html>
        """

        try:
            resend.Emails.send({
                "from": "StockMatrix <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": subject,
                "html": html_content
            })
            
            tg_msg = (
                f"📊 <b>{market_name} 監控報表</b>\n"
                f"成功率: {success_rate}\n"
                f"更新: {success} 檔 / 失敗: {fail} 檔"
            )
            self.send_telegram(tg_msg)
            return True
        except Exception as e:
            print(f"❌ 報表發送失敗: {e}")
            return False
