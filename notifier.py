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
        """
        獲取台北時間 (UTC+8) 字串
        """
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """
        發送 Telegram 即時通知
        """
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
        """
        發送 HTML 專業監控報表
        """
        if not self.resend_api_key:
            return False

        report_time = self.get_now_time_str()
        
        # 解析統計數據
        stats = stats or {}
        total = stats.get('total', 'N/A')
        success = stats.get('success', 0)
        fail = stats.get('fail', 0)
        fail_list = stats.get('fail_list', [])
        
        success_rate = "0%"
        if isinstance(total, int) and total > 0:
            success_rate = f"{(success / total * 100):.1f}%"

        # 判斷健康狀態顏色 (是否有警告圖示)
        health_color = "#dc3545" if "⚠️" in text_reports else "#28a745"
        health_bg = "#fff4f4" if "⚠️" in text_reports else "#f6fff8"

        # 失敗名單 HTML
        fail_html = ""
        if fail_list:
            display_fails = fail_list[:20]
            fail_html = f"""
            <div style="margin-top: 20px; padding: 15px; background-color: #fff4f4; border-left: 5px solid #dc3545; border-radius: 4px;">
                <strong style="color: #dc3545;">⚠️ 失敗/異常名單摘要 (前 20 筆):</strong><br>
                <code style="word-break: break-all;">{", ".join(map(str, display_fails))}</code>
                {"<br><small style='color: #666;'>...等其餘股票請查看 GitHub Log</small>" if len(fail_list) > 20 else ""}
            </div>
            """

        subject = f"📊 {market_name} 市場監控報告 - {report_time.split(' ')[0]}"
        
        html_content = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; border-top: 10px solid {health_color}; padding: 20px; border-radius: 8px;">
                <h2 style="margin-top: 0;">{market_name} 數據倉庫更新報告</h2>
                <p style="font-size: 14px; color: #666;">生成時間: {report_time} (UTC+8)</p>
                
                <div style="padding: 12px; background-color: {health_bg}; border-radius: 4px; color: {health_color}; font-weight: bold; margin-bottom: 20px;">
                    {text_reports}
                </div>

                <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
                
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 8px 0; border-bottom: 1px solid #f9f9f9;"><b>應收股票總數:</b></td>
                        <td style="padding: 8px 0; border-bottom: 1px solid #f9f9f9; text-align: right;">{total} 檔</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; border-bottom: 1px solid #f9f9f9;"><b>成功更新筆數:</b></td>
                        <td style="padding: 8px 0; border-bottom: 1px solid #f9f9f9; text-align: right; color: #28a745;">{success} 檔</td>
                    </tr>
                    <tr>
                        <td style="padding: 8px 0; border-bottom: 1px solid #f9f9f9;"><b>失敗/缺漏筆數:</b></td>
                        <td style="padding: 8px 0; border-bottom: 1px solid #f9f9f9; text-align: right; color: #dc3545;">{fail} 檔</td>
                    </tr>
                    <tr style="font-size: 18px;">
                        <td style="padding: 15px 0;"><b>本次更新成功率:</b></td>
                        <td style="padding: 15px 0; text-align: right;"><b>{success_rate}</b></td>
                    </tr>
                </table>

                {fail_html}

                <div style="font-size: 12px; color: #999; margin-top: 30px; text-align: center; border-top: 1px solid #eee; padding-top: 15px;">
                    💾 SQLite 數據庫已優化、壓縮並同步至 Google Drive<br>
                    此郵件為系統自動發送，請勿直接回覆
                </div>
            </div>
        </body>
        </html>
        """

        try:
            # 發送 Email
            resend.Emails.send({
                "from": "StockMatrix <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": subject,
                "html": html_content
            })
            
            # 發送 Telegram 摘要
            tg_msg = (
                f"📊 <b>{market_name} 監控報表</b>\n"
                f"結果: {'⚠️ 數量不足' if '⚠️' in text_reports else '✅ 正常'}\n"
                f"成功率: <b>{success_rate}</b>\n"
                f"更新: {success} / 總數: {total}"
            )
            self.send_telegram(tg_msg)
            return True
        except Exception as e:
            print(f"❌ 報表發送失敗: {e}")
            return False
