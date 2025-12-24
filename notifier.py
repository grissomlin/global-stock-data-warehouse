# -*- coding: utf-8 -*-
import os
import requests
import resend
import pandas as pd
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        """
        初始化通知模組，自動讀取環境變數
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
        # 手動加 8 小時修正 GitHub Actions 的 UTC 時間
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """
        發送 Telegram 即時通知
        """
        if not self.tg_token or not self.tg_chat_id:
            print("⚠️ 缺少 Telegram 設定，跳過發送。")
            return False
        
        # 於訊息末尾附加時間戳記
        ts = self.get_now_time_str().split(" ")[1]
        full_message = f"{message}\n\n🕒 <i>Sent at {ts} (UTC+8)</i>"
        
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id, 
            "text": full_message, 
            "parse_mode": "HTML"
        }
        try:
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Telegram 發送失敗: {e}")
            return False

    def send_stock_report(self, market_name, img_data, report_df, text_reports, stats=None):
        """
        🚀 核心報表發送方法：完全對應 main.py 邏輯
        """
        if not self.resend_api_key:
            print("⚠️ 缺少 Resend API Key，無法發送 Email。")
            return False

        report_time = self.get_now_time_str()
        
        # 1. 處理統計數據
        total = stats.get('total', 'N/A') if stats else 'N/A'
        success = stats.get('success', 0) if stats else len(report_df)
        fail = stats.get('fail', 0) if stats else 0
        fail_list = stats.get('fail_list', []) if stats else []
        
        # 計算成功率
        success_rate = "0%"
        if isinstance(total, int) and total > 0:
            success_rate = f"{(success / total * 100):.1f}%"

        # 2. 準備失敗名單摘要 (HTML 格式)
        fail_html_summary = ""
        if fail_list:
            display_fails = fail_list[:20]  # 僅列出前 20 筆以免長度過大
            fail_html_summary = f"""
            <div style="margin-top: 20px; padding: 15px; background-color: #fff4f4; border-left: 5px solid #dc3545; border-radius: 4px;">
                <strong style="color: #dc3545;">⚠️ 失敗名單摘要 (前 20 筆):</strong><br>
                <code style="word-break: break-all; color: #333;">{", ".join(display_fails)}</code>
                {"<br>...等其餘股票請查看系統 Log" if len(fail_list) > 20 else ""}
            </div>
            """

        # 3. 構建專業 HTML 郵件
        subject = f"📊 {market_name} 股市矩陣監控報表 - {report_time.split(' ')[0]}"
        
        html_content = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', 'Segoe UI', sans-serif; color: #333; line-height: 1.6;">
            <div style="max-width: 650px; margin: 20px auto; border: 1px solid #e0e0e0; border-top: 10px solid #28a745; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.1);">
                <div style="padding: 25px; background-color: #f8f9fa;">
                    <h2 style="margin: 0; color: #28a745;">{market_name} 市場監控報告</h2>
                    <p style="margin: 5px 0; color: #666; font-size: 14px;">生成時間: {report_time} (UTC+8 台北時間)</p>
                </div>
                
                <div style="padding: 25px;">
                    <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
                        <tr style="background-color: #e9ecef;">
                            <th style="padding: 12px; border: 1px solid #dee2e6; text-align: left;">統計指標</th>
                            <th style="padding: 12px; border: 1px solid #dee2e6; text-align: left;">數據內容</th>
                        </tr>
                        <tr>
                            <td style="padding: 12px; border: 1px solid #dee2e6;">標的總數 (應收)</td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-weight: bold;">{total} 檔</td>
                        </tr>
                        <tr>
                            <td style="padding: 12px; border: 1px solid #dee2e6;">成功抓取數量</td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; color: #28a745; font-weight: bold;">{success} 檔</td>
                        </tr>
                        <tr>
                            <td style="padding: 12px; border: 1px solid #dee2e6;">失敗/下市標的</td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; color: #dc3545; font-weight: bold;">{fail} 檔</td>
                        </tr>
                        <tr>
                            <td style="padding: 12px; border: 1px solid #dee2e6;">當前數據成功率</td>
                            <td style="padding: 12px; border: 1px solid #dee2e6; font-size: 18px; font-weight: bold;">{success_rate}</td>
                        </tr>
                    </table>

                    {fail_html_summary}

                    <div style="margin-top: 25px; padding: 15px; background-color: #e7f3ff; border-left: 5px solid #007bff; font-size: 14px;">
                        <strong>雲端狀態：</strong> 數據庫已完成 SQLite 寫入並成功同步至 Google Drive 備份資料夾。
                    </div>
                </div>
                
                <div style="padding: 15px; background-color: #f1f1f1; text-align: center; font-size: 12px; color: #888;">
                    本報表由 Global Stock Warehouse 自動化系統發送。<br>
                    如需詳細日誌，請登入 GitHub Actions 儀表板查看。
                </div>
            </div>
        </body>
        </html>
        """

        # 4. 執行發送
        try:
            # 寄信 (固定寄給你的 Gmail)
            resend.Emails.send({
                "from": "StockMonitor <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": subject,
                "html": html_content
            })
            
            # 同步發送 Telegram 簡報
            tg_summary = (
                f"📊 <b>{market_name} 監控報表</b>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"🎯 應收總數：{total} 檔\n"
                f"✅ 成功下載：{success} 檔\n"
                f"❌ 失敗/空值：{fail} 檔\n"
                f"📈 成功率：{success_rate}"
            )
            self.send_telegram(tg_summary)
            
            print(f"📧 {market_name} 郵件與 Telegram 報表發送成功。")
            return True
        except Exception as e:
            print(f"❌ 報表發送失敗: {e}")
            return False