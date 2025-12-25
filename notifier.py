# -*- coding: utf-8 -*-
import os, requests, resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        # 💡 初始化時不強制綁定，改在發送時判斷
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        """獲獲台北時間 (UTC+8)"""
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """發送 Telegram 即時通知 (支援 HTML 格式)"""
        if not self.tg_token or not self.tg_chat_id:
            print("⚠️ 缺失 Telegram 配置，跳過發送。")
            return False
            
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id, 
            "text": message, 
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        try:
            r = requests.post(url, json=payload, timeout=15)
            r.raise_for_status()
            return True
        except Exception as e:
            print(f"❌ Telegram 發送失敗: {e}")
            return False

    def send_stock_report_email(self, all_summaries):
        """
        整合報告發送流程：
        1. 構建數據內容
        2. 獨立發送 Telegram (不受 Email 影響)
        3. 獨立發送 Email (受 Key 檢查保護)
        """
        report_time = self.get_now_time_str()
        market_sections = ""
        tg_brief_list = []

        # --- 數據解析與建構 ---
        for s in all_summaries:
            status_color = "#28a745" if s['status'] == "✅" else "#dc3545"
            success_rate = (s['success'] / s['expected']) * 100 if s['expected'] > 0 else 0
            fail_list = s.get('fail_list', [])
            fail_summary = ", ".join(map(str, fail_list[:20])) if fail_list else "無"
            fail_count_text = f"...等其餘 {len(fail_list)-20} 檔" if len(fail_list) > 20 else ""

            # Email HTML 區塊構建
            market_sections += f"""
            <div style="margin-bottom: 30px; border: 1px solid #ddd; padding: 20px; border-radius: 12px; background-color: #fff;">
                <h2 style="margin-top: 0; color: #333; font-size: 18px;">{s['market']} 數據報告</h2>
                <div style="font-size: 14px; color: #444;">
                    <b>更新覆蓋率:</b> <span style="font-size: 18px; font-weight: bold; background-color: #fff3cd;">{s['coverage']}</span><br>
                    <b>成功/應收:</b> {s['success']} / {s['expected']} ({success_rate:.1f}%)<br>
                    <b>最新日期:</b> {s['end_date']} | <b>總筆數:</b> {s['total_rows']:,}<br>
                    <div style="margin-top: 10px; color: #dc3545; font-size: 12px;">
                        <b>異常摘要:</b> {fail_summary} {fail_count_text}
                    </div>
                </div>
            </div>
            """

            # Telegram 文本構建
            tg_market_msg = (
                f"<b>【{s['market']} 數據報告】</b>\n"
                f"狀態: {s['status']} | 覆蓋率: <b>{s['coverage']}</b>\n"
                f"成功: <code>{s['success']}</code> / <code>{s['expected']}</code>\n"
                f"日期: <code>{s['end_date']}</code> | 異常: <code>{len(fail_list)}</code> 檔"
            )
            tg_brief_list.append(tg_market_msg)

        # --- 第一階段：發送 Telegram (最高優先權) ---
        final_tg_msg = f"📉 <b>全球數據倉儲同步總結</b>\n\n" + "\n\n---\n\n".join(tg_brief_list)
        tg_ok = self.send_telegram(final_tg_msg)
        if tg_ok:
            print("✨ Telegram 通報成功發送。")

        # --- 第二階段：發送 Email (Resend) ---
        # 💡 修正點：嚴格檢查 API Key，失敗不崩潰
        if not self.resend_api_key or len(self.resend_api_key) < 10:
            print("⏭️ 未偵測到有效的 Resend Token，跳過 Email 發送。")
            return tg_ok

        try:
            html_full = f"""
            <html>
            <body style="font-family: sans-serif; background-color: #f4f7f6; padding: 20px;">
                <div style="max-width: 600px; margin: auto; background: white; padding: 25px; border-radius: 12px; border-top: 10px solid #007bff;">
                    <h1 style="text-align: center; color: #333; font-size: 24px;">🌍 數據倉儲監控報告</h1>
                    <p style="text-align: center; color: #888;">報告時間: {report_time}</p>
                    {market_sections}
                    <p style="font-size: 12px; color: #bbb; text-align: center;">💾 自動化系統發送，請勿直接回覆。</p>
                </div>
            </body>
            </html>
            """
            resend.Emails.send({
                "from": "MatrixBot <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": f"📊 股市同步報告 - {report_time.split(' ')[0]}",
                "html": html_full
            })
            print("📧 Email 通報成功發送。")
            return True
        except Exception as e:
            # 💡 即使 Email 因為額度限制失敗，程式也會在這裡捕捉，不會影響主程式運行
            print(f"⚠️ Email 發送失敗 (可能是額度已滿): {e}")
            return False
