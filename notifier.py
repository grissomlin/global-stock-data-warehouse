# -*- coding: utf-8 -*-
import os, requests, resend
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

    def _get_market_config(self, market):
        """100% 恢復六國連結，絕對不漏掉任何一個市場"""
        m = market.upper()
        if m == 'US': return "StockCharts", "https://stockcharts.com/sc3/ui/?s=GWAV"
        if m == 'CN': return "東方財富網 (EastMoney)", "https://quote.eastmoney.com/sh603165.html"
        if m == 'HK': return "AASTOCKS 阿思達克", "http://www.aastocks.com/tc/stocks/quote/stocktrend.aspx?symbol=08203"
        if m == 'TW': return "玩股網 (WantGoo)", "https://www.wantgoo.com/stock/2330"
        if m == 'JP': return "樂天證券 (Rakuten)", "https://www.rakuten-sec.co.jp/web/market/search/quote.html?ric=2850.T"
        if m == 'KR': return "Investing.com KR", "https://kr.investing.com/indices/kospi"
        return "Yahoo Finance", "https://finance.yahoo.com/"

    def send_telegram(self, message):
        if not self.tg_token or not self.tg_chat_id: return False
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {"chat_id": self.tg_chat_id, "text": message, "parse_mode": "HTML"}
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except: return False

    def send_stock_report_email(self, all_summaries):
        if not self.resend_api_key: return False
        
        report_time = self.get_now_time_str()
        market_sections = ""
        tg_brief = []

        for s in all_summaries:
            status_color = "#28a745" if s['status'] == "✅" else "#dc3545"
            site_name, chart_url = self._get_market_config(s['market'])
            
            # 💡 依照你要求的格式，手動展開 HTML，欄位全齊
            market_sections += f"""
            <div style="margin-bottom: 40px; border: 1px solid #ddd; padding: 25px; border-radius: 12px; background-color: #fff;">
                <h2 style="margin-top: 0; color: #333; font-size: 20px;">{s['market']}股市 全方位監控報告</h2>
                <div style="font-size: 14px; color: #666; margin-bottom: 15px;">生成時間: {report_time} (台北時間)</div>

                <div style="font-size: 16px; line-height: 1.8; color: #444;">
                    <div style="margin-bottom: 15px;">
                        <b>應收標的</b><br><span style="font-size: 18px;">{s['expected']}</span><br>
                        <b>更新成功(含快取)</b><br><span style="font-size: 18px; color: #28a745;">{s['success']}</span><br>
                        <b>今日覆蓋率</b><br><span style="font-size: 22px; font-weight: bold; background-color: #fff3cd; padding: 2px 8px;">{s['coverage']}</span>
                    </div>
                    
                    <div style="border-top: 1px dashed #ccc; padding-top: 15px; margin-top: 15px;">
                        <b>狀態:</b> <span style="color: {status_color}; font-weight: bold;">{s['status']}</span> | <b>最新日期:</b> {s['end_date']}<br>
                        <b>股票數:</b> {s['success']} | <b>總筆數:</b> <span style="color: #6f42c1; font-weight: bold;">{s['total_rows']:,}</span><br>
                        <b>名稱同步:</b> {s['names_synced']}
                    </div>
                </div>

                <div style="margin-top: 20px; font-size: 13px; color: #666;">
                    💡 提示：下方的數據報表若包含股票代號，點擊可直接跳轉至 <b>{site_name}</b> 查看該市場之即時技術線圖。
                </div>
                <a href="{chart_url}" style="display: inline-block; margin-top: 10px; color: #007bff; text-decoration: none; font-weight: bold; border: 1px solid #007bff; padding: 5px 15px; border-radius: 5px;">
                    🔗 進入 {site_name} 技術線圖
                </a>
            </div>
            """
            tg_brief.append(f"{s['status']} {s['market']}: {s['coverage']} (總筆數: {s['total_rows']:,})")

        html_full = f"""
        <html>
        <body style="font-family: 'Microsoft JhengHei', sans-serif; background-color: #f4f7f6; padding: 20px;">
            <div style="max-width: 650px; margin: auto; background: white; padding: 30px; border-radius: 12px; border-top: 15px solid #007bff; box-shadow: 0 4px 15px rgba(0,0,0,0.1);">
                <h1 style="text-align: center; color: #333; margin-bottom: 30px;">🌍 全球股市數據倉儲監控報告</h1>
                {market_sections}
                <div style="font-size: 12px; color: #bbb; text-align: center; margin-top: 40px; border-top: 1px solid #eee; padding-top: 20px;">
                    💾 熱數據庫已優化並同步至 Google Drive<br>
                    此為自動發送，請勿直接回覆。
                </div>
            </div>
        </body>
        </html>
        """

        try:
            resend.Emails.send({
                "from": "StockMatrix <onboarding@resend.dev>",
                "to": "grissomlin643@gmail.com",
                "subject": f"📊 全球股市同步報告 - {report_time.split(' ')[0]}",
                "html": html_full
            })
            self.send_telegram("📉 <b>數據倉庫同步概況</b>\n" + "\n".join(tg_brief))
            return True
        except Exception as e:
            print(f"❌ 通報錯誤: {e}")
            return False
