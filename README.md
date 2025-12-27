# 🤖 Global Stock Data Warehouse (全球股市資料倉庫)

[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

這是一個專為量化交易與技術指標分析設計的資料後援系統。它能自動爬取、處理並儲存來自 **台股 (TW)**、**美股 (US)**、**港股 (HK)** 的全球市場數據，並將其結構化儲存於 SQLite 資料庫中，同時支援雲端同步功能。



---

## 🌟 核心特色

* **多市場支援**：完整覆蓋台股、美股、港股，自動更新標的名單。
* **高效率存取**：利用 SQLite 建立本地快取，大幅降低 API 請求次數，解決 yfinance 頻繁存取被鎖 IP 的問題。
* **雲端同步備援**：支援 Google Drive API 與 GitHub PAT 同步，確保資料不因本地故障而遺失。
* **模組化設計**：分析、爬蟲、資料庫管理邏輯分離，易於擴充自定義指標。

---

## 🛠️ 程式碼功用說明

| 檔案名稱 | 功能描述 |
| :--- | :--- |
| `main.py` | **系統調度員**：控制整體執行流程，決定更新哪個市場。 |
| `crawler.py` | **網路爬蟲**：自動獲取各國交易所最新的成份股與代號清單。 |
| `database.py` | **資料庫管理**：處理 SQLite 的讀寫、資料表結構建立。 |
| `sync_manager.py` | **同步助手**：負責將資料庫同步至 Google Drive 或 GitHub 私有倉庫。 |
| `settings.py` | **設定管理**：集中管理所有環境變數與 API 金鑰。 |

> 📖 **詳細 Repo 架構說明**：[方格子 - 專案詳細說明](https://vocus.cc/article/694f79a8fd89780001ff8c0c)

---

## ⚙️ 環境變數設定說明 (GitHub Secrets)

為了確保程式安全運行，請在 GitHub Repo 的 `Settings > Secrets and variables > Actions` 中設定以下變數：

* **`TELEGRAM_BOT_TOKEN`**: Telegram Bot 的 API Token。
* **`TELEGRAM_CHAT_ID`**: 接收通知的頻道或個人 ID。
* **`GOOGLE_CREDENTIALS`**: Google 服務帳戶的 JSON 憑證內容。
* **`GH_PAT`**: GitHub 個人存取權杖，用於同步私有儲存庫。

> 📘 **Step-by-Step 教學**：[方格子 - GitHub 變數設定教學](https://vocus.cc/article/694f6534fd89780001f9c6ad)

---

## 🆘 技術支援與反饋

在使用過程中，若遇到 **Google Drive 403 Forbidden** 錯誤，通常與服務帳戶的儲存空間配額（Storage Quota）有關。

**如果您有更優雅的解決方案或任何技術建議，歡迎透過 Issues 或方格子留言告知，不吝指教！**

---

## ☕ 支持與贊助 (Donate)

如果您覺得這個專案對您的投資分析有幫助，歡迎請我喝杯咖啡，您的支持是我持續優化程式碼的最大動力！

* **[👉 點此透過方格子贊助支持本專案](https://vocus.cc/pay/donate/606146a3fd89780001ba32e9?donateSourceType=article&donateSourceRefID=694f6534fd89780001f9c6ad)**

---

<img width="679" height="745" alt="image" src="https://github.com/user-attachments/assets/62e191d8-e10c-45a5-b0cd-f42b365d76df" />


## 📄 免責聲明 (Disclaimer)

本專案僅供研究與教育用途，所提供之數據不構成任何投資建議。投資人應自行評估風險，並對其投資結果負責。


