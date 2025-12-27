# 🚀 Alpha-Data-Cleaning-Lab

**Alpha-Data-Cleaning-Lab** 是一個基於 Streamlit 開發的全球股市大數據分析儀表板。結合了 Google Gemini AI 的強大診斷能力，協助投資者從海量數據中快速萃取關鍵洞察。

### ✍️ 關於這個專案 (開發者的真心話)
這是一個**普通上班族**利用業餘時間開發的專案。初衷是為了簡化日常投資分析的瑣碎流程。

在數據處理上，我已經**盡可能地進行資料清洗（Data Cleaning）**，力求將資料洗得乾淨、正確；但受限於個人開發資源與原始數據源的品質，難免會有力有未逮之處。這不是專業機器的產物，而是純手工調教的心血。

**⚠️ 關於資料範圍：**
受限於 GitHub 儲存空間與原始資料結構的影響，本儀表板數據目前僅提供 **2023 年至最新交易日** 的資料。這對於短中線分析已具備足夠參考價值。

---

## 🛠️ 必要的前置作業

本專案主要負責「前端呈現與 AI 診斷」，並不包含爬蟲抓取功能。您**必須搭配以下資料庫專案**才能正常運作：

* **資料來源 Repo**：[global-stock-data-warehouse](https://github.com/grissomlin/global-stock-data-warehouse)
  *(請先依照該專案說明生成各國市場的 `.db` 檔案，並放置於本專案目錄下，否則儀表板將無資料可讀)*

---

## 📖 快速上手與相關資源

為了讓您能更順利地架設並使用本系統，建議參考以下資源：

* 🌐 **正式版儀表板**：[點此立即體驗](https://alpha-data-cleaning-lab-c5mtpheerynowxcw2vtzew.streamlit.app/)
* ⚙️ **環境與 AI 設定教學**：[點此閱讀文章](https://vocus.cc/article/694f813afd8978000101e75a)
* 📖 **儀表板功能詳解**：[點此閱讀文章](https://vocus.cc/article/694f88bdfd89780001042d74)

---

## ✨ 核心分析功能

### 🌎 全球強勢股連動 (`Global_Trend.py`)
* **跨國同步**：支援 TW, US, CN, JP, HK, KR 六大市場。
* **AI 宏觀診斷**：自動分析全球產業資金流向與跨國聯動現象。

### 🔍 個股 AI 深度掃描 (`Deep_Scan.py`)
* **雷達圖分析**：動能、穩定度、防禦力三維評分。
* **股性統計**：2023 至今的漲停次數、炸板率與隔日溢價期望值。

---
☕ 支持贊助與免責聲明
支持與贊助 (Donate)
這是一個由普通上班族獨立維護的開源計畫。如果您覺得這個工具對您的投資有幫助，歡迎請我喝杯咖啡，給予我持續優化程式碼的動力！

👉 [點此透過方格子贊助支持本專案([https://vocus.cc/article/694f813afd8978000101e75a](https://vocus.cc/pay/donate/606146a3fd89780001ba32e9?donateSourceType=article&donateSourceRefID=694f88bdfd89780001042d74))

免責聲明 (Disclaimer)
本專案所提供的數據分析與 AI 診斷結果僅供參考，不構成投資建議。股市有風險，投資人應獨立審慎評估，並自負投資損益。


<img width="1790" height="816" alt="image" src="https://github.com/user-attachments/assets/3892f31b-e0bb-4b95-9f30-c1d32adf8184" />
<img width="1579" height="725" alt="image" src="https://github.com/user-attachments/assets/4b0d6fb0-1d80-4ab0-80ff-b6758021dd0b" />
<img width="587" height="830" alt="image" src="https://github.com/user-attachments/assets/360e6d49-d695-48b3-bef0-b33123c811a8" />
<img width="642" height="564" alt="image" src="https://github.com/user-attachments/assets/835c939e-fd1e-4585-841d-2b3d5b5b9292" />


## ⚙️ 環境變數設定
請在專案中設定以下變數（`.env` 或 Streamlit Secrets）：

```toml
# 核心大腦 (必填)
GEMINI_API_KEY = "您的_Gemini_API_Key"

# Google Drive 資料庫同步 (選配)
GDRIVE_SERVICE_ACCOUNT = "您的_Service_Account_JSON_內容"





