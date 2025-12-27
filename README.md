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

## 📖 快速上手與教學

為了讓您能更順利地架設並使用本系統，建議參考以下在方格子（vocus）上的圖文教學：

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

## ⚙️ 環境變數設定

請在專案中設定以下變數（`.env` 或 Streamlit Secrets）：

```toml
# 核心大腦 (必填)
GEMINI_API_KEY = "您的_Gemini_API_Key"

# Google Drive 資料庫同步 (選配)
GDRIVE_SERVICE_ACCOUNT = "您的_Service_Account_JSON_內容"
