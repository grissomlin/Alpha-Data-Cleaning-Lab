import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os  # <--- 確保這行存在，修復 NameError
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.generativeai as genai

# 1. 網頁基本設定
st.set_page_config(page_title="Alpha 全球強勢股診斷站", layout="wide")

# 2. 側邊欄配置 - 市場切換（這是所有數據的根源）
st.sidebar.header("⚙️ 全球市場配置")
market_option = st.sidebar.selectbox(
    "選擇追蹤市場",
    ("TW", "JP", "CN", "US", "HK", "KR")
)

# 3. Google Drive 下載函數 (加上 cache 避免重複下載)
@st.cache_data(show_spinner=False)
def download_db_from_drive(db_name):
    try:
        info = json.loads(st.secrets["GDRIVE_SERVICE_ACCOUNT"])
        parent_id = st.secrets["PARENT_FOLDER_ID"]
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)
        query = f"name = '{db_name}' and '{parent_id}' in parents"
        results = service.files().list(q=query).execute()
        items = results.get('files', [])
        if not items: return False
        request = service.files().get_media(fileId=items[0]['id'])
        with open(db_name, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()
        return True
    except: return False

# 4. 資料庫加載邏輯
db_map = {
    "TW": "tw_stock_warehouse.db",
    "JP": "jp_stock_warehouse.db",
    "CN": "cn_stock_warehouse.db",
    "US": "us_stock_warehouse.db",
    "HK": "hk_stock_warehouse.db",
    "KR": "kr_stock_warehouse.db"
}
target_db = db_map[market_option]

if not os.path.exists(target_db):
    with st.status(f"🚀 同步 {market_option} 資料庫...", expanded=True) as status:
        if download_db_from_drive(target_db):
            status.update(label="✅ 同步完成", state="complete", expanded=False)
        else:
            st.error("下載失敗，請檢查資料夾 ID 與權限")
            st.stop()

# 5. 建立資料庫連線與搜尋清單
conn = sqlite3.connect(target_db)

@st.cache_data
def get_stock_list(_conn):
    return pd.read_sql("SELECT symbol, name FROM stock_info", _conn)

try:
    stock_df = get_stock_list(conn)
    stock_df['display'] = stock_df['symbol'] + " " + stock_df['name']
except:
    stock_df = pd.DataFrame(columns=['symbol', 'name', 'display'])

# 6. UI 介面設計 (分頁)
st.title(f"📊 {market_option} 市場強勢股看板")
tab1, tab2 = st.tabs(["🔥 市場熱度分析", "🤖 AI 個股診斷"])

with tab1:
    # 這裡放你原本的 5 日統計數據、行業排行榜與強勢清單
    query = f"SELECT p.*, i.name as 股名, i.sector as 行業 FROM cleaned_daily_base p LEFT JOIN stock_info i ON p.StockID = i.symbol WHERE p.日期 >= (SELECT date(MAX(日期), '-5 day') FROM cleaned_daily_base)"
    df_dashboard = pd.read_sql(query, conn)
    
    col1, col2 = st.columns([1, 2])
    with col1:
        st.metric("5日樣本數", len(df_dashboard))
        lu_df = df_dashboard[df_dashboard['is_limit_up'] == 1]
        st.metric("強勢股家數", len(lu_df))
    with col2:
        if not lu_df.empty:
            fig = px.bar(lu_df['行業'].value_counts().reset_index(), x='count', y='行業', orientation='h', title="熱門行業排行榜")
            st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("🔍 AI 專家診斷系統")
    
    # 實現你要求的：輸入 1 出現清單的功能
    selected_stock = st.selectbox(
        "搜尋股票 (輸入代碼或名稱)",
        options=stock_df['display'].tolist(),
        index=None,
        placeholder="請輸入... 例如 2330 或 1"
    )

    if selected_stock:
        target_symbol = selected_stock.split(" ")[0]
        st.write(f"正在分析: **{selected_stock}**")
        
        # 撈取個股統計數據
        diag_q = f"SELECT COUNT(*) as total, SUM(is_limit_up) as lu, AVG(CASE WHEN Prev_LU=1 THEN Overnight_Alpha END) as ov, AVG(CASE WHEN Prev_LU=1 THEN Next_1D_Max END) as nxt FROM cleaned_daily_base WHERE StockID = '{target_symbol}'"
        res = pd.read_sql(diag_q, conn).iloc[0]
        
        if res['total'] > 0:
            c1, c2, c3 = st.columns(3)
            c1.metric("歷史漲停次數", f"{int(res['lu'] or 0)} 次")
            c2.metric("隔日平均溢價", f"{(res['ov'] or 0)*100:.2f}%")
            c3.metric("隔日最高期望", f"{(res['nxt'] or 0)*100:.2f}%")
            
            # AI 分析邏輯
            if st.button("🚀 啟動 Gemini AI 深度分析"):
                if "GEMINI_API_KEY" in st.secrets:
                    genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                    model = genai.GenerativeModel('gemini-1.5-flash')
                    prompt = f"分析股票{target_symbol}：五年漲停{res['lu']}次，隔日開盤溢價{(res['ov'] or 0)*100:.2f}%，隔日最高均值{(res['nxt'] or 0)*100:.2f}%。請評價操作風險。"
                    with st.spinner("AI 正在思考..."):
                        response = model.generate_content(prompt)
                        st.info(response.text)
                else:
                    st.warning("請先在 Secrets 中設定 GEMINI_API_KEY")

conn.close()
