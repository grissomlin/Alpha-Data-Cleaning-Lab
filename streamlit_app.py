import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.generativeai as genai

# --- 1. 網頁基本配置 ---
st.set_page_config(page_title="Alpha 全球強勢股診斷站", layout="wide")

# --- 2. 側邊欄：市場切換 ---
st.sidebar.header("🌍 全球市場配置")
market_option = st.sidebar.selectbox(
    "選擇追蹤市場",
    ("TW", "JP", "CN", "US", "HK", "KR")
)

# --- 3. Google Drive 下載邏輯 ---
def download_db_from_drive(db_name):
    try:
        if "GDRIVE_SERVICE_ACCOUNT" not in st.secrets:
            st.error("Secrets 中找不到 GDRIVE_SERVICE_ACCOUNT")
            return False
            
        info = json.loads(st.secrets["GDRIVE_SERVICE_ACCOUNT"])
        parent_id = st.secrets["PARENT_FOLDER_ID"]
        
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)

        query = f"name = '{db_name}' and '{parent_id}' in parents"
        results = service.files().list(q=query).execute()
        items = results.get('files', [])

        if not items:
            all_files = service.files().list(q=f"'{parent_id}' in parents").execute().get('files', [])
            names = [f['name'] for f in all_files]
            st.error(f"❌ 找不到檔案: {db_name}")
            st.info(f"雲端資料夾內容: {names}")
            return False

        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        with open(db_name, 'wb') as f:
            f.write(fh.getvalue())
        return True
    except Exception as e:
        st.error(f"下載失敗: {str(e)}")
        return False

# --- 4. 資料庫同步與連線 ---
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
            status.update(label=f"✅ {market_option} 同步完成", state="complete", expanded=False)
        else:
            st.stop()

# 使用快取讀取股票清單
@st.cache_data
def get_stock_list(_db_path):
    conn_local = sqlite3.connect(_db_path)
    df = pd.read_sql("SELECT symbol, name FROM stock_info", conn_local)
    conn_local.close()
    return df

# --- 5. UI 主介面 ---
st.title(f"📊 {market_option} 市場強勢股看板")
tab1, tab2 = st.tabs(["🔥 市場熱度分析", "🤖 AI 個股診斷"])

# 分頁 1: 市場熱度
with tab1:
    conn = sqlite3.connect(target_db)
    try:
        q_dash = """
        SELECT p.日期, p.StockID, i.name as 股名, i.sector as 行業, p.收盤, p.is_limit_up, p.Seq_LU_Count, p.Ret_Day
        FROM cleaned_daily_base p
        LEFT JOIN stock_info i ON p.StockID = i.symbol
        WHERE p.日期 >= (SELECT date(MAX(日期), '-5 day') FROM cleaned_daily_base)
        """
        df_dash = pd.read_sql(q_dash, conn)
        df_dash['日期'] = pd.to_datetime(df_dash['日期']).dt.date
        lu_df = df_dash[df_dash['is_limit_up'] == 1]
        
        c1, c2, c3 = st.columns(3)
        c1.metric("5日總樣本", f"{len(df_dash):,}")
        c2.metric("強勢股家數", f"{len(lu_df):,}")
        c3.metric("市場熱度", f"{(len(lu_df)/len(df_dash)*100):.2f}%" if len(df_dash)>0 else "0%")

        if not lu_df.empty:
            fig = px.bar(lu_df['行業'].value_counts().reset_index(), x='count', y='行業', orientation='h', title="強勢行業排行")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(lu_df.sort_values('日期', ascending=False), hide_index=True)
    finally:
        conn.close()

# 分頁 2: AI 診斷
with tab2:
    st.subheader("🔍 個股大數據 AI 診斷")
    
    # 獲取搜尋清單
    try:
        stocks = get_stock_list(target_db)
        stocks['display'] = stocks['symbol'] + " " + stocks['name']
        selected_stock = st.selectbox("請輸入代碼或名稱搜尋", options=stocks['display'].tolist(), index=None, placeholder="例如: 2330 或 1")

        if selected_stock:
            target_symbol = selected_stock.split(" ")[0]
            conn = sqlite3.connect(target_db)
            diag_q = f"""
            SELECT COUNT(*) as total, SUM(is_limit_up) as lu, 
            AVG(CASE WHEN Prev_LU=1 THEN Overnight_Alpha END) as ov, 
            AVG(CASE WHEN Prev_LU=1 THEN Next_1D_Max END) as nxt 
            FROM cleaned_daily_base WHERE StockID = '{target_symbol}'
            """
            res = pd.read_sql(diag_q, conn).iloc[0]
            conn.close()
            
            if res['total'] > 0:
                st.write(f"### {selected_stock} 數據回測")
                m1, m2, m3 = st.columns(3)
                m1.metric("漲停/大漲次數", f"{int(res['lu'] or 0)} 次")
                m2.metric("隔日開盤溢價均值", f"{(res['ov'] or 0)*100:.2f}%")
                m3.metric("隔日最高期望值", f"{(res['nxt'] or 0)*100:.2f}%")
                
                # --- AI 分析按鈕 ---
                if st.button("🚀 啟動 AI 專家分析"):
                    if "GEMINI_API_KEY" in st.secrets:
                        try:
                            genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                            
                            # 自動偵測可用模型
                            available_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                            target_model_name = 'models/gemini-1.5-flash' if 'models/gemini-1.5-flash' in available_models else available_models[0]
                            
                            model = genai.GenerativeModel(target_model_name)
                            
                            prompt = f"""
                            你是一位量化交易專家，請針對股票 {selected_stock} 進行診斷：
                            1. 過去5年漲停次數：{res['lu']} 次
                            2. 漲停後隔日開盤溢價均值：{(res['ov'] or 0)*100:.2f}%
                            3. 漲停後隔日盤中最高價平均：{(res['nxt'] or 0)*100:.2f}%
                            請分析該股的慣性（如：是否容易開高走低、隔日沖勝率等）並給予操作建議。
                            """
                            
                            with st.spinner(f"AI 正在進行深度運算 (使用 {target_model_name})..."):
                                response = model.generate_content(prompt)
                                st.markdown("---")
                                st.markdown(f"### 🤖 AI 專家診斷報告\n{response.text}")
                        except Exception as ai_e:
                            st.error(f"AI 啟動失敗: {ai_e}")
                    else:
                        st.warning("請在 Secrets 中設定 GEMINI_API_KEY")
            else:
                st.warning("該個股數據不足。")
    except Exception as e:
        st.error(f"搜尋組件載入失敗: {e}")
