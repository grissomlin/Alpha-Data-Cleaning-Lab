import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# 1. 網頁基本設定
st.set_page_config(page_title="Alpha 全球強勢股監控站", layout="wide")
st.title("📊 全球股市漲停機率與資金流向")

# 2. 側邊欄配置
st.sidebar.header("配置與篩選")
market_option = st.sidebar.selectbox(
    "選擇市場",
    ("TW", "JP", "CN", "US", "HK", "KR")
)

# 連板次數篩選 (Seq_LU_Count)
min_seq = st.sidebar.slider("最小連板/連漲次數", 1, 10, 1)

# 3. Google Drive 下載邏輯
@st.cache_data(show_spinner=False)
def download_db_from_drive(db_name):
    """從 Google Drive 下載最新的資料庫檔案"""
    try:
        # 從 Streamlit Secrets 讀取 (請務必在雲端後台設定)
        info = json.loads(st.secrets["GDRIVE_SERVICE_ACCOUNT"])
        parent_id = st.secrets["PARENT_FOLDER_ID"]
        
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/drive'])
        service = build('drive', 'v3', credentials=creds)

        query = f"name = '{db_name}' and '{parent_id}' in parents"
        results = service.files().list(q=query).execute()
        items = results.get('files', [])

        if not items: return False

        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        
        with open(db_name, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
        return True
    except Exception as e:
        st.error(f"下載失敗: {e}")
        return False

# 4. 主執行邏輯
db_map = {
    "TW": "tw_stock_warehouse.db",
    "JP": "jp_stock_warehouse.db",
    "CN": "cn_stock_warehouse.db",
    "US": "us_stock_warehouse.db",
    "HK": "hk_stock_warehouse.db",
    "KR": "kr_stock_warehouse.db"
}
target_db = db_map[market_option]

# 自動下載/更新
if not os.path.exists(target_db):
    with st.spinner(f"正在同步 {market_option} 全球數據庫..."):
        success = download_db_from_drive(target_db)
else:
    success = True

if success:
    try:
        conn = sqlite3.connect(target_db)
        
        # SQL: 抓取最近 5 天數據 + 行業 + 連板資訊
        query = """
        SELECT p.日期, p.StockID, i.name as 股名, p.收盤, p.Ret_Day, 
               p.is_limit_up, p.Seq_LU_Count, i.sector as 行業,
               p.Next_1D_Max, p.Fwd_5D_Max
        FROM cleaned_daily_base p
        LEFT JOIN stock_info i ON p.StockID = i.symbol
        WHERE p.日期 >= (SELECT date(MAX(日期), '-5 day') FROM cleaned_daily_base)
        """
        df = pd.read_sql(query, conn)
        df['日期'] = pd.to_datetime(df['日期']).dt.date
        
        # 指標定義與提示
        unlimited_markets = ["US", "HK", "KR"]
        is_unlimited = market_option in unlimited_markets
        
        # --- 看板數據 ---
        total_samples = len(df)
        df_lu = df[(df['is_limit_up'] == 1) & (df['Seq_LU_Count'] >= min_seq)].copy()
        lu_count = len(df_lu)
        lu_ratio = (lu_count / total_samples) * 100 if total_samples > 0 else 0

        col1, col2, col3 = st.columns(3)
        col1.metric("5日總樣本 (家數*天)", f"{total_samples:,}")
        
        if is_unlimited:
            col2.metric(f"強勢股家數 (>10%)", f"{lu_count:,}")
            st.info(f"💡 **市場註記**: {market_option} 無漲跌幅限制，系統以 **單日漲幅 ≥ 10%** 且收紅K定義為強勢標的。")
        else:
            col2.metric("總漲停家數", f"{lu_count:,}")
            st.success(f"💡 **市場註記**: {market_option} 依據該國官方漲停板規則判定。")
            
        col3.metric("市場賺錢效應 (佔比)", f"{lu_ratio:.2f}%")

        # --- 圖表與明細 ---
        tab1, tab2 = st.tabs(["🔥 行業熱點分析", "📋 強勢股詳細名單"])
        
        with tab1:
            if not df_lu.empty:
                sector_stats = df_lu['行業'].value_counts().reset_index()
                sector_stats.columns = ['行業', '強勢個股次數']
                fig = px.bar(sector_stats, x='強勢個股次數', y='行業', orientation='h', 
                             color='強勢個股次數', color_continuous_scale='Viridis')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("當前篩選條件下無符合數據。")

        with tab2:
            st.subheader(f"最近 5 日 {market_option} 強勢股明細 (按日期排序)")
            if not df_lu.empty:
                display_df = df_lu.sort_values(by=['日期', 'Ret_Day'], ascending=False).head(100)
                # 格式化顯示
                display_df['漲幅'] = (display_df['Ret_Day'] * 100).round(2).astype(str) + '%'
                display_df['Next_1D'] = (display_df['Next_1D_Max'] * 100).round(2).astype(str) + '%'
                
                st.dataframe(
                    display_df[['日期', 'StockID', '股名', '行業', '收盤', '漲幅', 'Seq_LU_Count', 'Next_1D']],
                    column_config={
                        "Seq_LU_Count": "連板天數",
                        "Next_1D": "T+1最高預期"
                    },
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.write("目前無符合條件的個股。")

        conn.close()
    except Exception as e:
        st.error(f"讀取錯誤: {e}")
