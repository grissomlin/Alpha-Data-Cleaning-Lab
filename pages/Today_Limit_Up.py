import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import google.generativeai as genai
import os

# --- 1. 頁面配置與樣式 ---
st.set_page_config(page_title="今日漲停與產業熱度分析", layout="wide")
st.markdown("""
    <style>
    .main { background-color: #fafafa; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 資料庫連線與市場選擇 ---
market_option = st.sidebar.selectbox("🚩 選擇分析市場", ("TW", "JP", "CN", "US", "HK", "KR"), key="today_market")
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
    st.error(f"找不到 {market_option} 資料庫，請先確保數據已同步。")
    st.stop()

conn = sqlite3.connect(target_db)

try:
    # A. 自動獲取最新交易日
    latest_date = pd.read_sql("SELECT MAX(日期) FROM cleaned_daily_base", conn).iloc[0, 0]
    
    # B. 抓取當日漲停股票數據 (包含連板數 Seq_LU_Count)
    query_today = f"""
    SELECT p.StockID, i.name as Name, i.sector as Sector, p.收盤, p.Ret_Day, p.Seq_LU_Count
    FROM cleaned_daily_base p
    LEFT JOIN stock_info i ON p.StockID = i.symbol
    WHERE p.日期 = '{latest_date}' AND p.is_limit_up = 1
    ORDER BY p.Seq_LU_Count DESC, p.StockID ASC
    """
    df_today = pd.read_sql(query_today, conn)

    # --- 頁面標題 ---
    st.title(f"🚀 {market_option} 今日漲停戰情室")
    st.caption(f"數據基準日：{latest_date} (自動抓取最後一筆交易日)")

    if df_today.empty:
        st.warning("⚠️ 此交易日尚無漲停股票數據。")
    else:
        # --- 第一部分：產業別統計與家數 ---
        st.divider()
        col1, col2 = st.columns([1.2, 1])
        
        with col1:
            st.subheader("📊 漲停產業別分佈")
            sector_counts = df_today['Sector'].value_counts().reset_index()
            sector_counts.columns = ['產業別', '漲停家數']
            
            fig = px.bar(sector_counts, x='漲停家數', y='產業別', orientation='h', 
                         color='漲停家數', color_continuous_scale='Reds',
                         text='漲停家數')
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("📋 今日強勢榜")
            # 整理顯示表格
            display_df = df_today[['StockID', 'Name', 'Sector', 'Seq_LU_Count']].copy()
            display_df.columns = ['代碼', '名稱', '產業', '連板次數']
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

        # --- 第二部分：個股深入診斷 (下拉選單 + 歷史回測) ---
        st.divider()
        st.subheader("🔍 個股妖性回測與隔日沖統計")
        
        # 下拉選單：今日漲停名單
        df_today['select_label'] = df_today['StockID'] + " " + df_today['Name'] + " (" + df_today['Seq_LU_Count'].astype(str) + "連板)"
        selected_label = st.selectbox("請選擇一檔今日漲停股進行 AI 深度分析：", options=df_today['select_label'].tolist())
        
        if selected_label:
            target_id = selected_label.split(" ")[0]
            stock_detail = df_today[df_today['StockID'] == target_id].iloc[0]

            # 抓取該股過去 5 天的詳細數據 (用於顯示近期走勢)
            history_5d_q = f"""
            SELECT 日期, 收盤, Ret_Day, is_limit_up, Overnight_Alpha 
            FROM cleaned_daily_base 
            WHERE StockID = '{target_id}' AND 日期 <= '{latest_date}'
            ORDER BY 日期 DESC LIMIT 5
            """
            df_5d = pd.read_sql(history_5d_q, conn)

            # 抓取該股長期 (5年) 漲停後的表現
            backtest_q = f"""
            SELECT 
                COUNT(*) as total_lu,
                AVG(Overnight_Alpha) as avg_open,
                AVG(Next_1D_Max) as avg_max
            FROM cleaned_daily_base 
            WHERE StockID = '{target_id}' AND Prev_LU = 1
            """
            bt = pd.read_sql(backtest_q, conn).iloc[0]

            # 顯示看板
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("今日狀態", f"{stock_detail['Seq_LU_Count']} 連板")
            m2.metric("歷史漲停次數", f"{int(bt['total_lu'] or 0)} 次")
            m3.metric("隔日開盤期望值", f"{(bt['avg_open'] or 0)*100:.2f}%")
            m4.metric("隔日最高期望值", f"{(bt['avg_max'] or 0)*100:.2f}%")

            # 最近 5 天列表 (包含隔日沖參考)
            st.write("**📅 近 5 日交易表現：**")
            st.table(df_5d.rename(columns={'Ret_Day': '漲跌幅', 'is_limit_up': '是否漲停', 'Overnight_Alpha': '隔日溢價'}))

            # --- 第三部分：AI 一鍵分析按鈕 ---
            st.divider()
            if st.button(f"🤖 詢問 AI：{stock_detail['Name']} 為何漲停？屬於什麼概念股？"):
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    st.warning("請在 .streamlit/secrets.toml 中設定 GEMINI_API_KEY 才能啟用 AI 功能。")
                else:
                    try:
                        genai.configure(api_key=api_key)
                        model = genai.GenerativeModel('gemini-1.5-flash')
                        
                        prompt = f"""
                        你是一位專業的股市分析師，請針對以下資訊進行深度分析：
                        股票名稱：{stock_detail['Name']} ({target_id})
                        所屬產業：{stock_detail['Sector']}
                        今日表現：第 {stock_detail['Seq_LU_Count']} 天連板漲停
                        歷史隔日溢價期望值：{(bt['avg_open'] or 0)*100:.2f}%
                        
                        請提供：
                        1. **概念股分類**：這檔股票屬於哪些市場熱門題材（例如：AI伺服器、低軌衛星、政策題材等）？
                        2. **漲停原因解析**：結合當前產業趨勢，分析其漲停的可能原因。
                        3. **操作建議**：根據其連板數與歷史表現，明天的續航力如何？應注意哪些風險？
                        """
                        
                        with st.spinner(f"正在分析 {stock_detail['Name']} 的市場地位..."):
                            response = model.generate_content(prompt)
                            st.info("### 🤖 AI 診斷結果")
                            st.markdown(response.text)
                    except Exception as e:
                        st.error(f"AI 呼叫失敗: {e}")

except Exception as e:
    st.error(f"頁面載入錯誤: {e}")
finally:
    conn.close()
