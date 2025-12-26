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
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; border: 1px solid #f0f2f6; }
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
    
    # B. 抓取當日漲停股票數據
    query_today = f"""
    SELECT p.StockID, i.name as Name, i.sector as Sector, p.收盤, p.Ret_Day, p.Seq_LU_Count
    FROM cleaned_daily_base p
    LEFT JOIN stock_info i ON p.StockID = i.symbol
    WHERE p.日期 = '{latest_date}' AND p.is_limit_up = 1
    ORDER BY p.Seq_LU_Count DESC, p.StockID ASC
    """
    df_today = pd.read_sql(query_today, conn)

    st.title(f"🚀 {market_option} 今日漲停戰情室")
    st.caption(f"📅 數據基準日：{latest_date}")

    if df_today.empty:
        st.warning("⚠️ 此交易日尚無漲停股票數據。")
    else:
        # --- 第一部分：產業分析 ---
        st.divider()
        col1, col2 = st.columns([1.2, 1])
        
        with col1:
            st.subheader("📊 漲停產業別分佈")
            sector_counts = df_today['Sector'].value_counts().reset_index()
            sector_counts.columns = ['產業別', '漲停家數']
            fig = px.bar(sector_counts, x='漲停家數', y='產業別', orientation='h', 
                         color='漲停家數', color_continuous_scale='Reds', text='漲停家數')
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=400)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("📋 今日強勢清單")
            display_df = df_today[['StockID', 'Name', 'Sector', 'Seq_LU_Count']].copy()
            display_df.columns = ['代碼', '名稱', '產業', '連板次數']
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

        # --- 第二部分：個股診斷 ---
        st.divider()
        st.subheader("🔍 今日漲停股回測統計")
        
        df_today['select_label'] = df_today['StockID'] + " " + df_today['Name']
        selected_label = st.selectbox("請選擇今日漲停股：", options=df_today['select_label'].tolist())
        
        if selected_label:
            target_id = selected_label.split(" ")[0]
            stock_detail = df_today[df_today['StockID'] == target_id].iloc[0]

            # 抓取回測數據
            backtest_q = f"""
            SELECT COUNT(*) as total_lu, AVG(Overnight_Alpha) as avg_open, AVG(Next_1D_Max) as avg_max
            FROM cleaned_daily_base WHERE StockID = '{target_id}' AND Prev_LU = 1
            """
            bt = pd.read_sql(backtest_q, conn).iloc[0]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("今日狀態", f"{stock_detail['Seq_LU_Count']} 連板")
            m2.metric("5年漲停次數", f"{int(bt['total_lu'] or 0)} 次")
            m3.metric("隔日溢價期望", f"{(bt['avg_open'] or 0)*100:.2f}%")
            m4.metric("最高價期望", f"{(bt['avg_max'] or 0)*100:.2f}%")

            # 近 5 日明細
            history_q = f"SELECT 日期, 收盤, ROUND(Ret_Day*100,2) as '漲跌%', is_limit_up FROM cleaned_daily_base WHERE StockID = '{target_id}' AND 日期 <= '{latest_date}' ORDER BY 日期 DESC LIMIT 5"
            st.table(pd.read_sql(history_q, conn))

            # --- 第三部分：AI 深度診斷 (已修復 404 錯誤) ---
            if st.button(f"🤖 詢問 AI：為何 {stock_detail['Name']} 會漲停？"):
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    st.warning("請在 Secrets 設定 GEMINI_API_KEY")
                else:
                    try:
                        genai.configure(api_key=api_key)
                        # 自動偵測模型
                        available = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                        target_model = next((c for c in ['models/gemini-1.5-flash', 'gemini-1.5-flash', 'models/gemini-pro'] if c in available), available[0])
                        
                        model = genai.GenerativeModel(target_model)
                        prompt = f"""
                        分析股票 {selected_label}：
                        - 產業板塊：{stock_detail['Sector']}
                        - 今日表現：第 {stock_detail['Seq_LU_Count']} 天漲停
                        - 歷史數據：漲停次數 {bt['total_lu']} 次，隔日開盤平均溢價 {(bt['avg_open'] or 0)*100:.2f}%
                        - 同產業/概念股參考：{related_stocks_str}
                        
                        請針對以下重點進行分析：
                        1. 該公司的核心概念股題材。
                        2. 今日漲停的原因（結合同族群表現判斷是個股因素還是族群集體噴發）。
                        3. 列出上述參考清單中，哪些公司具備聯動性。
                        4. 評估明日的續航力與操作策略。
                        """                 
                        with st.spinner(f"AI 解析中 (使用 {target_model})..."):
                            response = model.generate_content(prompt)
                            st.info(f"### 🤖 AI 診斷結果")
                            st.markdown(response.text)
                    except Exception as e:
                        st.error(f"AI 分析失敗: {e}")

except Exception as e:
    st.error(f"載入失敗: {e}")
finally:
    conn.close()
