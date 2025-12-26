import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import google.generativeai as genai
import os

# --- 1. 頁面配置與樣式 ---
st.set_page_config(page_title="全球漲停板 AI 分析儀", layout="wide")
st.markdown("""
    <style>
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; border: 1px solid #f0f2f6; box-shadow: 2px 2px 5px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- 2. 市場資料庫配置 ---
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
    st.error(f"❌ 找不到 {market_option} 資料庫檔案。")
    st.stop()

conn = sqlite3.connect(target_db)

try:
    # A. 獲取最新交易日
    latest_date = pd.read_sql("SELECT MAX(日期) FROM cleaned_daily_base", conn).iloc[0, 0]
    
    # B. 抓取當日漲停股票數據
    query_today = f"""
    SELECT p.StockID, i.name as Name, i.sector as Sector, p.收盤, p.Ret_Day, p.Seq_LU_Count, p.is_limit_up
    FROM cleaned_daily_base p
    LEFT JOIN stock_info i ON p.StockID = i.symbol
    WHERE p.日期 = '{latest_date}' AND p.is_limit_up = 1
    ORDER BY p.Seq_LU_Count DESC, p.StockID ASC
    """
    df_today = pd.read_sql(query_today, conn)

    st.title(f"🚀 {market_option} 今日漲停戰情室")
    st.caption(f"📅 基準日：{latest_date} | AI 分析助手：Gemini 1.5 系列")

    if df_today.empty:
        st.warning(f"⚠️ {latest_date} 此交易日尚無漲停股票數據。")
    else:
        # --- 第一部分：產業分析 ---
        st.divider()
        col1, col2 = st.columns([1.2, 1])
        with col1:
            st.subheader("📊 漲停產業別分佈")
            df_today['Sector'] = df_today['Sector'].fillna('未分類')
            sector_counts = df_today['Sector'].value_counts().reset_index()
            sector_counts.columns = ['產業別', '漲停家數']
            fig = px.bar(sector_counts, x='漲停家數', y='產業別', orientation='h', color='漲停家數', color_continuous_scale='Reds')
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("📋 今日強勢清單")
            st.dataframe(df_today[['StockID', 'Name', 'Sector', 'Seq_LU_Count']], use_container_width=True, hide_index=True)

        # --- 第二部分：個股與族群 ---
        st.divider()
        df_today['select_label'] = df_today['StockID'] + " " + df_today['Name'].fillna("")
        selected_label = st.selectbox("🎯 請選擇要分析的漲停股：", options=df_today['select_label'].tolist())
        
        if selected_label:
            target_id = selected_label.split(" ")[0]
            stock_detail = df_today[df_today['StockID'] == target_id].iloc[0]

            # 抓取回測數據
            backtest_q = f"SELECT COUNT(*) as total_lu, AVG(Overnight_Alpha) as avg_open, AVG(Next_1D_Max) as avg_max FROM cleaned_daily_base WHERE StockID = '{target_id}' AND Prev_LU = 1"
            bt = pd.read_sql(backtest_q, conn).iloc[0]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("今日狀態", f"{stock_detail['Seq_LU_Count']} 連板")
            m2.metric("歷史漲停次數", f"{int(bt['total_lu'] or 0)} 次")
            m3.metric("隔日溢價期望", f"{(bt['avg_open'] or 0)*100:.2f}%")
            m4.metric("隔日最高期望", f"{(bt['avg_max'] or 0)*100:.2f}%")

            # 💡 獲取同族群聯動數據 (定義 related_stocks_str)
            current_sector = stock_detail['Sector']
            related_q = f"""
            SELECT p.StockID, i.name as Name, p.is_limit_up
            FROM cleaned_daily_base p
            LEFT JOIN stock_info i ON p.StockID = i.symbol
            WHERE i.sector = '{current_sector}' AND p.日期 = '{latest_date}' AND p.StockID != '{target_id}'
            LIMIT 10
            """
            df_related = pd.read_sql(related_q, conn)
            related_stocks_str = "暫無同產業其他公司數據"
            if not df_related.empty:
                related_list = [f"{r['StockID']} {r['Name']}{'(亦漲停)' if r['is_limit_up']==1 else ''}" for _, r in df_related.iterrows()]
                related_stocks_str = "、".join(related_list)
            
            st.info(f"🌿 **同產業聯動參考：** {related_stocks_str}")

            # --- 第三部分：AI 深度診斷 (自動偵測模型邏輯) ---
            if st.button(f"🤖 點擊讓 AI 診斷：{stock_detail['Name']}"):
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    st.warning("⚠️ 請設定 GEMINI_API_KEY")
                else:
                    try:
                        genai.configure(api_key=api_key)
                        
                        # 核心修復：自動列出可用的模型並挑選
                        all_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                        # 優先順序：1.5-pro -> 1.5-flash -> 第一個可用的
                        target_model = None
                        for candidate in ['models/gemini-1.5-pro', 'gemini-1.5-pro', 'models/gemini-1.5-flash', 'gemini-1.5-flash']:
                            if candidate in all_models:
                                target_model = candidate
                                break
                        if not target_model: target_model = all_models[0]
                        
                        model = genai.GenerativeModel(target_model)
                        
                        prompt = f"""
                        你是專業短線交易員。請分析股票 {selected_label}：
                        - 市場：{market_option} | 產業：{stock_detail['Sector']}
                        - 今日表現：連板第 {stock_detail['Seq_LU_Count']} 天
                        - 歷史數據：隔日開盤平均溢價為 {(bt['avg_open'] or 0)*100:.2f}%。
                        - 同族群今日表現：{related_stocks_str}
                        
                        分析重點：
                        1. 核心題材為何？
                        2. 是「個股消息」還是「族群效應」？
                        3. 同產業參考清單中誰最有聯動性？
                        4. 給予明日續航力評分(1-10分)與操作建議。
                        """
                        
                        with st.spinner(f"AI 正在解析 (模型: {target_model})..."):
                            response = model.generate_content(prompt)
                            st.success(f"### 🤖 AI 診斷報告")
                            st.markdown(response.text)
                    except Exception as e:
                        st.error(f"AI 分析失敗: {e}")

except Exception as e:
    st.error(f"錯誤: {e}")
finally:
    conn.close()
