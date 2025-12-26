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
    .stDataFrame { border-radius: 10px; }
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

# 檢查檔案是否存在 (Colab 環境需掛載 Drive)
if not os.path.exists(target_db):
    st.error(f"❌ 找不到 {market_option} 資料庫檔案 ({target_db})，請確認路徑正確。")
    st.stop()

conn = sqlite3.connect(target_db)

try:
    # A. 獲取最新交易日
    latest_date = pd.read_sql("SELECT MAX(日期) FROM cleaned_daily_base", conn).iloc[0, 0]
    
    # B. 抓取當日漲停股票數據 (JOIN 資訊表以獲知名稱與產業)
    query_today = f"""
    SELECT p.StockID, i.name as Name, i.sector as Sector, p.收盤, p.Ret_Day, p.Seq_LU_Count, p.is_limit_up
    FROM cleaned_daily_base p
    LEFT JOIN stock_info i ON p.StockID = i.symbol
    WHERE p.日期 = '{latest_date}' AND p.is_limit_up = 1
    ORDER BY p.Seq_LU_Count DESC, p.StockID ASC
    """
    df_today = pd.read_sql(query_today, conn)

    st.title(f"🚀 {market_option} 今日漲停戰情室")
    st.caption(f"📅 數據基準日：{latest_date} | AI 模型：Gemini 1.5 Pro")

    if df_today.empty:
        st.warning(f"⚠️ {latest_date} 此交易日尚無漲停股票數據。")
    else:
        # --- 第一部分：產業分析概覽 ---
        st.divider()
        col1, col2 = st.columns([1.2, 1])
        
        with col1:
            st.subheader("📊 漲停產業別分佈")
            # 處理 Sector 為空的情況
            df_today['Sector'] = df_today['Sector'].fillna('未分類')
            sector_counts = df_today['Sector'].value_counts().reset_index()
            sector_counts.columns = ['產業別', '漲停家數']
            
            fig = px.bar(sector_counts, x='漲停家數', y='產業別', orientation='h', 
                         color='漲停家數', color_continuous_scale='Reds', text='漲停家數')
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, height=400, margin=dict(l=0, r=0, t=30, b=0))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("📋 今日強勢清單")
            display_df = df_today[['StockID', 'Name', 'Sector', 'Seq_LU_Count']].copy()
            display_df.columns = ['代碼', '名稱', '產業', '連板次數']
            st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

        # --- 第二部分：個股診斷與同族群對照 ---
        st.divider()
        st.subheader("🔍 個股深度回測與族群對照")
        
        df_today['select_label'] = df_today['StockID'] + " " + df_today['Name'].fillna("")
        selected_label = st.selectbox("🎯 請選擇要分析的漲停股：", options=df_today['select_label'].tolist())
        
        if selected_label:
            target_id = selected_label.split(" ")[0]
            stock_detail = df_today[df_today['StockID'] == target_id].iloc[0]

            # 1. 抓取歷史回測數據 (過去漲停後的表現)
            backtest_q = f"""
            SELECT COUNT(*) as total_lu, AVG(Overnight_Alpha) as avg_open, AVG(Next_1D_Max) as avg_max
            FROM cleaned_daily_base WHERE StockID = '{target_id}' AND Prev_LU = 1
            """
            bt = pd.read_sql(backtest_q, conn).iloc[0]

            # 顯示指標卡
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("今日狀態", f"{stock_detail['Seq_LU_Count']} 連板")
            m2.metric("歷史漲停次數", f"{int(bt['total_lu'] or 0)} 次")
            m3.metric("隔日溢價期望", f"{(bt['avg_open'] or 0)*100:.2f}%")
            m4.metric("隔日最高期望", f"{(bt['avg_max'] or 0)*100:.2f}%")

            # 2. 獲取同族群聯動數據 (關鍵新增：相關概念股)
            current_sector = stock_detail['Sector']
            related_q = f"""
            SELECT p.StockID, i.name as Name, p.is_limit_up
            FROM cleaned_daily_base p
            LEFT JOIN stock_info i ON p.StockID = i.symbol
            WHERE i.sector = '{current_sector}' 
              AND p.日期 = '{latest_date}' 
              AND p.StockID != '{target_id}'
            LIMIT 10
            """
            df_related = pd.read_sql(related_q, conn)
            
            # 建立相關清單字串
            related_stocks_str = "暫無同產業其他公司數據"
            if not df_related.empty:
                related_list = []
                for _, row in df_related.iterrows():
                    status = "(今日亦漲停)" if row['is_limit_up'] == 1 else ""
                    related_list.append(f"{row['StockID']} {row['Name']}{status}")
                related_stocks_str = "、".join(related_list)
            
            st.info(f"🌿 **同產業聯動參考：** {related_stocks_str}")

            # --- 第三部分：AI 深度診斷 ---
            if st.button(f"🤖 點擊讓 Gemini 診斷：{stock_detail['Name']}"):
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    st.warning("⚠️ 請在 Streamlit Secrets 中設定 GEMINI_API_KEY")
                else:
                    try:
                        genai.configure(api_key=api_key)
                        model = genai.GenerativeModel('gemini-1.5-pro') # 強制使用 Pro 版
                        
                        prompt = f"""
                        你是專業的短線動能交易分析師。請分析股票 {selected_label}：
                        - 市場：{market_option}
                        - 產業板塊：{stock_detail['Sector']}
                        - 今日表現：連板第 {stock_detail['Seq_LU_Count']} 天
                        - 歷史數據：該股過去漲停後，隔日開盤平均溢價為 {(bt['avg_open'] or 0)*100:.2f}%，最高點期望值為 {(bt['avg_max'] or 0)*100:.2f}%。
                        - 同族群今日表現：{related_stocks_str}
                        
                        請提供以下深度分析：
                        1. **題材判斷**：該公司核心題材是什麼？是否有熱點支撐？
                        2. **族群效應**：結合同產業其他公司的表現，判斷今天是「孤軍奮戰」還是「板塊集體爆發」？
                        3. **連動預測**：參考清單中哪些股票最可能與其產生「龍頭-補漲」關係？
                        4. **續航力評估**：給予明日續航力評分 (1-10分)，並說明操作策略（如：開高是否建議追價、防守位設定）。
                        """
                        
                        with st.spinner("AI 正在解析大數據與族群聯動性..."):
                            response = model.generate_content(prompt)
                            st.success(f"### 🤖 AI 診斷報告")
                            st.markdown(response.text)
                    except Exception as e:
                        st.error(f"AI 分析失敗: {e}")

except Exception as e:
    st.error(f"程式執行錯誤: {e}")
finally:
    conn.close()
