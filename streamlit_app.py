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
    st.caption(f"📅 基準日：{latest_date} | AI 助手：Gemini 1.5 Pro (含超連結導航)")

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
            m4.metric("最高期望", f"{(bt['avg_max'] or 0)*100:.2f}%")

            # 💡 獲取同族群聯動數據
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

            # --- 第三部分：AI 深度診斷 (強效超連結指令版) ---
            if st.button(f"🤖 點擊讓 AI 診斷：{stock_detail['Name']}"):
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    st.warning("⚠️ 請設定 GEMINI_API_KEY")
                else:
                    try:
                        # 定義連結範本
                        url_templates = {
                            "TW": "https://www.wantgoo.com/stock/{s}/technical-chart",
                            "US": "https://www.tradingview.com/symbols/{s}/",
                            "JP": "https://jp.tradingview.com/symbols/TSE-{s}/",
                            "CN": "https://panyi.eastmoney.com/pc_sc_kline.html?s={s}",
                            "HK": "https://www.tradingview.com/symbols/HKEX-{s}/",
                            "KR": "https://www.tradingview.com/symbols/KRX-{s}/"
                        }
                        current_url_base = url_templates.get(market_option, "https://www.google.com/search?q={s}")
                        clean_target_id = target_id.split('.')[0]

                        genai.configure(api_key=api_key)
                        all_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                        target_model = next((c for c in ['models/gemini-1.5-pro', 'models/gemini-1.5-flash'] if c in all_models), all_models[0])
                        
                        model = genai.GenerativeModel(target_model)
                        
                        # 強效 Prompt：使用絕對命令與範例
                        prompt = f"""
                        你是專業短線交易員，現在要分析股票：{selected_label}。
                        
                        🚨 **絕對指令：Markdown 連結規範** 🚨
                        你必須在回覆中，將出現的「所有股票代號與名稱」轉換成 Markdown 連結。
                        格式必須嚴格遵守：[代號 名稱](連結網址)
                        
                        針對 {selected_label} 的範例連結：[{clean_target_id} {stock_detail['Name']}]({current_url_base.format(s=clean_target_id)})
                        
                        請將此連結邏輯應用到回覆中的「每一家」公司，包含同產業參考清單。
                        ---
                        數據背景：
                        - 市場：{market_option} | 產業：{stock_detail['Sector']}
                        - 今日狀態：連板第 {stock_detail['Seq_LU_Count']} 天
                        - 歷史隔日溢價期望：{(bt['avg_open'] or 0)*100:.2f}%
                        - 同族群今日狀態：{related_stocks_str}

                        分析任務：
                        1. **核心題材**：該公司為何漲停？
                        2. **族群效應**：是「個股利多」還是「板塊集體噴發」（注意標記「亦漲停」的股票）？
                        3. **連動預測**：清單中誰最有聯動性？(務必附上連結)
                        4. **續航力與建議**：明日續航力評分(1-10)與操作策略。
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
