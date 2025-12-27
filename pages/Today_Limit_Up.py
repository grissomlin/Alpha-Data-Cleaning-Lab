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

# 外部圖表連結模板
url_templates = {
    "TW": "https://www.wantgoo.com/stock/{s}/technical-chart",
    "US": "https://www.tradingview.com/symbols/{s}/",
    "JP": "https://jp.tradingview.com/symbols/TSE-{s}/",
    "CN": "https://panyi.eastmoney.com/pc_sc_kline.html?s={s}",
    "HK": "https://www.tradingview.com/symbols/HKEX-{s}/",
    "KR": "https://www.tradingview.com/symbols/KRX-{s}/"
}
current_url_base = url_templates.get(market_option, "https://google.com/search?q={s}")
target_db = db_map[market_option]

if not os.path.exists(target_db):
    st.error(f"❌ 找不到 {market_option} 資料庫檔案。")
    st.stop()

conn = sqlite3.connect(target_db)

try:
    # A. 獲取最新交易日
    latest_date = pd.read_sql("SELECT MAX(日期) FROM cleaned_daily_base", conn).iloc[0, 0]
    
    # B. 抓取當日漲停股票數據 (使用 LEFT JOIN 確保顯示公司名稱)
    query_today = f"""
    SELECT p.StockID, i.name as Name, i.sector as Sector, p.收盤, p.Ret_Day, p.Seq_LU_Count, p.is_limit_up
    FROM cleaned_daily_base p
    LEFT JOIN stock_info i ON p.StockID = i.symbol
    WHERE p.日期 = '{latest_date}' AND p.is_limit_up = 1
    ORDER BY p.Seq_LU_Count DESC, p.StockID ASC
    """
    df_today = pd.read_sql(query_today, conn)

    st.title(f"🚀 {market_option} 今日漲停戰情室")
    st.caption(f"📅 基準日：{latest_date} | 數據範圍：2023 至今")

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

        # --- 第二部分：個股深度分析 ---
        st.divider()
        df_today['select_label'] = df_today['StockID'] + " " + df_today['Name'].fillna("")
        selected_label = st.selectbox("🎯 請選擇要分析的漲停股：", options=df_today['select_label'].tolist())
        
        if selected_label:
            target_id = selected_label.split(" ")[0]
            stock_detail = df_today[df_today['StockID'] == target_id].iloc[0]

            # 🚀 修正後的聚合查詢：同時計算漲停、炸板與溢價期望值
            backtest_q = f"""
            SELECT 
                SUM(is_limit_up) as total_lu, 
                SUM(CASE WHEN is_limit_up = 0 AND Ret_High > failed_lu_threshold THEN 1 ELSE 0 END) as total_failed,
                AVG(CASE WHEN Prev_LU = 1 THEN Overnight_Alpha END) as avg_open,
                AVG(CASE WHEN Prev_LU = 1 THEN Next_1D_Max END) as avg_max
            FROM cleaned_daily_base 
            WHERE StockID = '{target_id}'
            """
            bt = pd.read_sql(backtest_q, conn).iloc[0]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("今日狀態", f"{stock_detail['Seq_LU_Count']} 連板")
            m2.metric("2023至今漲停", f"{int(bt['total_lu'] or 0)} 次")
            m3.metric("2023至今炸板", f"{int(bt['total_failed'] or 0)} 次", delta_color="inverse")
            m4.metric("隔日溢價期望", f"{(bt['avg_open'] or 0)*100:.2f}%")

            # 💡 同族群聯動 (加上公司名稱與超連結)
            current_sector = stock_detail['Sector']
            related_q = f"""
            SELECT p.StockID, i.name as Name, p.is_limit_up
            FROM cleaned_daily_base p
            LEFT JOIN stock_info i ON p.StockID = i.symbol
            WHERE i.sector = '{current_sector}' AND p.日期 = '{latest_date}' AND p.StockID != '{target_id}'
            LIMIT 12
            """
            df_related = pd.read_sql(related_q, conn)
            
            st.write(f"🌿 **同產業聯動參考 ({current_sector})：**")
            if not df_related.empty:
                links = []
                for _, r in df_related.iterrows():
                    pure_symbol = r['StockID'].split('.')[0]
                    link_url = current_url_base.replace("{s}", pure_symbol)
                    status_suffix = " 🔥" if r['is_limit_up'] == 1 else ""
                    # 🚀 加入公司名稱在超連結中
                    links.append(f"[{r['StockID']} {r['Name']}{status_suffix}]({link_url})")
                
                st.markdown(" ".join(links))
            else:
                st.caption("暫無同產業其他公司數據")

            # --- 第三部分：AI 診斷 (自動偵測模型) ---
            st.divider()
            if st.button(f"🤖 點擊讓 AI 診斷：{stock_detail['Name']}"):
                api_key = st.secrets.get("GEMINI_API_KEY")
                if not api_key:
                    st.warning("⚠️ 請設定 GEMINI_API_KEY")
                else:
                    try:
                        genai.configure(api_key=api_key)
                        all_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                        target_model = None
                        for candidate in ['models/gemini-1.5-pro', 'models/gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-1.5-flash']:
                            if candidate in all_models:
                                target_model = candidate
                                break
                        if not target_model: target_model = all_models[0]
                        
                        model = genai.GenerativeModel(target_model)
                        
                        prompt = f"""
                        你是專業短線交易員。請分析股票 {selected_label}：
                        - 市場：{market_option} | 產業：{current_sector}
                        - 今日狀態：連板第 {stock_detail['Seq_LU_Count']} 天
                        - 2023至今：漲停 {int(bt['total_lu'])} 次，衝板失敗(炸板) {int(bt['total_failed'])} 次。
                        - 隔日溢價期望：{(bt['avg_open'] or 0)*100:.2f}%
                        
                        分析重點：
                        1. 考慮到炸板次數與成功漲停的比例，該股籌碼是否穩定？
                        2. 同產業板塊目前的強勢程度與續航力。
                        3. 給予明日操作建議與風控。
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
