import streamlit as st
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import google.generativeai as genai
import os

# 1. 頁面配置
st.set_page_config(page_title="AI 綜合個股深度掃描", layout="wide")

# 2. 側邊欄與資料庫連線
market_option = st.sidebar.selectbox("🚩 選擇市場", ("TW", "JP", "CN", "US", "HK", "KR"), key="scan_market")
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
    st.error(f"請先回到首頁同步 {market_option} 數據庫")
    st.stop()

# 3. 核心數據讀取
@st.cache_data
def get_full_stock_info(_db_path):
    conn = sqlite3.connect(_db_path)
    try:
        df = pd.read_sql("SELECT symbol, name FROM stock_info", conn)
    except:
        df = pd.DataFrame(columns=['symbol', 'name'])
    conn.close()
    return df

try:
    stock_df = get_full_stock_info(target_db)
    stock_df['display'] = stock_df['symbol'] + " " + stock_df['name']
    
    st.title("🔍 AI 綜合個股深度掃描")
    st.write("本模組整合 **動能、風險、隔日沖妖性** 三大維度，提供全方位回測。")

    selected = st.selectbox("請搜尋代碼或名稱 (例如輸入 1101 或 台泥)", options=stock_df['display'].tolist(), index=None)

    if selected:
        target_symbol = selected.split(" ")[0]
        conn = sqlite3.connect(target_db)
        
        # 抓取該股最新一筆所有資料
        scan_q = f"SELECT * FROM cleaned_daily_base WHERE StockID = '{target_symbol}' ORDER BY 日期 DESC LIMIT 1"
        data_all = pd.read_sql(scan_q, conn)
        
        # 抓取歷史隔日沖統計 (五年)
        hist_q = f"""
        SELECT COUNT(*) as t, SUM(is_limit_up) as lu, 
        AVG(CASE WHEN Prev_LU=1 THEN Overnight_Alpha END) as ov,
        AVG(CASE WHEN Prev_LU=1 THEN Next_1D_Max END) as nxt
        FROM cleaned_daily_base WHERE StockID = '{target_symbol}'
        """
        hist = pd.read_sql(hist_q, conn).iloc[0]

        # 抓取隔日沖樣本數據 (用於計算勝率與分布)
        sample_q = f"SELECT Overnight_Alpha, Next_1D_Max FROM cleaned_daily_base WHERE StockID = '{target_symbol}' AND Prev_LU = 1"
        samples = pd.read_sql(sample_q, conn)
        conn.close()

        if not data_all.empty:
            data = data_all.iloc[0]
            cols = data.index.tolist()

            def get_val(names):
                for n in names:
                    if n in cols: return data[n]
                return 0

            # 基礎指標
            r5 = get_val(['Ret_5D', 'Ret_5d', '5日漲跌幅'])
            r20 = get_val(['Ret_20D', 'Ret_20d', '20日漲跌幅'])
            r200 = get_val(['Ret_200D', 'Ret_200d', '200日漲跌幅'])
            vol = get_val(['volatility_20d', 'vol_20', '20日波動率'])
            dd = get_val(['drawdown_after_high_20d', 'dd_20', '20日回撤'])
            curr_price = get_val(['收盤', 'Close', 'price'])

            # --- 佈局一：雷達圖與核心指標 ---
            st.divider()
            col_left, col_right = st.columns([1, 1])
            
            with col_left:
                st.subheader("📊 多維度體質評分")
                categories = ['短線動能', '中線動能', '長線動能', '穩定度', '防禦力']
                plot_values = [
                    min(max(r5 * 5 + 0.5, 0.1), 1),
                    min(max(r20 * 2 + 0.5, 0.1), 1),
                    min(max(r200 + 0.5, 0.1), 1),
                    max(1 - vol * 2, 0.1),
                    max(1 + dd, 0.1)
                ]
                fig = go.Figure(data=go.Scatterpolar(r=plot_values, theta=categories, fill='toself', name=selected))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])), showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col_right:
                st.subheader("📋 當前關鍵指標")
                st.write(f"**最新日期**：{data['日期']}")
                st.write(f"**收盤價格**：{curr_price}")
                st.write(f"**20D 波動率**：{vol*100:.2f}%")
                st.write(f"**20D 最大回撤**：{dd*100:.2f}%")
                st.write(f"**5年漲停次數**：{int(hist['lu'] or 0)} 次")
                st.write(f"**平均溢價期望**：{(hist['ov'] or 0)*100:.2f}%")

            # --- 佈局二：⚡ 隔日沖專項數據 ---
            st.divider()
            st.subheader("⚡ 隔日沖慣性回測 (五年樣本)")
            
            if hist['lu'] > 0:
                win_count = len(samples[samples['Overnight_Alpha'] > 0])
                win_rate = (win_count / hist['lu'] * 100)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("隔日開紅機率 (勝率)", f"{win_rate:.1f}%")
                c2.metric("開盤獲利均值", f"{(samples['Overnight_Alpha'].mean()*100):.2f}%")
                c3.metric("盤中最高期望值", f"{(samples['Next_1D_Max'].mean()*100):.2f}%")
                
                # 分布圖
                fig_hist = px.histogram(
                    samples, x=samples['Overnight_Alpha']*100, 
                    nbins=15, title="隔日開盤利潤分布 (%)",
                    labels={'x': '利潤 %', 'count': '次數'},
                    color_discrete_sequence=['#FFD700']
                )
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("該個股過去五年無漲停紀錄，暫無隔日沖數據。")

            # --- 佈局三：歷史明細與 AI 報告 ---
            st.divider()
            with st.expander("📅 查看 5 年內漲停/大漲詳細日期"):
                detail_q = f"SELECT 日期, 收盤, ROUND(Ret_Day*100,2) as '漲幅%', ROUND(Overnight_Alpha*100,2) as '隔日溢價%' FROM cleaned_daily_base WHERE StockID = '{target_symbol}' AND is_limit_up = 1 ORDER BY 日期 DESC"
                st.dataframe(pd.read_sql(detail_q, sqlite3.connect(target_db)), use_container_width=True, hide_index=True)

            if st.button("🚀 生成 AI 專家深度診斷報告"):
                if "GEMINI_API_KEY" in st.secrets:
                    try:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        model = genai.GenerativeModel('gemini-1.5-flash')
                        prompt = f"""
                        分析股票 {selected}：
                        - 20D波動率/回撤：{vol*100:.1f}% / {dd*100:.1f}%
                        - 5年漲停次數：{hist['lu']}
                        - 隔日沖勝率：{win_rate if hist['lu']>0 else 0:.1f}%
                        - 隔日開盤溢價均值：{(hist['ov'] or 0)*100:.2f}%
                        請評估該股是否適合『隔日沖交易』，並分析其漲停後的慣性。
                        """
                        with st.spinner("AI 正在解析歷史妖性..."):
                            response = model.generate_content(prompt)
                            st.markdown(f"### 🤖 AI 診斷報告\n{response.text}")
                    except Exception as e:
                        st.error(f"AI 分析失敗: {e}")
                else:
                    st.warning("請先設定 GEMINI_API_KEY")

except Exception as e:
    st.error(f"模組載入失敗: {e}")
