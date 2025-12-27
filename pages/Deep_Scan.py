import streamlit as st
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import google.generativeai as genai
import os
import re

# 1. 頁面基本配置
st.set_page_config(page_title="AI 綜合個股深度掃描", layout="wide")

# 市場與資料庫對照表
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

# 外部技術圖表連結模板
url_templates = {
    "TW": "https://www.wantgoo.com/stock/{s}/technical-chart",
    "US": "https://www.tradingview.com/symbols/{s}/",
    "JP": "https://jp.tradingview.com/symbols/TSE-{s}/",
    "CN": "https://panyi.eastmoney.com/pc_sc_kline.html?s={s}",
    "HK": "https://www.tradingview.com/symbols/HKEX-{s}/",
    "KR": "https://www.tradingview.com/symbols/KRX-{s}/"
}
current_url_base = url_templates.get(market_option, "https://google.com/search?q={s}")

# 檢查資料庫是否存在
if not os.path.exists(target_db):
    st.error(f"請先回到首頁同步 {market_option} 數據庫")
    st.stop()

@st.cache_data
def get_full_stock_info(_db_path):
    conn = sqlite3.connect(_db_path)
    try:
        df = pd.read_sql("SELECT symbol, name, sector FROM stock_info", conn)
    except:
        df = pd.DataFrame(columns=['symbol', 'name', 'sector'])
    conn.close()
    return df

try:
    # 獲取股票清單
    stock_df = get_full_stock_info(target_db)
    stock_df['display'] = stock_df['symbol'] + " " + stock_df['name']
    
    st.title("🔍 AI 綜合個股深度掃描")
    selected = st.selectbox("請搜尋代碼或名稱 (例如 2330)", options=stock_df['display'].tolist(), index=None)

    if selected:
        target_symbol = selected.split(" ")[0]
        conn = sqlite3.connect(target_db)
        
        # A. 抓取最新指標數據
        scan_q = f"SELECT * FROM cleaned_daily_base WHERE StockID = '{target_symbol}' ORDER BY 日期 DESC LIMIT 1"
        data_all = pd.read_sql(scan_q, conn)
        
        # B. 歷史股性統計 (2023 至今)
        # 使用 Ret_High 判斷「盤中曾達漲停門檻但收盤未鎖住」的炸板行為
        hist_q = f"""
        SELECT COUNT(*) as t, SUM(is_limit_up) as lu, 
        SUM(CASE WHEN Prev_LU = 0 AND is_limit_up = 0 AND Ret_High > 0.095 THEN 1 ELSE 0 END) as failed_lu,
        AVG(CASE WHEN Prev_LU=1 THEN Overnight_Alpha END) as ov,
        AVG(CASE WHEN Prev_LU=1 THEN Next_1D_Max END) as nxt
        FROM cleaned_daily_base WHERE StockID = '{target_symbol}'
        """
        hist = pd.read_sql(hist_q, conn).iloc[0]

        # C. 獲取產業與同業資訊
        temp_info_q = f"SELECT sector FROM stock_info WHERE symbol = '{target_symbol}'"
        sector_res = pd.read_sql(temp_info_q, conn)
        sector_name = sector_res.iloc[0,0] if not sector_res.empty else "未知"
        
        peer_q = f"SELECT symbol, name FROM stock_info WHERE sector = '{sector_name}' AND symbol != '{target_symbol}' LIMIT 8"
        peers_df = pd.read_sql(peer_q, conn)
        conn.close()

        if not data_all.empty:
            data = data_all.iloc[0]
            st.divider()
            
            col_radar, col_stats = st.columns(2)
            
            # --- 左側：雷達圖繪製 ---
            with col_radar:
                st.subheader("📊 多維度體質評分")
                r5 = data.get('Ret_5D', 0) or 0
                r20 = data.get('Ret_20D', 0) or 0
                r200 = data.get('Ret_200D', 0) or 0
                vol = data.get('volatility_20d', 0) or 0
                dd = data.get('drawdown_after_high_20d', 0) or 0

                categories = ['短線動能', '中線動能', '長線動能', '抗震穩定度', '防禦力']
                # 評分標準化邏輯
                plot_values = [
                    min(max(r5 * 5 + 0.5, 0.1), 1),
                    min(max(r20 * 2 + 0.5, 0.1), 1),
                    min(max(r200 + 0.5, 0.1), 1),
                    max(1 - vol * 2, 0.1),
                    max(1 + dd, 0.1)
                ]
                
                fig = go.Figure(data=go.Scatterpolar(
                    r=plot_values,
                    theta=categories,
                    fill='toself',
                    name=selected,
                    line_color='#00d4ff'
                ))
                fig.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                    showlegend=False,
                    template="plotly_dark",
                    margin=dict(l=40, r=40, t=20, b=20)
                )
                st.plotly_chart(fig, use_container_width=True)
                
            # --- 右側：行為統計與指標 ---
            with col_stats:
                st.subheader("📋 股性統計 (2023~至今)")
                m1, m2 = st.columns(2)
                m1.metric("成功漲停次數", f"{int(hist['lu'] or 0)} 次")
                m2.metric("衝板失敗(炸板)", f"{int(hist['failed_lu'] or 0)} 次")
                
                st.write(f"**最新收盤價**：`{data['收盤']}`")
                st.write(f"**所屬產業**：`{sector_name}`")
                st.write(f"**20日波動率**：{vol*100:.2f}%")
                st.write(f"**漲停隔日溢價均值**：{(hist['ov'] or 0)*100:.2f}%")
                
                if not peers_df.empty:
                    st.write("**🔗 同產業參考**：")
                    links = [f"[{row['symbol']}]({current_url_base.replace('{s}', row['symbol'].split('.')[0])})" for _, row in peers_df.iterrows()]
                    st.caption(" ".join(links))

            # --- 下方：AI 深度診斷按鈕 (修正 404 問題) ---
            st.divider()
            if st.button("🚀 詢問 AI 專家對該股的深度判斷"):
                if "GEMINI_API_KEY" in st.secrets:
                    try:
                        genai.configure(api_key=st.secrets["GEMINI_API_KEY"])
                        
                        # 🚀 修正點：使用不帶 latest 後綴的標準模型名稱，提高相容性
                        model = genai.GenerativeModel('gemini-1.5-flash')
                        
                        prompt = f"""
                        你是資深量化交易員。請針對股票 {selected} 進行深度診斷：
                        數據表現：
                        - 2023至今成功漲停：{int(hist['lu'])} 次
                        - 2023至今炸板次數：{int(hist['failed_lu'])} 次
                        - 漲停隔日溢價期望值：{(hist['ov'] or 0)*100:.2f}%
                        - 20日波動率：{vol*100:.2f}%
                        
                        請結合「炸板次數」分析該股的籌碼壓力，判斷是否具備強勢股基因，並給予短線風控建議。
                        """
                        
                        with st.spinner("AI 正在解析數據並生成報告..."):
                            response = model.generate_content(prompt)
                            st.info("### 🤖 AI 專家診斷報告")
                            st.markdown(response.text)
                    except Exception as e:
                        # 備援方案：若 1.5-flash 失敗，嘗試 gemini-pro
                        try:
                            model = genai.GenerativeModel('gemini-pro')
                            response = model.generate_content(prompt)
                            st.info("### 🤖 AI 專家診斷報告 (使用備援模型)")
                            st.markdown(response.text)
                        except Exception as e2:
                            st.error(f"AI 服務暫時不可用，請檢查 API Key 或稍後再試。錯誤: {e2}")
                else:
                    st.warning("請在 Streamlit Secrets 中設定 GEMINI_API_KEY")
            
except Exception as e:
    st.error(f"系統異常: {e}")
