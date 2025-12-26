import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import os

# 1. 頁面配置
st.set_page_config(page_title="風險指標深度掃描", layout="wide")

# 2. 超連結函數
def get_market_link(symbol, market):
    if market == "TW": return f"https://tw.stock.yahoo.com/quote/{symbol}"
    elif market == "US": return f"https://finviz.com/quote.ashx?t={symbol}"
    else: return f"https://www.tradingview.com/symbols/{symbol}"

# 3. 讀取資料庫
market_option = st.sidebar.selectbox("🚩 選擇市場", ("TW", "JP", "CN", "US", "HK", "KR"), key="risk_market")
db_map = {"TW":"tw_stock_warehouse.db", "JP":"jp_stock_warehouse.db", "CN":"cn_stock_warehouse.db", 
          "US":"us_stock_warehouse.db", "HK":"hk_stock_warehouse.db", "KR":"kr_stock_warehouse.db"}
target_db = db_map[market_option]

if not os.path.exists(target_db):
    st.error(f"請先回到主頁面同步 {market_option} 資料庫")
    st.stop()

conn = sqlite3.connect(target_db)

try:
    # 抓取風險相關欄位
    query = """
    SELECT StockID, 日期, 
           (SELECT name FROM stock_info WHERE symbol = StockID) as Name,
           (SELECT sector FROM stock_info WHERE symbol = StockID) as Sector,
           volatility_10d, volatility_20d, volatility_50d,
           drawdown_after_high_10d, drawdown_after_high_20d, drawdown_after_high_50d,
           recovery_from_dd_10d, [月累计漲跌幅(本月开盘)] as Ret_M
    FROM cleaned_daily_base
    WHERE 日期 = (SELECT MAX(日期) FROM cleaned_daily_base)
    """
    df = pd.read_sql(query, conn)
    
    st.title(f"🛡️ {market_option} 市場風險與穩定度分析")
    st.info("本頁面專注於『防禦性指標』，分析強勢股在拉回時的韌性。")

    # --- 區塊一：回撤與恢復力分布 ---
    st.subheader("📉 最大回撤分布 (Max Drawdown)")
    c1, c2, c3 = st.columns(3)
    
    with c1:
        fig1 = px.histogram(df, x='drawdown_after_high_10d', title="10D 回撤分布", color_discrete_sequence=['#ff4b4b'])
        st.plotly_chart(fig1, use_container_width=True)
    with c2:
        fig2 = px.histogram(df, x='drawdown_after_high_20d', title="20D 回撤分布", color_discrete_sequence=['#ff4b4b'])
        st.plotly_chart(fig2, use_container_width=True)
    with c3:
        # 散佈圖：分析『月漲幅』與『回撤』的關係
        fig3 = px.scatter(df, x='Ret_M', y='drawdown_after_high_20d', color='volatility_20d',
                         title="報酬 vs. 回撤 (顏色為波動率)", hover_name='Name')
        st.plotly_chart(fig3, use_container_width=True)

    # --- 區塊二：風險分箱排行榜 ---
    st.divider()
    col_l, col_r = st.columns(2)

    with col_l:
        st.subheader("🔥 高波動警戒區 (Volatility Top 20)")
        # 波動率越大，風險越高
        high_vol = df.sort_values('volatility_20d', ascending=False).head(20)
        st.dataframe(high_vol[['StockID', 'Name', 'volatility_20d', 'Ret_M']], use_container_width=True, hide_index=True)

    with col_r:
        st.subheader("🧱 抗跌韌性區 (Low Drawdown & Positive Return)")
        # 篩選月漲幅為正，且 20D 回撤極小的股票 (代表一路上漲沒回頭)
        resilient = df[(df['Ret_M'] > 0.05) & (df['drawdown_after_high_20d'] > -0.05)].sort_values('Ret_M', ascending=False).head(20)
        st.dataframe(resilient[['StockID', 'Name', 'Ret_M', 'drawdown_after_high_20d']], use_container_width=True, hide_index=True)

    # --- 區塊三：行業風險分析 ---
    st.divider()
    st.subheader("🏘️ 行業平均波動與回撤")
    sector_risk = df.groupby('Sector')[['volatility_20d', 'drawdown_after_high_20d']].mean().reset_index()
    fig_sec = px.bar(sector_risk, x='Sector', y='volatility_20d', color='drawdown_after_high_20d',
                    title="各行業平均波動率 (顏色深淺代表平均回撤幅度)")
    st.plotly_chart(fig_sec, use_container_width=True)

    # --- 區塊四：搜尋個股風險診斷 ---
    st.divider()
    st.subheader("🔍 個股風險深度診斷")
    selected = st.selectbox("選擇股票查看風險歷程", options=(df['StockID'] + " " + df['Name']).tolist())
    if selected:
        sid = selected.split(" ")[0]
        # 這裡可以加入該股過去 20 天的波動與回撤曲線
        st.write(f"已選取 {selected}，連結至：[外部分析圖表]({get_market_link(sid, market_option)})")
        risk_data = df[df['StockID'] == sid].iloc[0]
        st.write(f"該股當前 20D 波動率為 `{risk_data['volatility_20d']*100:.2f}%`，20D 最大回撤為 `{risk_data['drawdown_after_high_20d']*100:.2f}%`。")

except Exception as e:
    st.error(f"風險指標加載失敗: {e}")

finally:
    conn.close()
