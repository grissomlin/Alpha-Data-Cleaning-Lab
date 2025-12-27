import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import google.generativeai as genai
import os

# 1. 頁面配置
st.set_page_config(page_title="長周期與滾動漲跌分析", layout="wide")

# 2. 共用函數：取得市場專屬超連結
def get_market_link(symbol, market):
    if market == "TW":
        return f"https://tw.stock.yahoo.com/quote/{symbol}"
    elif market == "US":
        return f"https://finviz.com/quote.ashx?t={symbol}"
    elif market == "JP":
        return f"https://minkabu.jp/stock/{symbol.split('.')[0]}"
    elif market == "HK":
        return f"http://www.aastocks.com/tc/stocks/analysis/stock-quote.ashx?stockid={symbol.split('.')[0]}"
    else:
        return f"https://www.tradingview.com/symbols/{symbol}"

# 3. 讀取資料庫
market_option = st.sidebar.selectbox("🚩 選擇市場", ("TW", "JP", "CN", "US", "HK", "KR"), key="period_market")
db_map = {"TW":"tw_stock_warehouse.db", "JP":"jp_stock_warehouse.db", "CN":"cn_stock_warehouse.db", 
          "US":"us_stock_warehouse.db", "HK":"hk_stock_warehouse.db", "KR":"kr_stock_warehouse.db"}
target_db = db_map[market_option]

if not os.path.exists(target_db):
    st.error(f"請先回到主頁面同步 {market_option} 資料庫")
    st.stop()

conn = sqlite3.connect(target_db)

# 4. 抓取最新日期的統計數據
try:
    query = """
    SELECT StockID, 日期, Ret_Day, 
           (SELECT name FROM stock_info WHERE symbol = StockID) as Name,
           [周累计漲跌幅(本周开盘)] as Ret_W,
           [月累计漲跌幅(本月开盘)] as Ret_M,
           [年累計漲跌幅(本年开盘)] as Ret_Y,
           Ret_5D, Ret_20D, Ret_200D,
           volatility_20d, drawdown_after_high_20d
    FROM cleaned_daily_base
    WHERE 日期 = (SELECT MAX(日期) FROM cleaned_daily_base)
    """
    df = pd.read_sql(query, conn)
    
    st.title(f"🚀 {market_option} 長周期動能儀表板")
    st.caption(f"數據基準日: {df['日期'].iloc[0] if not df.empty else 'N/A'}")

    # --- 九宮格圖表 (3x3) ---
    st.subheader("📊 滾動與日曆周期分布")
    
    metrics = [
        ('Ret_5D', '滾動 5D'), ('Ret_20D', '滾動 20D'), ('Ret_200D', '滾動 200D'),
        ('Ret_W', '本周 (W)'), ('Ret_M', '本月 (M)'), ('Ret_Y', '本年 (Y)'),
        ('volatility_20d', '20D 波動率'), ('drawdown_after_high_20d', '20D 回撤'), ('Ret_Day', '今日漲跌')
    ]

    rows = [st.columns(3) for _ in range(3)]
    for idx, (col_name, label) in enumerate(metrics):
        with rows[idx//3][idx%3]:
            if col_name in df.columns:
                fig = px.histogram(df, x=col_name, title=f"{label} 分布", 
                                   nbins=50, color_discrete_sequence=['#3366ff'])
                fig.update_layout(margin=dict(l=20, r=20, t=40, b=20), height=250)
                st.plotly_chart(fig, use_container_width=True)

    # --- 分箱清單 (Binning) ---
    st.divider()
    st.subheader("📦 強勢分箱清單 (本月累計)")
    
    bins = [-float('inf'), -0.1, -0.05, 0, 0.05, 0.1, 0.2, float('inf')]
    labels = ["慘跌(<-10%)", "回檔(-10%~-5%)", "平盤(-5%~0%)", "轉強(0~5%)", "強勢(5~10%)", "噴發(10~20%)", "妖股(>20%)"]
    df['Bin'] = pd.cut(df['Ret_M'], bins=bins, labels=labels)

    bin_tabs = st.tabs(labels[::-1]) # 從強到弱排列
    for i, label in enumerate(labels[::-1]):
        with bin_tabs[i]:
            subset = df[df['Bin'] == label][['StockID', 'Name', 'Ret_M', 'drawdown_after_high_20d']]
            if not subset.empty:
                subset['連結'] = subset['StockID'].apply(lambda x: get_market_link(x, market_option))
                st.dataframe(
                    subset.sort_values('Ret_M', ascending=False),
                    column_config={"連結": st.column_config.LinkColumn("外部連結")},
                    use_container_width=True, hide_index=True
                )
            else:
                st.write("目前無符合條件的股票")

    # --- 5. AI 週期動能診斷 (新增功能) ---
    st.divider()
    if st.button(f"🤖 啟動 {market_option} 市場週期動能 AI 診斷"):
        api_key = st.secrets.get("GEMINI_API_KEY")
        if not api_key:
            st.warning("⚠️ 請先在 Streamlit Secrets 中設定 GEMINI_API_KEY")
        else:
            try:
                genai.configure(api_key=api_key)
                all_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                target_model = next((m for m in ['models/gemini-1.5-flash', 'gemini-1.5-flash'] if m in all_models), all_models[0])
                model = genai.GenerativeModel(target_model)
                
                # 準備市場分佈摘要給 AI
                bin_summary = df['Bin'].value_counts().to_string()
                avg_ret_5d = df['Ret_5D'].mean() * 100
                avg_ret_20d = df['Ret_20D'].mean() * 100
                
                prompt = f"""你是一位資深量化分析師。請分析 {market_option} 市場目前的週期動能分佈：
市場分佈摘要 (本月累積漲跌幅分箱)：
{bin_summary}

額外指標：
- 滾動 5 日平均漲跌幅：{avg_ret_5d:.2f}%
- 滾動 20 日平均漲跌幅：{avg_ret_20d:.2f}%

請根據以上數據：
1. 判斷目前市場處於「過熱」、「健康」還是「低迷」狀態？
2. 針對「妖股」與「噴發」箱體內的個股，給予目前的風險評估。
3. 給予短中線的操作建議。"""
                
                with st.spinner(f"AI 正在解析市場動能 (模型: {target_model})..."):
                    response = model.generate_content(prompt)
                    st.info("### 🤖 市場週期動能 AI 診斷報告")
                    st.markdown(response.text)
                    
                    # 提問詞複製區
                    st.divider()
                    st.subheader("📋 複製提問詞 (至 ChatGPT / Claude)")
                    st.caption("您可以複製下方指令，並將數據提供給其他 AI 進行交叉驗證：")
                    st.code(prompt.strip(), language="text")
            except Exception as e:
                st.error(f"AI 分析失敗: {e}")

except Exception as e:
    st.error(f"圖表生成失敗: {e}")
    st.info("請檢查資料庫欄位是否包含 Ret_5D, Ret_20D 等滾動數據。")

finally:
    conn.close()

# --- 6. 底部快速連結 (Footer) ---
st.divider()
st.markdown("### 🔗 快速資源連結")
col_link1, col_link2, col_link3 = st.columns(3)
with col_link1:
    st.page_link("https://vocus.cc/article/694f813afd8978000101e75a", label="⚙️ 環境與 AI 設定教學", icon="🛠️")
with col_link2:
    st.page_link("https://vocus.cc/article/694f88bdfd89780001042d74", label="📖 儀表板功能詳解", icon="📊")
with col_link3:
    st.page_link("https://github.com/grissomlin/Alpha-Data-Cleaning-Lab", label="💻 GitHub 專案原始碼", icon="🐙")
