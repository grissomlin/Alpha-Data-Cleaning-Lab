# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sqlite3

class AlphaCoreEngine:
    def __init__(self, conn, rules, market_abbr):
        self.conn = conn
        self.rules = rules
        self.market_abbr = market_abbr.upper()
        self.df = None

    def execute(self):
        print(f"--- 🚀 啟動 {self.market_abbr} 數據精煉 (興櫃數據回歸版) ---")
        
        # 1. 直接從原始股價表 (stock_prices) 讀取原料，確保看到最新日期
        query = """
            SELECT date as 日期, symbol as StockID, open as 開盤, 
                   high as 最高, low as 最低, close as 收盤, volume as 成交量
            FROM stock_prices 
            WHERE date >= '2023-01-01'
        """
        
        try:
            self.df = pd.read_sql(query, self.conn)
            if self.df.empty:
                print(f"❌ {self.market_abbr} 原始表 stock_prices 無數據。")
                return "Error: No raw data found"
        except Exception as e:
            print(f"⚠️ 讀取原始數據失敗: {e}")
            return f"Error: {e}"

        print(f"📊 讀入原始數據量: {len(self.df)} 筆。")

        # 2. 基礎預處理
        self.df = self.df.sort_values(['StockID', '日期']).reset_index(drop=True)
        self.df['日期'] = pd.to_datetime(self.df['日期'])
        
        # 3. 整合市場別資訊 (用於興櫃判定)
        try:
            info_df = pd.read_sql("SELECT symbol as StockID, market as MarketType, name as stock_name FROM stock_info", self.conn)
            self.df = pd.merge(self.df, info_df, on='StockID', how='left')
        except Exception as e:
            print(f"⚠️ 無法獲取市場資訊: {e}")
            self.df['MarketType'] = 'Unknown'
            self.df['stock_name'] = 'Unknown'

        # 4. 套用市場規則 (上市櫃 10% 判定)
        self.df = self.rules.apply(self.df)
        
        # 5. 💡 興櫃補強邏輯：找回消失的 10% 紅棒 (必須在計算 sequence 之前)
        self._apply_market_type_adjustments()

        # 6. 計算各項技術指標與報酬率
        self.calculate_returns()
        self.calculate_rolling_returns()
        self.calculate_period_returns()
        self.calculate_sequence_counts()
        self.calculate_risk_metrics_extended()
        
        # 7. 數據清洗與輸出格式化
        self.df['日期'] = self.df['日期'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 8. 覆蓋寫入加工表
        print(f"💾 正在更新加工表 cleaned_daily_base (共 {len(self.df)} 筆)...")
        self.df.to_sql("cleaned_daily_base", self.conn, if_exists="replace", index=False)
        
        # 9. 維護資料庫效能
        print("🧹 執行資料庫效能優化 (VACUUM & INDEX)...")
        try:
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_date ON cleaned_daily_base (StockID, 日期)")
            self.conn.execute("VACUUM")
        except:
            pass
        
        max_date = self.df['日期'].max()
        return f"✅ {self.market_abbr} 精煉完成！最新日期：{max_date}"

    # --- 核心邏輯增強 ---

    def _apply_market_type_adjustments(self):
        """
        🚀 興櫃補強：找回消失的 10% 強勢標的
        """
        if 'MarketType' not in self.df.columns:
            return

        # 計算兩種漲幅：對昨收(強度) 與 對今開(實體氣勢)
        prev_close = self.df.groupby('StockID')['收盤'].shift(1)
        ret_vs_prev = (self.df['收盤'] / prev_close) - 1
        ret_intraday = (self.df['收盤'] / self.df['開盤']) - 1 

        # 判定興櫃標的 (同時檢查標籤與 .TWO 後綴)
        is_rotc = (self.df['MarketType'].isin(['興櫃', 'ROTC'])) | (self.df['StockID'].str.endswith('.TWO'))
        
        # 門檻設為 9.8% (0.098) 避免浮點數精準度漏掉 10.0% 的股票
        is_strong = (ret_vs_prev >= 0.098) | (ret_intraday >= 0.098)
        
        # 寫入專屬標記
        self.df['is_rotc_strong'] = (is_rotc & is_strong).astype(int)
        
        # 強制入庫至漲停標籤，讓篩選器能抓到
        self.df.loc[is_rotc & is_strong, 'is_limit_up'] = 1
        
        print(f"📊 興櫃處理：已標註 {(is_rotc & is_strong).sum()} 筆 10% 以上強勢事件 (含實體紅棒)。")

    def calculate_returns(self):
        """計算基礎報酬率"""
        self.df['Prev_Close'] = self.df.groupby('StockID')['收盤'].shift(1)
        self.df['Ret_Day'] = (self.df['收盤'] / self.df['Prev_Close']) - 1
        self.df['Overnight_Alpha'] = (self.df['開盤'] / self.df['Prev_Close']) - 1
        self.df['Ret_High'] = (self.df['最高'] / self.df['Prev_Close']) - 1
        self.df['Next_1D_Max'] = self.df['Ret_High']
        
        if 'is_limit_up' in self.df.columns:
            self.df['Prev_LU'] = self.df.groupby('StockID')['is_limit_up'].shift(1).fillna(0)

    def calculate_rolling_returns(self):
        """計算 5D, 20D, 200D 滾動報酬"""
        for d in [5, 20, 200]:
            self.df[f'Ret_{d}D'] = self.df.groupby('StockID')['收盤'].transform(lambda x: x / x.shift(d) - 1)

    def calculate_period_returns(self):
        """計算定錨週期報酬 (週、月、年)"""
        temp_dt = pd.to_datetime(self.df['日期'])
        week_first = self.df.groupby(['StockID', temp_dt.dt.to_period('W')])['收盤'].transform('first')
        self.df['周累计漲跌幅(本周开盘)'] = (self.df['收盤'] / week_first) - 1
        
        month_first = self.df.groupby(['StockID', temp_dt.dt.to_period('M')])['收盤'].transform('first')
        self.df['月累计漲跌幅(本月开盘)'] = (self.df['收盤'] / month_first) - 1
        
        year_first = self.df.groupby(['StockID', temp_dt.dt.year])['收盤'].transform('first')
        self.df['年累計漲跌幅(本年开盘)'] = (self.df['收盤'] / year_first) - 1

    def calculate_sequence_counts(self):
        """計算連續漲停天數"""
        def get_sequence(series):
            blocks = (series != series.shift()).cumsum()
            return series * (series.groupby(blocks).cumcount() + 1)
        self.df['Seq_LU_Count'] = self.df.groupby('StockID')['is_limit_up'].transform(get_sequence)

    def calculate_risk_metrics_extended(self):
        """計算波動率與回檔指標"""
        for d in [10, 20, 50]:
            self.df[f'volatility_{d}d'] = self.df.groupby('StockID')['Ret_Day'].transform(
                lambda x: x.rolling(d).std() * (252**0.5)
            )
            rolling_max = self.df.groupby('StockID')['收盤'].transform(lambda x: x.rolling(d, min_periods=1).max())
            self.df[f'drawdown_after_high_{d}d'] = (self.df['收盤'] / rolling_max) - 1
            
        rolling_min_10d = self.df.groupby('StockID')['收盤'].transform(lambda x: x.rolling(10, min_periods=1).min())
        self.df['recovery_from_dd_10d'] = (self.df['收盤'] / rolling_min_10d) - 1
