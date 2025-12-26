import pandas as pd
import numpy as np

class AlphaCoreEngine:
    def __init__(self, conn, rules, market_abbr):
        self.conn = conn
        self.rules = rules
        self.market_abbr = market_abbr
        self.df = None

    def execute(self):
        """
        執行精煉任務，產出週期分析所需的所有欄位
        """
        # 1. 讀取數據
        self.df = pd.read_sql("SELECT * FROM cleaned_daily_base", self.conn)
        
        if self.df.empty:
            return f"Market {self.market_abbr}: No data found."

        # 2. 排序 (日期必須正確排序)
        self.df = self.df.sort_values(['StockID', '日期']).reset_index(drop=True)
        self.df['日期'] = pd.to_datetime(self.df['日期'])
        
        # 3. 套用漲停判定規則 (來自 market_rules.py)
        self.df = self.rules.apply(self.df)
        
        # 4. 執行核心計算
        self.calculate_returns()           # 基礎報酬
        self.calculate_rolling_returns()    # 5D, 20D, 200D 滾動報酬
        self.calculate_period_returns()     # 周、月、年累計 (修正報錯關鍵)
        self.calculate_sequence_counts()    # 連板計數
        self.calculate_risk_metrics()       # 波動率與回撤
        
        # 5. 寫回資料庫
        self.df.to_sql("cleaned_daily_base", self.conn, if_exists="replace", index=False)
        
        summary_text = (
            f"✅ {self.market_abbr} 精煉完成！\n"
            f"📊 總筆數: {len(self.df)}\n"
            f"📈 漲停總數: {int(self.df['is_limit_up'].sum())}\n"
        )
        return summary_text

    def calculate_returns(self):
        self.df['Prev_Close'] = self.df.groupby('StockID')['收盤'].shift(1)
        self.df['Ret_Day'] = (self.df['收盤'] / self.df['Prev_Close']) - 1
        self.df['Overnight_Alpha'] = (self.df['開盤'] / self.df['Prev_Close']) - 1
        self.df['Next_1D_Max'] = (self.df['最高'] / self.df['Prev_Close']) - 1

    def calculate_rolling_returns(self):
        """
        計算 5D, 20D, 200D 滾動報酬
        """
        for days in [5, 20, 200]:
            col_name = f'Ret_{days}D'
            self.df[col_name] = self.df.groupby('StockID')['收盤'].transform(
                lambda x: x / x.shift(days) - 1
            )

    def calculate_period_returns(self):
        """
        計算周、月、年累計漲跌幅 (對應 Period_Analysis 的需求)
        """
        # 確保日期格式正確
        dt = self.df['日期']
        
        # 建立週期分組 (週、月、年)
        self.df['week_grp'] = dt.dt.to_period('W').astype(str)
        self.df['month_grp'] = dt.dt.to_period('M').astype(str)
        self.df['year_grp'] = dt.dt.year.astype(str)

        # 計算週期累計：(今日收盤 / 該週期第一天收盤) - 1
        def get_cum_ret(group_col):
            first_closes = self.df.groupby(['StockID', group_col])['收盤'].transform('first')
            return (self.df['收盤'] / first_closes) - 1

        self.df['周累计漲跌幅(本周开盘)'] = get_cum_ret('week_grp')
        self.df['月累计漲跌幅(本月开盘)'] = get_cum_ret('month_grp')
        self.df['年累計漲跌幅(本年开盘)'] = get_cum_ret('year_grp')

        # 移除暫時的輔助欄位
        self.df.drop(['week_grp', 'month_grp', 'year_grp'], axis=1, inplace=True)

    def calculate_sequence_counts(self):
        def get_sequence(series):
            blocks = (series != series.shift()).cumsum()
            return series * (series.groupby(blocks).cumcount() + 1)
        self.df['Seq_LU_Count'] = self.df.groupby('StockID')['is_limit_up'].transform(get_sequence)

    def calculate_risk_metrics(self):
        self.df['volatility_20d'] = self.df.groupby('StockID')['Ret_Day'].transform(
            lambda x: x.rolling(window=20).std() * (252**0.5)
        )
        self.df['rolling_max_20d'] = self.df.groupby('StockID')['收盤'].transform(
            lambda x: x.rolling(window=20, min_periods=1).max()
        )
        self.df['drawdown_after_high_20d'] = (self.df['收盤'] / self.df['rolling_max_20d']) - 1
