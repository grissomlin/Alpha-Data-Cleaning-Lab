import pandas as pd
import numpy as np

class AlphaCoreEngine:
    def __init__(self, conn, rules, market_abbr):
        """
        適應 main_pipeline.py 的參數結構
        conn: sqlite3 連線物件
        rules: MarketRuleRouter 實例
        market_abbr: 市場縮寫 (TW, US, JP...)
        """
        self.conn = conn
        self.rules = rules
        self.market_abbr = market_abbr
        self.df = None

    def refine_all(self):
        """
        執行完整清洗流程，對應 pipeline.run_process 內部的呼叫
        """
        # 1. 從資料庫讀取原始數據
        self.df = pd.read_sql("SELECT * FROM cleaned_daily_base", self.conn)
        
        if self.df.empty:
            return self.df
        
        # 2. 排序 (確保計算 Prev_Close 正確)
        self.df = self.df.sort_values(['StockID', '日期']).reset_index(drop=True)
        
        # 3. 套用漲停判定規則 (這會呼叫 MarketRuleRouter)
        self.df = self.rules.apply(self.df)
        
        # 4. 計算報酬率與溢價
        self.calculate_returns()
        
        # 5. 計算連板次數 (關鍵：歸零邏輯)
        self.calculate_sequence_counts()
        
        # 6. 計算風險指標
        self.calculate_risk_metrics()
        
        # 7. 將結果寫回資料庫 (根據一般 pipeline 邏輯，這裡回傳 df 讓外部寫入)
        return self.df

    def calculate_returns(self):
        self.df['Prev_Close'] = self.df.groupby('StockID')['收盤'].shift(1)
        self.df['Ret_Day'] = (self.df['收盤'] / self.df['Prev_Close']) - 1
        self.df['Overnight_Alpha'] = (self.df['開盤'] / self.df['Prev_Close']) - 1
        self.df['Next_1D_Max'] = (self.df['最高'] / self.df['Prev_Close']) - 1

    def calculate_sequence_counts(self):
        """
        修正連板次數：確保 is_limit_up 為 0 時強制歸零
        解決 ETF 1454 次連板問題
        """
        def get_sequence(series):
            # 建立區塊識別，狀態改變即增加
            blocks = (series != series.shift()).cumsum()
            # 區塊內計數
            cum_counts = series.groupby(blocks).cumcount() + 1
            # 當 is_limit_up 為 0，乘積即為 0
            return series * cum_counts

        self.df['Seq_LU_Count'] = self.df.groupby('StockID')['is_limit_up'].transform(get_sequence)

    def calculate_risk_metrics(self):
        # 20日波動率
        self.df['volatility_20d'] = self.df.groupby('StockID')['Ret_Day'].transform(
            lambda x: x.rolling(window=20).std() * (252**0.5)
        )
        # 20日最大回撤
        self.df['rolling_max_20d'] = self.df.groupby('StockID')['收盤'].transform(
            lambda x: x.rolling(window=20, min_periods=1).max()
        )
        self.df['drawdown_after_high_20d'] = (self.df['收盤'] / self.df['rolling_max_20d']) - 1
