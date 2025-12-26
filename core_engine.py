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
        執行精煉任務並輸出執行日誌
        """
        # 1. 讀取數據
        print(f"--- 🚀 啟動 {self.market_abbr} 精煉任務 ---")
        self.df = pd.read_sql("SELECT * FROM cleaned_daily_base", self.conn)
        
        if self.df.empty:
            msg = f"❌ Market {self.market_abbr}: 資料表 cleaned_daily_base 是空的，跳過計算。"
            print(msg)
            return msg

        print(f"📈 讀取成功：共 {len(self.df)} 筆原始數據")

        # 2. 排序與套用市場規則 (判定 is_limit_up)
        self.df = self.df.sort_values(['StockID', '日期']).reset_index(drop=True)
        self.df = self.rules.apply(self.df)
        print(f"⚖️ 市場規則套用完成，目前漲停標記總數: {self.df['is_limit_up'].sum()}")
        
        # 3. 核心計算 (連板歸零邏輯就在這裡)
        print("🧮 正在計算報酬率、連板次數與風險指標...")
        self.calculate_returns()
        self.calculate_sequence_counts() 
        self.calculate_risk_metrics()
        
        # 4. 寫回資料庫
        print(f"💾 正在將精煉數據寫回 {self.market_abbr} 資料庫...")
        self.df.to_sql("cleaned_daily_base", self.conn, if_exists="replace", index=False)
        
        # 5. 構建總結訊息
        limit_up_total = int(self.df['is_limit_up'].sum())
        max_seq = int(self.df['Seq_LU_Count'].max())
        
        summary_text = (
            f"✅ {self.market_abbr} 精煉完成！\n"
            f"📊 總筆數: {len(self.df)}\n"
            f"📈 漲停總數: {limit_up_total}\n"
            f"🚀 最大連板: {max_seq}\n"
        )
        print(summary_text)
        return summary_text

    def calculate_returns(self):
        # 確保基準是昨日收盤
        self.df['Prev_Close'] = self.df.groupby('StockID')['收盤'].shift(1)
        self.df['Ret_Day'] = (self.df['收盤'] / self.df['Prev_Close']) - 1
        self.df['Overnight_Alpha'] = (self.df['開盤'] / self.df['Prev_Close']) - 1
        self.df['Next_1D_Max'] = (self.df['最高'] / self.df['Prev_Close']) - 1

    def calculate_sequence_counts(self):
        def get_sequence(series):
            blocks = (series != series.shift()).cumsum()
            cum_counts = series.groupby(blocks).cumcount() + 1
            return series * cum_counts
        self.df['Seq_LU_Count'] = self.df.groupby('StockID')['is_limit_up'].transform(get_sequence)

    def calculate_risk_metrics(self):
        # 20日波動率與回撤
        self.df['volatility_20d'] = self.df.groupby('StockID')['Ret_Day'].transform(
            lambda x: x.rolling(window=20).std() * (252**0.5)
        )
        self.df['rolling_max_20d'] = self.df.groupby('StockID')['收盤'].transform(
            lambda x: x.rolling(window=20, min_periods=1).max()
        )
        self.df['drawdown_after_high_20d'] = (self.df['收盤'] / self.df['rolling_max_20d']) - 1
