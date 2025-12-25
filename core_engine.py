# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

class AlphaCoreEngine:
    def __init__(self, conn, rules, market_abbr):
        self.conn = conn
        self.rules = rules
        self.market_abbr = market_abbr

    def execute(self):
        # 1. 讀取與排序
        df = pd.read_sql("SELECT * FROM daily_prices", self.conn)
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values(['StockID', '日期']).reset_index(drop=True)

        # 2. 清洗與基礎指標
        df = self._clean_data(df)
        df = self._calculate_base_metrics(df)

        # 3. 國別漲跌停判定 (會產出 is_limit_up, Limit_Price, is_anomaly)
        df = self.rules.apply(df)

        # 4. 漲停行為分類 (LU_Type4) 與 隔日沖死法 (Fail_Type)
        df = self._calculate_pattern_analysis(df)

        # 5. 未來報酬分佈 (隔日, 5D, 6-10D, 11-20D)
        df = self._calculate_forward_returns(df)

        # 6. 存入資料庫
        df.to_sql("cleaned_daily_base", self.conn, if_exists='replace', index=False)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sid_date ON cleaned_daily_base (StockID, 日期)")
        
        return f"{self.market_abbr}: 處理 {len(df)} 筆, 偵測漲停 {df['is_limit_up'].sum()} 筆"

    def _clean_data(self, df):
        # 排除 Ghost Row
        mask_ghost = (df['成交量'] == 0) & (df['開盤'] == df['收盤']) & (df['最高'] == df['最低'])
        return df[~mask_ghost].copy()

    def _calculate_base_metrics(self, df):
        df['PrevClose'] = df.groupby('StockID')['收盤'].shift(1)
        df['Ret_Day'] = df['收盤'] / df['PrevClose'] - 1
        df['Vol_MA5'] = df.groupby('StockID')['成交量'].transform(lambda x: x.rolling(5).mean())
        df['Vol_Ratio'] = df['成交量'] / df.groupby('StockID')['Vol_MA5'].shift(1)
        return df

    def _calculate_pattern_analysis(self, df):
        # 判定 LU_Type4 與 Fail_Type
        df['Prev_LU'] = df.groupby('StockID')['is_limit_up'].shift(1).fillna(False)
        df['Overnight_Alpha'] = (df['開盤'] / df['PrevClose'] - 1).where(df['Prev_LU'])
        
        # 這裡呼叫 rules 裡的分類邏輯
        df['LU_Type4'] = df.apply(lambda r: self.rules.classify_lu_type4(r, r.get('Limit_Price', 0)) if r['is_limit_up'] else 0, axis=1)
        df['Fail_Type'] = df.apply(lambda r: self.rules.classify_fail_type(r) if r['Prev_LU'] else 0, axis=1)
        
        # 連板計數
        df['Seq_LU_Count'] = df.groupby((df['is_limit_up'] != df.groupby('StockID')['is_limit_up'].shift()).cumsum())['is_limit_up'].cumsum()
        df.loc[~df['is_limit_up'], 'Seq_LU_Count'] = 0
        return df

    def _calculate_forward_returns(self, df):
        # 預計算未來區間的極值 (以收盤價為基準計算報酬)
        def get_forward_stats(col, shift_start, window):
            shifted = df.groupby('StockID')[col].shift(-shift_start)
            return shifted.rolling(window, min_periods=1)

        # 隔日 (T+1)
        df['Next_1D_Max'] = (df.groupby('StockID')['最高'].shift(-1) / df['收盤']) - 1
        df['Next_1D_Min'] = (df.groupby('StockID')['最低'].shift(-1) / df['收盤']) - 1

        # 未來 5 日 (T+1 ~ T+5)
        df['Fwd_5D_Max'] = (get_forward_stats('最高', 1, 5).max() / df['收盤']) - 1
        df['Fwd_5D_Min'] = (get_forward_stats('最低', 1, 5).min() / df['收盤']) - 1

        # 未來 6-10 日
        df['Fwd_6_10D_Max'] = (get_forward_stats('最高', 6, 5).max() / df['收盤']) - 1
        df['Fwd_6_10D_Min'] = (get_forward_stats('最低', 6, 5).min() / df['收盤']) - 1

        # 未來 11-20 日
        df['Fwd_11_20D_Max'] = (get_forward_stats('最高', 11, 10).max() / df['收盤']) - 1
        df['Fwd_11_20D_Min'] = (get_forward_stats('最低', 11, 10).min() / df['收盤']) - 1
        return df