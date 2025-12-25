# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

class AlphaCoreEngine:
    def __init__(self, conn, rules, market_abbr):
        self.conn = conn
        self.rules = rules
        self.market_abbr = market_abbr

    def execute(self):
        # 1. 執行 JOIN 確保拿到價格與市場類別 (解決母體問題)
        query = """
        SELECT p.*, i.market as MarketType, i.name as stock_name
        FROM stock_prices p
        LEFT JOIN stock_info i ON p.symbol = i.symbol
        """
        print(f"📡 {self.market_abbr}: 讀取 stock_prices 並關聯 stock_info...")
        df = pd.read_sql(query, self.conn)

        # 2. 強制將所有欄位轉為標準中文名稱 (解決 KeyError 與秒殺問題)
        rename_map = {
            'date': '日期', 'symbol': 'StockID', 
            'open': '開盤', 'high': '最高', 'low': '最低', 
            'close': '收盤', 'volume': '成交量'
        }
        df = df.rename(columns=rename_map)

        # 3. 確保資料型態正確
        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        df['收盤'] = pd.to_numeric(df['收盤'], errors='coerce')
        
        # 排除掉 None 或 無日期資料
        initial_count = len(df)
        df = df.dropna(subset=['日期', '收盤']).reset_index(drop=True)
        print(f"📊 {self.market_abbr}: 原始筆數 {initial_count} -> 有效筆數 {len(df)}")

        if df.empty:
            return f"{self.market_abbr}: 處理失敗 - 無有效資料列"

        # 4. 排序並計算基礎指標
        df = df.sort_values(['StockID', '日期'])
        df['PrevClose'] = df.groupby('StockID')['收盤'].shift(1)
        df['Ret_Day'] = df['收盤'] / df['PrevClose'] - 1
        
        # 處理成交量與均量
        df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce').fillna(0)
        df['Vol_MA5'] = df.groupby('StockID')['成交量'].transform(lambda x: x.rolling(5).mean())
        df['Vol_Ratio'] = df['成交量'] / df.groupby('StockID')['Vol_MA5'].shift(1)

        # 5. 國別漲跌停判定
        df = self.rules.apply(df)

        # 6. 型態分類與未來報酬 (確保 is_limit_up 為布林值)
        df['is_limit_up'] = df['is_limit_up'].astype(bool)
        df['Prev_LU'] = df.groupby('StockID')['is_limit_up'].shift(1).fillna(False)
        df['Overnight_Alpha'] = (df['開盤'] / df['PrevClose'] - 1).where(df['Prev_LU'])
        
        df['LU_Type4'] = df.apply(lambda r: self.rules.classify_lu_type4(r, r.get('Limit_Price', 0)) if r['is_limit_up'] else 0, axis=1)
        df['Fail_Type'] = df.apply(lambda r: self.rules.classify_fail_type(r) if r['Prev_LU'] else 0, axis=1)
        
        # 連板計數
        df['Seq_LU_Count'] = df.groupby((df['is_limit_up'] != df.groupby('StockID')['is_limit_up'].shift()).cumsum())['is_limit_up'].cumsum()
        df.loc[~df['is_limit_up'], 'Seq_LU_Count'] = 0

        # 7. 未來報酬極值計算
        df = self._calculate_forward_returns(df)

        # 8. 存入資料庫
        df.to_sql("cleaned_daily_base", self.conn, if_exists='replace', index=False)
        return f"{self.market_abbr}: 精煉完成 ({len(df)} 筆), 偵測漲停 {df['is_limit_up'].sum()} 次"

    def _calculate_forward_returns(self, df):
        def get_fwd(col, s, w):
            return df.groupby('StockID')[col].shift(-s).rolling(w, min_periods=1)
        
        df['Next_1D_Max'] = (df.groupby('StockID')['最高'].shift(-1) / df['收盤']) - 1
        df['Fwd_5D_Max'] = (get_fwd('最高', 1, 5).max() / df['收盤']) - 1
        df['Fwd_5D_Min'] = (get_fwd('最低', 1, 5).min() / df['收盤']) - 1
        df['Fwd_11_20D_Max'] = (get_fwd('最高', 11, 10).max() / df['收盤']) - 1
        return df
