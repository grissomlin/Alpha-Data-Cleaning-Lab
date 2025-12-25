# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

class AlphaCoreEngine:
    def __init__(self, conn, rules, market_abbr):
        self.conn = conn
        self.rules = rules
        self.market_abbr = market_abbr

    def _auto_map_columns(self, df):
        """自動識別欄位名稱，將其統一名稱為標準中文"""
        cands = {
            '日期': ['日期', 'Date', 'date', 'time', 'Time', 'datetime'],
            'StockID': ['StockID', 'symbol', 'Symbol', 'code', 'Code', 'Ticker'],
            '開盤': ['開盤', '開盤價', 'Open', 'open'],
            '最高': ['最高', '最高價', 'High', 'high'],
            '最低': ['最低', '最低價', 'Low', 'low'],
            '收盤': ['收盤', '收盤價', 'Close', 'close', 'Adj Close'],
            '成交量': ['成交量', 'Volume', 'volume', 'vol', 'Vol']
        }
        
        rename_dict = {}
        for target, aliases in cands.items():
            for alias in aliases:
                if alias in df.columns:
                    rename_dict[alias] = target
                    break
        return df.rename(columns=rename_dict)

    def execute(self):
        # 1. 動態偵測資料表
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [t[0] for t in cursor.fetchall()]
        
        target_table = 'daily_prices' if 'daily_prices' in tables else \
                       [t for t in tables if t != 'cleaned_daily_base'][0]
        
        print(f"🔍 {self.market_abbr}: 使用資料表 '{target_table}'")
        df = pd.read_sql(f"SELECT * FROM {target_table}", self.conn)

        # 2. 自動修正欄位名稱 (解決 KeyError: '日期' 的關鍵)
        df = self._auto_map_columns(df)
        
        # 檢查必要欄位是否備齊
        required = ['日期', 'StockID', '收盤']
        missing = [r for r in required if r not in df.columns]
        if missing:
            raise ValueError(f"❌ {self.market_abbr}: 欄位缺失 {missing}。現有欄位: {list(df.columns)}")

        # 3. 基礎預處理
        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
        df = df.dropna(subset=['日期'])
        df = df.sort_values(['StockID', '日期']).reset_index(drop=True)

        # 4. 清洗與衍生指標
        df = self._clean_data(df)
        df = self._calculate_base_metrics(df)

        # 5. 國別漲跌停判定 (is_limit_up, Limit_Price, is_anomaly)
        df = self.rules.apply(df)

        # 6. 漲停行為分類與隔日沖死法
        df = self._calculate_pattern_analysis(df)

        # 7. 未來報酬分佈 (T+1 ~ T+20)
        df = self._calculate_forward_returns(df)

        # 8. 存入資料庫
        df.to_sql("cleaned_daily_base", self.conn, if_exists='replace', index=False)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_sid_date ON cleaned_daily_base (StockID, 日期)")
        
        return f"{self.market_abbr}: 成功精煉 {len(df)} 筆，發現 {df['is_limit_up'].sum()} 次漲停"

    def _clean_data(self, df):
        # 排除 Ghost Row
        if all(c in df.columns for c in ['成交量', '開盤', '收盤', '最高', '最低']):
            mask_ghost = (df['成交量'] == 0) & (df['開盤'] == df['收盤']) & (df['最高'] == df['最低'])
            return df[~mask_ghost].copy()
        return df

    def _calculate_base_metrics(self, df):
        df['PrevClose'] = df.groupby('StockID')['收盤'].shift(1)
        df['Ret_Day'] = df['收盤'] / df['PrevClose'] - 1
        if '成交量' in df.columns:
            df['Vol_MA5'] = df.groupby('StockID')['成交量'].transform(lambda x: x.rolling(5).mean())
            df['Vol_Ratio'] = df['成交量'] / df.groupby('StockID')['Vol_MA5'].shift(1)
        else:
            df['Vol_Ratio'] = 1.0
        return df

    def _calculate_pattern_analysis(self, df):
        df['Prev_LU'] = df.groupby('StockID')['is_limit_up'].shift(1).fillna(False)
        df['Overnight_Alpha'] = (df['開盤'] / df['PrevClose'] - 1).where(df['Prev_LU'])
        
        # 呼叫 Rules 邏輯
        df['LU_Type4'] = df.apply(lambda r: self.rules.classify_lu_type4(r, r.get('Limit_Price', 0)) if r['is_limit_up'] else 0, axis=1)
        df['Fail_Type'] = df.apply(lambda r: self.rules.classify_fail_type(r) if r['Prev_LU'] else 0, axis=1)
        
        # 連板計數
        df['Seq_LU_Count'] = df.groupby((df['is_limit_up'] != df.groupby('StockID')['is_limit_up'].shift()).cumsum())['is_limit_up'].cumsum()
        df.loc[~df['is_limit_up'], 'Seq_LU_Count'] = 0
        return df

    def _calculate_forward_returns(self, df):
        # 滾動未來報酬
        def get_fwd(col, shift_s, win):
            return df.groupby('StockID')[col].shift(-shift_s).rolling(win, min_periods=1)

        if '最高' in df.columns and '最低' in df.columns:
            df['Next_1D_Max'] = (df.groupby('StockID')['最高'].shift(-1) / df['收盤']) - 1
            df['Fwd_5D_Max'] = (get_fwd('最高', 1, 5).max() / df['收盤']) - 1
            df['Fwd_5D_Min'] = (get_fwd('最低', 1, 5).min() / df['收盤']) - 1
            df['Fwd_11_20D_Max'] = (get_fwd('最高', 11, 10).max() / df['收盤']) - 1
        return df
