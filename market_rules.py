import pandas as pd

class MarketRuleRouter:
    def __init__(self, market_type="TW"):
        self.market_type = market_type

    @classmethod
    def get_rules(cls, market_abbr):
        """
        供 main_pipeline.py 呼叫的類別方法
        """
        # 建立一個實例並回傳
        return cls(market_type=market_abbr)

    def apply(self, df):
        """
        執行漲停判定邏輯
        """
        if df.empty:
            return df

        # 1. 確保排序以計算前日收盤
        df = df.sort_values(['StockID', '日期']).reset_index(drop=True)
        df['Prev_Close'] = df.groupby('StockID')['收盤'].shift(1)

        # 2. 根據市場分發規則
        if self.market_type == "TW":
            return self._apply_taiwan_rules(df)
        elif self.market_type == "US":
            return self._apply_us_rules(df)
        else:
            return self._apply_generic_rules(df)

    def _apply_taiwan_rules(self, df):
        # 核心修正：判定是否為 ETF (代碼 00 開頭 或 產業為空)
        # 產業欄位名稱請根據你資料庫實際狀況調整，通常為 '產業' 或 'Sector'
        sector_col = '產業' if '產業' in df.columns else 'Sector'
        
        is_etf = df['StockID'].str.startswith('00')
        if sector_col in df.columns:
            is_etf = is_etf | df[sector_col].isna()
        
        df['is_limit_up'] = 0
        
        # 判定基準：今日收盤 >= 昨日收盤 * 1.09 (且非 ETF)
        mask_lu = (~is_etf) & (df['收盤'] >= df['Prev_Close'] * 1.09)
        df.loc[mask_lu, 'is_limit_up'] = 1
        
        return df

    def _apply_us_rules(self, df):
        df['is_limit_up'] = ((df['收盤'] / df['Prev_Close'] - 1) >= 0.15).astype(int)
        return df

    def _apply_generic_rules(self, df):
        # 通用規則 (日/韓/港/中) 暫採 9.5% 門檻
        df['is_limit_up'] = ((df['收盤'] / df['Prev_Close'] - 1) >= 0.095).astype(int)
        return df
