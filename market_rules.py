import pandas as pd

class MarketRuleRouter:
    def __init__(self, market_type="TW"):
        self.market_type = market_type

    def apply(self, df):
        """
        根據市場類型分發判定邏輯
        """
        if df.empty:
            return df

        # 確保排序以計算昨日收盤
        df = df.sort_values(['StockID', '日期']).reset_index(drop=True)
        df['Prev_Close'] = df.groupby('StockID')['收盤'].shift(1)

        if self.market_type == "TW":
            return self._apply_taiwan_rules(df)
        elif self.market_type == "US":
            return self._apply_us_rules(df)
        else:
            # 其他市場 (JP, CN, KR, HK) 暫時採用通用 9% 邏輯，可後續擴充
            return self._apply_generic_rules(df)

    def _apply_taiwan_rules(self, df):
        # 1. 判定是否為 ETF (代碼 00 開頭 或 產業為 nan)
        # 修正 1454次連板的關鍵：排除 ETF
        is_etf = df['StockID'].str.startswith('00') | df['產業'].isna()
        
        df['is_limit_up'] = 0
        
        # 2. 漲停判定：昨日收盤 * 1.09 <= 今日收盤 (且排除 ETF)
        # 這樣就不會再發生 ETF 誤判漲停
        mask_lu = (~is_etf) & (df['收盤'] >= df['Prev_Close'] * 1.09)
        df.loc[mask_lu, 'is_limit_up'] = 1
        
        return df

    def _apply_us_rules(self, df):
        # 美股無漲跌停，通常以 15% 異常漲幅標記
        df['is_limit_up'] = ((df['收盤'] / df['Prev_Close'] - 1) >= 0.15).astype(int)
        return df

    def _apply_generic_rules(self, df):
        # 通用邏輯：漲幅 > 9.5% 判定為漲停
        df['is_limit_up'] = ((df['收盤'] / df['Prev_Close'] - 1) >= 0.095).astype(int)
        return df
