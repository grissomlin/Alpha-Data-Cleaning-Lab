# -*- coding: utf-8 -*-

class MarketRuleRouter:
    """
    國別策略路由器：根據資料庫名稱縮寫 (TW, JP, US, CN, KR, HK) 分流至不同規則。
    """
    @staticmethod
    def get_rules(market_abbr):
        market_abbr = market_abbr.upper()
        if market_abbr == 'TW':
            return TaiwanRules()
        elif market_abbr == 'JP':
            return JapanRules()
        elif market_abbr == 'CN':
            return ChinaRules()
        elif market_abbr == 'KR':
            return KoreaRules()
        # 美股與港股目前套用基礎統計規則
        return BaseRules()

class BaseRules:
    """
    基礎規則類別：定義通用的分類邏輯。
    """
    def classify_lu_type4(self, row, limit_price):
        """
        漲停行為分類 (LU_Type4)：
        1: 無量鎖死 (一字板) - 開盤即漲停且最高等於最低。
        2: GAP-UP (跳空板) - 開盤漲幅 >= 7%。
        3: 高量換手 (爆量板) - 當日成交量 >= 5日均量之 3 倍。
        4: 浮動漲停 (普通板) - 其他鎖死型態。
        """
        if row['開盤'] >= limit_price - 0.01 and row['最高'] == row['最低']: 
            return 1
        if (row['開盤'] / row['PrevClose'] - 1) >= 0.07: 
            return 2
        if row.get('Vol_Ratio', 0) >= 3.0: 
            return 3
        return 4

    def classify_fail_type(self, row):
        """
        隔日沖死法分類 (Fail_Type)：
        1: 崩潰 (Fail1) - 昨漲停今跌幅超過 5%。
        2: 炸板 (Fail2) - 盤中觸及漲停價但收盤未鎖住。
        4: 無溢價 - 昨漲停今日開盤價 <= 0%。
        """
        # 取得漲停價，若無定義則預設為收盤價
        limit_p = row.get('Limit_Price', row['收盤'])
        
        if row['Ret_Day'] <= -0.05: 
            return 1
        if row['最高'] >= limit_p and not row.get('is_limit_up', False): 
            return 2
        if row.get('Overnight_Alpha', 0) <= 0: 
            return 4
        return 0

    def apply(self, df):
        """預設套用於無漲跌幅限制市場。"""
        df['Limit_Price'] = df['收盤'] # 無限制市場以收盤為準計算
        df['is_limit_up'] = False
        df['is_limit_down'] = False
        # 美港股判定 20% 為暴漲
        df['is_anomaly'] = df['Ret_Day'].abs() > 1.0 
        return df

class JapanRules(BaseRules):
    """
    日本市場規則：依據價格區間固定金額判定 (TSE Special Quote 制度)。
    """
    def get_jp_limit_amount(self, price):
        """實作東京證券交易所最大漲跌額度表。"""
        if price < 100: return 30
        if price < 500: return 80
        if price < 1000: return 150
        if price < 1500: return 300
        if price < 3000: return 500
        if price < 5000: return 700
        if price < 10000: return 1500
        if price < 30000: return 5000
        return 10000

    def apply(self, df):
        df['JP_Limit'] = df['PrevClose'].apply(self.get_jp_limit_amount)
        df['Limit_Price'] = df['PrevClose'] + df['JP_Limit']
        
        # 判定漲停：收盤價 >= 昨收 + 限制金額 (容許1單位誤差)
        df['is_limit_up'] = df['收盤'] >= (df['Limit_Price'] - 1)
        # 標記異常：遠超區間限制 50% 視為資料錯誤
        df['is_anomaly'] = df['收盤'] > (df['Limit_Price'] + df['JP_Limit'] * 0.5)
        df['is_limit_down'] = False # 暫不計算日股跌停
        return df

class TaiwanRules(BaseRules):
    """
    台灣市場規則：區分上市櫃與興櫃，判定 LU_Type4 與 Fail_Type。
    """
    def apply(self, df):
        # 台股 10% 漲跌停價格計算
        df['Limit_Price'] = (df['PrevClose'] * 1.1).round(2)
        
        # 區分市場別判定
        if 'MarketType' in df.columns:
            listed = df['MarketType'].isin(['上市', '上櫃'])
            df.loc[listed, 'is_limit_up'] = df['Ret_Day'] >= 0.095
            df.loc[listed, 'is_limit_down'] = df['Ret_Day'] <= -0.095
            
            # 興櫃不設漲停，但標記 80% 異常報酬
            emg = df['MarketType'] == '興櫃'
            df.loc[emg, 'is_limit_up'] = False
            df.loc[emg, 'is_anomaly'] = df['Ret_Day'].abs() > 0.8
        else:
            df['is_limit_up'] = df['Ret_Day'] >= 0.095
            df['is_limit_down'] = df['Ret_Day'] <= -0.095

        # 基礎極端值過濾
        df.loc[df['Ret_Day'].abs() > 0.11, 'is_anomaly'] = True
        return df

class ChinaRules(BaseRules):
    """
    中國 A 股規則：區分主板 10% 與科創/創業板 20%。
    """
    def apply(self, df):
        # 假設 StockID 格式能區分市場
        # 此處簡化邏輯，實務上需依 StockID 前綴判斷
        df['is_limit_up'] = df['Ret_Day'] >= 0.095
        df['is_anomaly'] = df['Ret_Day'].abs() > 0.22
        return df

class KoreaRules(BaseRules):
    """
    韓國市場規則：LMS 制度 ±30%。
    """
    def apply(self, df):
        df['is_limit_up'] = df['Ret_Day'] >= 0.295
        df['is_anomaly'] = df['Ret_Day'].abs() > 0.35
        return df
