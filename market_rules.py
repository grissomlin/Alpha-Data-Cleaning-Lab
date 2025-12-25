# -*- coding: utf-8 -*-
class BaseRules:
    def classify_lu_type4(self, row, limit_price):
        # 1:無量鎖死, 2:跳空, 3:爆量, 4:普通
        if row['開盤'] >= limit_price - 0.01 and row['最高'] == row['最低']: return 1
        if (row['開盤'] / row['PrevClose'] - 1) >= 0.07: return 2
        if row.get('Vol_Ratio', 0) >= 3.0: return 3
        return 4

    def classify_fail_type(self, row):
        # 1:崩潰(Fail1), 2:炸板(Fail2), 4:無溢價
        if row['Ret_Day'] <= -0.05: return 1
        if row['最高'] >= row.get('Limit_Price', 0) and not row['is_limit_up']: return 2
        if row.get('Overnight_Alpha', 0) <= 0: return 4
        return 0

class JapanRules(BaseRules):
    def apply(self, df):
        # 日本價格區間金額表
        def get_jp_limit(p):
            if p < 100: return 30
            if p < 500: return 80
            if p < 1000: return 150
            if p < 1500: return 300
            if p < 3000: return 500
            return 1000
            
        df['Limit_Price'] = df['PrevClose'] + df['PrevClose'].apply(get_jp_limit)
        df['is_limit_up'] = df['收盤'] >= (df['Limit_Price'] - 1)
        df['is_limit_down'] = df['Ret_Day'] <= -0.15 # 假設日股跌幅門檻
        df['is_anomaly'] = df['Ret_Day'].abs() > 0.5
        return df

class TaiwanRules(BaseRules):
    def apply(self, df):
        df['Limit_Price'] = (df['PrevClose'] * 1.1).round(2)
        listed = df['MarketType'].isin(['上市', '上櫃'])
        df.loc[listed, 'is_limit_up'] = df['Ret_Day'] >= 0.095
        df.loc[listed, 'is_limit_down'] = df['Ret_Day'] <= -0.095
        df.loc[listed, 'is_anomaly'] = df['Ret_Day'].abs() > 0.11
        return df