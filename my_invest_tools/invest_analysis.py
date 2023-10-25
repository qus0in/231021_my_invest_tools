from my_invest_tools import InvestMarket
# 설치
import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

class InvestAnalysis(InvestMarket):
    PERIODS=(3, 5, 8, 13, 21)

    @classmethod
    def get_tr(cls, price: pd.DataFrame):

        concat = lambda x, y: pd.concat([x, y], axis=1)
        th = concat(price.고가, price.종가.shift(1)).max(axis=1)
        tl = concat(price.저가, price.종가.shift(1)).min(axis=1)

        return th - tl
    

    @classmethod
    def get_aatr(cls, price: pd.DataFrame):

        atr = cls.get_tr(price).ewm(max(cls.PERIODS)).mean()

        return atr.iloc[-1] / price.종가.iloc[-1]
    

    @classmethod
    def get_corr_matrix(cls, prices: dict, row_num: int):

        data = {k:cls.get_tr(v) for k, v in prices}

        return pd.DataFrame(data)\
                .tail(row_num).dropna(axis=1).corr()


    @classmethod
    def get_score(cls, price):
        handler = lambda x: (x[-1] - x[0]) / x[0] * 100
        c = price[['종가']].tail(max(cls.PERIODS) * 2)
        scores = [c.apply(handler) / p for p
                  in cls.PERIODS if p <= len(price)]
        return pd.concat(scores, axis=1)\
                .mean().round(3).iloc[-1]


    @classmethod
    def get_data_groups(cls,
                        corr_matrix: pd.DataFrame,
                        n_clusters: int):
        
        cluster = AgglomerativeClustering(n_clusters=n_clusters)
        labels = cluster.fit_predict(corr_matrix)

        return [corr_matrix.index[labels == label].tolist()
                        for label in np.unique(labels)]
    

    @classmethod
    def get_ranking(cls,
                    screener_data: pd.DataFrame,
                    budget: int,
                    risk_limit: float=0.015,
                    enter_num: int=4):
        
        prices = cls.get_prices(screener_data)
        corr_matrix = cls.get_corr_matrix(prices, max(cls.PERIODS))
        data_groups = cls.get_data_groups(corr_matrix, 10)

        get_p = lambda x: {k:v for k, v in prices}.get(x)
        risk = lambda x: min(1, round(risk_limit / cls.get_aatr(get_p(x)), 3))
        record = lambda x: (x, screener_data.loc[x].itemname,
                            cls.get_score(get_p(x)), risk(x),)

        data = [sorted([record(itemcode) for itemcode in d],
                    key=lambda x: x[2])[-1] for d in data_groups]
        df = pd.DataFrame(data)
        df.columns = ['itemcode', 'itemname', 'score', 'risk']
        df['enter'] = ((df.risk * budget) / 100_000 / enter_num).apply(int) * 100_000

        return df.set_index('itemcode')\
            .sort_values('score', ascending=False)\
            .query('score > 0').head(enter_num + 1)