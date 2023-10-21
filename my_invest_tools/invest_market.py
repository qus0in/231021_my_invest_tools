from ko_etf_tools import KoETF
# 내장
from multiprocessing import Pool, cpu_count
# 설치
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

class InvestMarket(KoETF):

    @classmethod
    def get_price_with_symbol(cls, symbol):
        return symbol, cls.get_price(symbol)


    @classmethod
    def get_prices(cls, screener):
        with Pool(cpu_count() * 2) as p:
            prices = p.map(
                cls.get_price_with_symbol,
                screener.index.tolist())
        return prices