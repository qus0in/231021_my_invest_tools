import datetime
import pandas as pd
from ko_etf_tools import KoETF, DynamoDB

class InvestHelper(KoETF):
    
    @classmethod
    def get_screener(cls):
        ms = cls.get_etf_list()
        not_contains = lambda x: f'not itemname.str.contains("{x}")'
        join = lambda *x: '|'.join(x)
        # 3년 이하 단기채 및 금리 제외
        ms.query(not_contains(join('단기', '머니마켓', '3년', '금리')), inplace=True)
        # 배당, 커버드콜 전략 제외
        ms.query(not_contains(join('배당', '커버드콜')), inplace=True)
        # 채권 혼합, 종합채권 제외
        ms.query(not_contains(join('혼합', '종합', 'TRF')), inplace=True)
        # 만기매칭형 제외
        ms.query(not_contains('\d\d-\d\d'), inplace=True)

        ms['groupName'] = ms.itemname.str.upper()

        kwds = ['(^\S* )','선물','\(.*\)','TR$','액티브', 'PLUS','플러스',
                '동일가중','투자(?!등급)', 'ESG', 'SRI', 'KRX', 'KIS',
                'ISELECT', 'FN', 'SOLACTIVE', 'MSCI', 'INDXX', 'KAP',
                'ENHANCED', '스트립']

        repl_exprs = [
            # 주요 키워드 제거
            (join(*kwds), ''),
            # 섹터
            ('.*2차전지(?!.*인버스).*$', '2차전지'),
            ('.*바이오.*$', '바이오'),
            ('^(?=.*반도체)(?!.*미국).*$', '반도체'),
            ('(?=.*자율주행)(?=.*전기차).*$', '자율주행&전기차'),
            # 국가
            ('^일본(?!.*엔).*$', '일본'),
            ('^차이나(?!.*(전기차|항셍)).*$', '중국'),
            ('인도.*', '인도'),
            ('베트남.*', '베트남'),
            # 채권
            ('채권', '채'),
            ('국고채', '국채'),
            ('^미국.*30년.*', '미국국채30년'),
            ('^미국.*10년.*', '미국국채10년', '미국국채30년'),
            # 국내
            ('삼성그룹.*', '삼성그룹'),
            ('코스닥150\s*', '코스닥150 '),
        ]

        for v in repl_exprs:
            ms.groupName.replace(v[0], v[1], regex=True, inplace=True)

        # 코스피 관련 처리
        ms.groupName = ms.groupName.str.strip()\
            .replace('K200', '200', regex=True)\
            .replace('200\s*', '코스피200 ', regex=True)\
            .replace('^TOP', '코스피200 TOP', regex=True)\
            .replace('레버리지', '코스피200 레버리지', regex=False)\
            .replace('인버스', '코스피200 인버스', regex=False)
        
        gb = ms.reset_index().groupby('groupName')

        result = gb.first()
        result['groupMarketSum'] = gb.marketSum.sum()
        result['category'] = result.etfTabCode.map({
            1: '국내 시장지수', 2: '국내 업종/테마', 3: '국내 파생',
            4: '해외 주식', 5: '원자재', 6:'채권', 7: '기타'})
        query = '(etfTabCode in [3, 5, 7] and groupMarketSum >= 500) '\
                'or groupMarketSum >= 1000'
        
        return result\
            .query(query)\
            .sort_values('groupMarketSum', ascending=False)\
            [['itemcode', 'itemname', 'groupMarketSum', 'category']]\
            .set_index('itemcode')
    

    @classmethod
    @property
    def now_str(cls):
        return datetime.datetime.utcnow()\
                .strftime('%Y-%m-%d')


    @classmethod
    def put_screener(cls, db:DynamoDB):
        items = {k:dict(M={k2:dict(S=str(v2)) for k2, v2 in v.items()})
            for k, v in InvestHelper.get_screener().to_dict().items()}
        return db.put_item(
            dict(dt={'S':cls.now_str}, screener={'M':items})).json() or 'success'
    
    
    @classmethod
    def get_recent_screener(cls, db:DynamoDB):
        args = 'dt = :dt', {':dt': {'S': cls.now_str}}
        data = db.query(*args).json()\
            .get('Items')[0].get('screener').get('M')
        df = pd.DataFrame({
            k:{k2:v2.get('S') for k2, v2 in v.get('M').items()}
            for k, v in data.items()})
        return df.astype({'groupMarketSum':'int'})\
                .sort_values('groupMarketSum', ascending=False)