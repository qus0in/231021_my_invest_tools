import requests
import pandas as pd
import ast

class KoETF:
    @classmethod
    def get_etf_list(cls):
        """Naver 금융에서 ETF 목록을 가져와 DataFrame으로 반환"""
        URL = 'https://finance.naver.com/api/sise/etfItemList.nhn'
        
        # HTTP 요청 및 응답 처리
        response = requests.get(URL)
        data = response.json()['result']['etfItemList']
        
        # DataFrame 변환 및 인덱스 설정
        return pd.DataFrame(data).set_index('itemcode')

    @classmethod
    def get_price(cls, symbol, startTime='19000101', endTime='20991231', timeframe='day'):
        """Naver Finance에서 주어진 심볼의 가격 정보를 가져와 DataFrame으로 반환"""
        URL = 'https://api.finance.naver.com/siseJson.naver'
        params = {
            'symbol': symbol,
            'requestType': 1,
            'startTime': startTime,
            'endTime': endTime,
            'timeframe': timeframe
        }
        
        # HTTP 요청 및 응답 처리
        response = requests.get(URL, params)
        
        # 응답 데이터 정리 및 DataFrame 변환
        data = ast.literal_eval(response.text.replace('\n', ''))
        df = pd.DataFrame(data[1:], columns=data[0])
        
        # 날짜 형식 변환 및 인덱스 설정
        df['날짜'] = pd.to_datetime(df['날짜'], format='%Y%m%d')
        return df.set_index('날짜')