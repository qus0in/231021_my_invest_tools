import requests  # requests 라이브러리를 사용하여 HTTP 요청을 처리합니다.
import pandas as pd  # pandas 라이브러리를 사용하여 데이터를 처리합니다.

class KoETF:
    NAVER_FINANCE_DOMAIN = 'https://finance.naver.com'  # Naver 금융의 도메인 URL

    @classmethod
    def get_etf_list(cls):
        # Naver 금융 ETF 목록 API의 URL을 생성합니다.
        URL = cls.NAVER_FINANCE_DOMAIN + '/api/sise/etfItemList.nhn'
        
        # HTTP GET 요청을 보내고 응답을 받습니다.
        response = requests.get(URL)
        
        # 응답에서 JSON 데이터를 추출하고, 필요한 ETF 목록 정보만을 가져옵니다.
        data = response.json().get('result').get('etfItemList')
        
        # 데이터를 Pandas DataFrame으로 변환하고, 'itemcode'를 인덱스로 설정합니다.
        return pd.DataFrame(data).set_index('itemcode')