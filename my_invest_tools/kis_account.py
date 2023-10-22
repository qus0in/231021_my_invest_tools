from my_invest_tools import DynamoDB
from my_invest_tools import CommonUtils as utils
# 설치
import requests
import pandas as pd

class KISAccount(DynamoDB):
    DOMAIN = 'https://openapi.koreainvestment.com:9443'
    
    def __init__(self, db_url:str, db_key:str,
                 token_table:str,
                 cano:str, appkey:str, appsecret:str):
        
        super().__init__(db_url, db_key)
        self._kis_token = ''

        self.kis_cano = cano
        self.kis_ak = appkey
        self.kis_sk = appsecret
        self.token_table = token_table


    @property
    def kis_token(self):
        return self._kis_token or self.get_kis_token()


    def get_kis_token(self):
        self.TABLE_NAME = self.token_table
        result = self.query(
            'cano = :cano and dt = :dt',
            {':cano': {'S':self.kis_cano},
             ':dt': {'S':utils.now_str}}).json()
        if result['Count']:
            self._kis_token = result.get('Items')[0].get('token').get('S')
        else:
            print(f'토큰 업데이트 ({self.kis_cano}, {utils.now_str})')
            token = self.get_token_from_kis()
            self.put_item(dict(
                cano={'S': self.kis_cano},
                token={'S': token},
                dt={'S': utils.now_str}))
            self._kis_token = self.get_kis_token()
        return self.kis_token


    def url(self, path):
        return f'{self.DOMAIN}/{path}'


    def get_token_from_kis(self):
        PATH = 'oauth2/tokenP'
        response = requests.post(self.url(PATH),
            json=dict(
                grant_type='client_credentials',
                appkey=self.kis_ak,
                appsecret=self.kis_sk),
        )
        if response.status_code != 200:
            raise Exception(f'{response.status_code}: {response.text}')
        return response.json().get('access_token')
    

    def headers(self, tr_id):
        return {
            'content-type': 'application/json; charset=utf-8',
            'authorization': f'Bearer {self.kis_token}',
            'appkey': self.kis_ak,
            'appsecret': self.kis_sk,
            'tr_id': tr_id,
            'custtype': 'P',
        }

    
    def get_account_balance_from_kis(self):
        PATH = 'uapi/domestic-stock/v1/trading/inquire-account-balance'
        response = requests.get(self.url(PATH),
            params=dict(
                CANO=self.kis_cano,
                ACNT_PRDT_CD='01',
                INQR_DVSN_1='',
                BSPR_BF_DT_APLY_YN=''),
            headers=self.headers(tr_id='CTRP6548R')
        )
        if response.status_code != 200:
            raise Exception(f'{response.status_code}: {response.text}')
        data = response.json().get('output1')
        df = pd.DataFrame(data).astype(float)
        df.columns = ['매입금액', '평가금액', '평가손익금액',
                      '신용대출금액', '실제순자산금액', '전체비중율']
        df.index = ['주식', '펀드/MMW', '채권', 'ELS/DLS',
            'WRAP', '신탁/퇴직연금/외화신탁', 'RP/발행어음',
            '해외주식', '해외채권', '금현물', 'CD/CP', '단기사채',
            '타사상품', '외화단기사채', '외화 ELS/DLS', '외화',
            '예수금+CMA', '청약자예수금', '<합계>']
        return df


    @property
    def account_budget(self):
        return self.get_account_balance_from_kis()\
            .loc[['주식', 'RP/발행어음', '예수금+CMA']]\
            .실제순자산금액.sum()