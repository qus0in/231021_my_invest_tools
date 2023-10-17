import requests

class DynamoDB:
    
    def __init__(self, API_URL, API_KEY, TABLE_NAME):
        self.API_URL = API_URL
        self.API_KEY = API_KEY
        self.TABLE_NAME = TABLE_NAME


    def put_item(self, item: dict):
        data = {'TableName': self.TABLE_NAME, 'Item': item}
        headers = {'x-api-key': self.API_KEY}
        response = requests.post(self.API_URL, json=data, headers=headers)
        return response
    

    def query(self, expr:str, attr_val:dict):
        data = {'TableName': self.TABLE_NAME,
                'KeyConditionExpression': expr,
                'ExpressionAttributeValues': attr_val}
        headers = {'x-api-key': self.API_KEY}
        response = requests.post(self.API_URL + '/query', json=data, headers=headers)
        return response
