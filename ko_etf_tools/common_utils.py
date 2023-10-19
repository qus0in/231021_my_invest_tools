import datetime

class CommonUtils:
    @classmethod
    @property
    def now_str(cls):
        return datetime.datetime.utcnow()\
                .strftime('%Y-%m-%d')