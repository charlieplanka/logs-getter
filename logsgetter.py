import requests
import logging
from datetime import date

LOGS_URL = 'http://www.dsdev.tech/logs/'

logger = logging.getLogger(__name__)  # где уместнее настраивать логирование?
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)


class LogsGetter:
    def __init__(self, url: str):
        self.url = url

    def get_logs(self, date: date):
        logs = self._request_logs_from_server(date)
        logs = self._sort_logs_by_date()

    def _request_logs_from_server(self, date: date):
        date_formatted = date.strftime('%Y%m%d')
        url = '{}{}'.format(self.url, date_formatted)
        logs = requests.get(url).json()
        logger.info(f'Получены логи за {date.strftime("%Y/%m/%d")}')
        return logs
    
    def _sort_logs_by_date(self):
        pass


def main():
    logs_date = date(2021, 1, 23)
    getter = LogsGetter(LOGS_URL)
    getter.get_logs(logs_date)


if __name__ == '__main__': 
    main()
