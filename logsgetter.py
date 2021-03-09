import requests
from requests.exceptions import HTTPError
import logging
from datetime import date, datetime
import random

LOGS_URL = 'http://www.dsdev.tech/logs/'

logger = logging.getLogger(__name__)  # где уместнее настраивать логирование?
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)


class LogsGetter:
    def __init__(self, url: str):
        self.url = url
        self.logs = [] # хранить как параметр объекта или передавать из функции в фцнкцию?

    def get_logs(self, date: date):
        logs = self._request_logs_from_server(date)
        self._parse_logs(logs)
        self._sort_logs_by_date()

    def _request_logs_from_server(self, date: date):
        logger.info('Получаем логи с сервера..')
        date_formatted = date.strftime('%Y%m%d')
        url = '{}{}'.format(self.url, date_formatted)
        try:
            logs = requests.get(url).json()
        except HTTPError as e:
            logger.error(f'При запросе логов произошла ошибка: {e}')
            # выбросить исключение наружу класса
        else:
            logger.info(f'Получены логи за {date.strftime("%Y/%m/%d")}')
            return logs

    def _parse_logs(self, logs):
        logs = logs['logs']
        for entry in logs:
            created = entry['created_at']
            first_name, second_name = entry['first_name'], entry['second_name']
            message = entry['message']
            user_id = entry['user_id']
            entry_obj = LogEntry(created, first_name, second_name, message, user_id)
            self.logs.append(entry_obj)
        logger.info(f'Всего записей: {len(self.logs)}')

    def _sort_logs_by_date(self):
        self.logs = LogsGetter.quick_sorting(self.logs)

    @staticmethod
    def quick_sorting(logs):
        if len(logs) <= 1:
            return logs
        else:
            reference = random.choice(logs)
            less = []
            greater = []
            equall = []
            for log in logs:
                if log.created < reference.created:
                    less.append(log)
                elif log.created > reference.created:
                    greater.append(log)
                elif log.created == reference.created:
                    equall.append(log)
            return LogsGetter.quick_sorting(less) + equall + LogsGetter.quick_sorting(greater)


class LogEntry():
    def __init__(self, created: str, first_name=None, second_name=None, message=None, user_id=None):
        self.created = datetime.fromisoformat(created)
        self.first_name = first_name
        self.second_name = second_name
        self.message = message
        self.user_id = int(user_id)


def main():
    logs_date = date(2021, 1, 23)
    getter = LogsGetter(LOGS_URL)
    getter.get_logs(logs_date)


if __name__ == '__main__':
    main()
