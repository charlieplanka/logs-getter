from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine
from sqlalchemy.orm import sessionmaker
import requests
from requests.exceptions import HTTPError
import logging
from datetime import date, datetime
import random

LOGS_URL = 'http://www.dsdev.tech/logs/'
DB_CONN_STR = 'postgresql+psycopg2://graffit:graffit@localhost/graffit_logs'

logger = logging.getLogger(__name__)  # где уместнее настраивать логирование?
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
logger.addHandler(handler)

Base = declarative_base()


class LogsGetter:
    def __init__(self, url: str, db_connection_string: str):
        self.url = url
        self.logs = []  # хранить как параметр объекта или передавать из функции в фцнкцию?
        self.db_string = db_connection_string

    def get_logs(self, date: date):
        logs = self._request_logs_from_server(date)
        self._parse_logs(logs)
        self._sort_logs_by_date()
        self._save_logs_to_DB()

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

    def _save_logs_to_DB(self):
        logger.info('Сохраняем логи в базу..')
        session = self._connect_to_DB()
        for entry in self.logs:
            db_entry = LogEntryDB(
                created=entry.created,
                first_name=entry.first_name,
                second_name=entry.second_name,
                message=entry.message,
                user_id=entry.user_id
                )
            logger.debug(db_entry)
        session.commit()
        logger.info('Логи успешно записаны в базу')

    def _connect_to_DB(self):
        engine = create_engine(self.db_string)
        Base.metadata.create_all(engine)
        Session = sessionmaker(engine)
        return Session()

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


class LogEntryDB(Base):
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False)
    first_name = Column(String(100))
    second_name = Column(String(100))
    message = Column(Text)
    user_id = Column(Integer)

    def __repr__(self):
        return f'<Запись создана {self.created}, пользователь: {self.first_name} {self.second_name}, USER ID: {self.user_id}>'


def main():
    logs_date = date(2021, 1, 23)
    getter = LogsGetter(LOGS_URL, DB_CONN_STR)
    getter.get_logs(logs_date)


if __name__ == '__main__':
    main()
