from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine
from sqlalchemy.orm import sessionmaker
import requests
from requests.exceptions import HTTPError
from logger import logger
from datetime import date, datetime
import random

LOGS_URL = 'http://www.dsdev.tech/logs/'
DB_CONN_STR = 'postgresql+psycopg2://graffit:graffit@localhost/graffit_logs'

Base = declarative_base()


class LogsGetter:
    def __init__(self, url: str, db_connection_string: str):
        self.url = url
        self.db_string = db_connection_string
        self.logs = []

    def get_logs(self, log_date: date):
        if not isinstance(log_date, date):
            raise TypeError('Parameter "log_date" should be datetime.date')  # задокументировать

        try:
            logs = self._request_logs_from_server(log_date)
        except HTTPError as e:
            logger.error(f'An HTTP-error occured: {e}')
            return  # нужно ли прокидывать ошибку наружу?
        except RequestError as e:
            logger.error(f'An error occured: {e}')
            return

        try:
            self._parse_logs(logs)
        except LogsParsingError as e:
            logger.error(f'Cannot parse logs: {e}')
            return

        self._sort_logs_by_date()
        self._save_logs_to_DB()

    def _request_logs_from_server(self, date: date):
        logger.info(f'Requesting {date} logs from server..')
        date_formatted = date.strftime('%Y%m%d')
        url = '{}{}'.format(self.url, date_formatted)
        response = requests.get(url)
        response.raise_for_status()
        logs = response.json()
        error = logs['error']
        if error:
            raise RequestError(error)
        else:
            logger.info('Received successfully')
            return logs

    def _parse_logs(self, logs: dict):
        try:
            logs = logs['logs']
        except KeyError as e:
            raise LogsParsingError(f'no key {e}')  # нужен ли здесь эксепшн?

        for entry in logs:
            try:
                created = entry['created_at']
            except KeyError as e:
                logger.debug(entry)
                raise LogsParsingError(f'no required field {e}')

            # как обрабатывать случаи пропуска необязательных полей?
            first_name, second_name = entry['first_name'], entry['second_name']
            message = entry['message']
            user_id = entry['user_id']

            # logger.warning(f'В логе за {created} отсутствуют некоторые необязательные поля')
            # logger.debug(f'Отсуствует поле {e}')

            entry_obj = LogEntry(created, first_name, second_name, message, user_id)
            self.logs.append(entry_obj)
        logger.info(f'Total records: {len(self.logs)}')

    def _sort_logs_by_date(self):
        self.logs = LogsGetter.quick_sorting(self.logs)

    def _save_logs_to_DB(self):
        logger.info('Saving entries to DB..')
        session = self._connect_to_DB()
        # добавить роллбек
        for entry in self.logs:
            db_entry = LogEntryDB(
                created=entry.created,
                first_name=entry.first_name,
                second_name=entry.second_name,
                message=entry.message,
                user_id=entry.user_id
                )
            logger.debug(db_entry)
            session.add(db_entry)
        session.commit()
        logger.info('Saved successfully')

    def _connect_to_DB(self):
        engine = create_engine(self.db_string)
        Base.metadata.create_all(engine)
        Session = sessionmaker(engine)
        return Session()

    @staticmethod
    def quick_sorting(logs: list):
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
        return f'<{self.created}, user: {self.first_name} {self.second_name}, ID: {self.user_id}>'


class LogsParsingError(Exception):
    pass


class RequestError(Exception):
    pass


def main():
    logs_date = date(2021, 1, 23)
    getter = LogsGetter(LOGS_URL, DB_CONN_STR)
    getter.get_logs(logs_date)


if __name__ == '__main__':
    main()
