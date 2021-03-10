from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine
from sqlalchemy.orm import sessionmaker
import requests
from requests.exceptions import HTTPError
from logger import logger
from datetime import date, datetime
import random
import json

LOGS_URL = 'http://www.dsdev.tech/logs/'
DB_CONN_STR = 'postgresql+psycopg2://graffit:graffit@localhost/graffit_logs'

Base = declarative_base()


class LogEntry:
    def __init__(self, created: str, first_name=None, second_name=None, message=None, user_id=None):
        self.created = datetime.fromisoformat(created)
        self.first_name = first_name
        self.second_name = second_name
        self.message = message
        self.user_id = user_id


class LogsGetter:
    def __init__(self, url: str, db_connection_string: str):
        self._url = url
        self._db_string = db_connection_string
        self._http_requester = HttpRequester()

    def get_logs(self, log_date: date):
        if not isinstance(log_date, date):
            raise TypeError('Parameter "log_date" should be datetime.date object')  # задокументировать

        logs = self._request_logs_from_server(log_date)
        parsed_logs = self._parse_logs(logs)
        sorted_logs = self._sort_logs_by_date(parsed_logs)
        self._save_logs_to_DB(sorted_logs)

    def _request_logs_from_server(self, date: date):
        logger.info(f'Requesting {date} logs from server..')
        date_formatted = date.strftime('%Y%m%d')
        url = '{}{}'.format(self._url, date_formatted)

        try:
            response = self._http_requester.get_content(url)
        except HTTPError as e:
            msg = f'An HTTP-error occurred: {e}'
            logger.error(msg)
            raise LogsGetterError(msg)

        logs = json.loads(response)
        error = logs.get('error', 'no error')
        if error == 'no error':
            logger.warning('No "error" key in response')
        elif error:
            msg = f'Server responded with an error: {error}'
            logger.error(msg)
            raise LogsGetterError(msg)
        logger.info('Received logs')
        return logs

    def _parse_logs(self, logs: dict):
        parsed = []
        logs = logs.get('logs')

        if not logs:
            msg = 'Cannot parse logs. No "logs" key in response. This key is required'
            logger.error(msg)
            raise LogsGetterError(msg)

        for entry in logs:
            created = entry.get('created_at')
            if not created:
                msg = 'No "created_at" field for entry. This field is required. Entry skipped'
                logger.error(msg)
                logger.debug(entry)
                continue

            first_name, second_name = entry.get('first_name'), entry.get('second_name')
            message = entry.get('message')
            user_id = entry.get('user_id')
            if not first_name or not second_name or not message or not user_id:
                logger.warning(f'Some optional fields are absent for entry {created}')
                logger.debug(entry)

            try:
                entry_obj = LogEntry(created, first_name, second_name, message, user_id)
            except ValueError as e:
                msg = f'An error occured while parsing: {e}. Entry skipped'
                logger.error(msg)
                logger.debug(entry)
                continue

            parsed.append(entry_obj)

        logger.info(f'Total records: {len(parsed)}')
        return parsed

    def _sort_logs_by_date(self, logs: list):
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
            return self._sort_logs_by_date(less) + equall + self._sort_logs_by_date(greater)

    def _save_logs_to_DB(self, logs: list):
        logger.info('Saving entries to DB..')
        session = self._connect_to_DB()
        try:
            for entry in logs:
                db_entry = self._create_orm_object_from_entry(entry)
                logger.debug(db_entry)
                session.add(db_entry)
            session.commit()
            logger.info('All entries saved successfully')
        except Exception as e:
            session.rollback()
            msg = f'An error occurred while saving: {e}'
            logger.error(msg)
            raise LogsGetterError(msg)
        finally:
            session.close()

    def _create_orm_object_from_entry(self, entry: LogEntry):
        db_entry = LogEntryDB(
            created=entry.created,
            first_name=entry.first_name,
            second_name=entry.second_name,
            message=entry.message,
            user_id=entry.user_id
        )
        return db_entry

    def _connect_to_DB(self):
        engine = create_engine(self._db_string)
        Base.metadata.create_all(engine)
        Session = sessionmaker(engine)
        return Session()


class LogEntryDB(Base):
    __tablename__ = 'logs'
    id = Column(Integer, primary_key=True)
    created = Column(DateTime, nullable=False)
    first_name = Column(String(100))
    second_name = Column(String(100))
    message = Column(Text)
    user_id = Column(String(50))

    def __repr__(self):
        return f'<{self.created}, user: {self.first_name} {self.second_name}, ID: {self.user_id}>'


class HttpRequester:
    def get_content(self, url: str):
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content


class LogsGetterError(Exception):
    pass


def main():
    logs_date = date(2021, 1, 23)
    getter = LogsGetter(LOGS_URL, DB_CONN_STR)
    getter.get_logs(logs_date)


if __name__ == '__main__':
    main()
