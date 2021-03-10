from datetime import date, datetime
import random
import json
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine
from sqlalchemy.orm import sessionmaker
import requests
from requests.exceptions import HTTPError
from logger import logger

LOGS_URL = 'http://www.dsdev.tech/logs/'
DB_CONN_STR = 'postgresql+psycopg2://graffit:graffit@localhost/graffit_logs'

BASE = declarative_base()


class LogEntry:
    '''Represents log entry.'''
    def __init__(self, created: str, first_name=None, second_name=None, message=None, user_id=None):
        self.created = datetime.fromisoformat(created)
        self.first_name = first_name
        self.second_name = second_name
        self.message = message
        self.user_id = user_id


class LogsGetter:
    '''Saves logs from server to DB.

    Args:
        url: Server URL.
        db_connection_string: SQLAlchemy connection string.
    '''
    def __init__(self, url: str, db_connection_string: str):
        self._url = url
        self._db_string = db_connection_string
        self._http_requester = HttpRequester()

        # prepare ORM
        engine = create_engine(self._db_string)
        BASE.metadata.create_all(engine)
        self._sessionmaker = sessionmaker(engine)

    def get_logs(self, log_date: date):
        '''Gets logs for a certain date from server and saves to DB.

        Args:
            log_date: Date of logs.

        Returns:
            None

        Raises:
            TypeError: If log_date is not datetime.date instance.
        '''
        if not isinstance(log_date, date):
            raise TypeError('Parameter "log_date" should be datetime.date instance')

        logs = self._request_logs_from_server(log_date)
        parsed_logs = self._parse_logs(logs)
        if not parsed_logs:
            return
        sorted_logs = LogsGetter._sort_logs_by_date(parsed_logs)
        self._save_logs_to_DB(sorted_logs)

    def _request_logs_from_server(self, date: date):
        '''Requests logs for a certain date from server.

        Args:
            date: Date of logs.

        Returns:
            Dictionary with logs.

        Raises:
            LogsGetterError: If HTTP-error is occurred,
        or server responded with an error ('error' key in response is not empty).
        '''
        logger.info(f'Requesting {date} logs from server..')
        url = self._construct_request_url(date)

        try:
            response = self._http_requester.get_content(url)
        except HTTPError as e:
            msg = f'An HTTP-error occurred: {e}'
            logger.error(msg)
            raise LogsGetterError(msg)

        logs = json.loads(response)
        error = logs.get('error')
        if error is None:
            logger.warning('No "error" key in response')
        elif error:
            msg = f'Server responded with an error: {error}'
            logger.error(msg)
            raise LogsGetterError(msg)

        return logs

    def _parse_logs(self, logs: dict):
        '''Parses logs and creates a list with LogEntry objects.

        Args:
            logs: Dictionary with logs from server.

        Returns:
            List with LogEntry objects.

        Raises:
            LogsGetterError: If 'logs' key is absent in dictionary.
        '''
        logs = logs.get('logs')
        if logs is None:
            msg = 'Failed to get logs: no "logs" key in response'
            logger.error(msg)
            raise LogsGetterError(msg)
        elif not logs:
            msg = 'There are no logs for requested date'
            logger.info(msg)
            return logs

        parsed = []
        for entry in logs:
            entry_obj = self._create_entry_object(entry)
            if not entry_obj:
                continue
            parsed.append(entry_obj)

        logger.info(f'Total records: {len(parsed)}')
        return parsed

    def _create_entry_object(self, entry: dict):
        '''Parses entry dictionary and constructs LogEntry object from it.

        Args:
            entry: Dictionary with entry fields.

        Returns:
            LogEntry object.
        '''
        created = entry.get('created_at')
        if not created:
            msg = 'Entry field "created_at" is empty or absent. This field is required. Entry skipped'
            logger.error(msg)
            logger.debug(entry)
            return None

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
            return None

        return entry_obj

    def _save_logs_to_DB(self, logs: list):
        '''Saves entries to database.

        Args:
            logs: List of LogEntry objects.

        Returns:
            None

        Raises:
            LogsGetterError: If something went wrong while saving.
        '''
        logger.info('Saving entries to DB..')
        session = self._connect_to_DB()
        try:
            for entry in logs:
                db_entry = LogsGetter._create_orm_object_from_entry(entry)
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

    def _connect_to_DB(self):
        '''Connects to database.

        Returns:
            DB session object.
        '''
        return self._sessionmaker()

    def _construct_request_url(self, date: date):
        '''Constructs URL for request using date.

        Args:
            date: Date of logs.

        Returns:
            String with URL.
        '''
        date_formatted = date.strftime('%Y%m%d')
        url = '{}{}'.format(self._url, date_formatted)
        return url

    @staticmethod
    def _sort_logs_by_date(logs: list):
        '''Sorts entry objects by date in ascending order using quick sorting algorithm.

        Args:
            logs: List with LogEntry objects.

        Returns:
            Sorted list of LogEntry objects.
        '''
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
            return LogsGetter._sort_logs_by_date(less) + equall + LogsGetter._sort_logs_by_date(greater)

    @staticmethod
    def _create_orm_object_from_entry(entry: LogEntry):
        '''Creates ORM object from entry.

        Args:
            entry: LogEntry object.

        Returns:
            LogEntryDB object.
        '''
        db_entry = LogEntryDB(
            created=entry.created,
            first_name=entry.first_name,
            second_name=entry.second_name,
            message=entry.message,
            user_id=entry.user_id
        )
        return db_entry


class LogEntryDB(BASE):
    '''ORM class representing table with log entries.
    '''
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
    '''Sends http-requests.'''
    def get_content(self, url: str):
        '''Sends GET-request and gets content from response.

        Args:
            url: URL for request.

        Returns:
            Response content.
        '''
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content


class LogsGetterError(Exception):
    '''Custom exception to raise from LogsGetter.
    '''
    pass


def main():
    logs_date = date(2021, 1, 23)
    getter = LogsGetter(LOGS_URL, DB_CONN_STR)
    getter.get_logs(logs_date)


if __name__ == '__main__':
    main()
