from logsgetter import LogsGetterError
from conftest import OK_CREATED, OK_FIRST_NAME, OK_MESSAGE, OK_SECOND_NAME, OK_USER_ID, TEST_DATE
from datetime import datetime
import pytest


def test_parse_logs_ok(logs_ok):
    '''Tests if entry is parsed correctly.'''
    getter, logs = logs_ok
    parsed = getter._parse_logs(logs)
    entry = parsed[0]
    assert entry.created == datetime.fromisoformat(OK_CREATED)
    assert entry.first_name == OK_FIRST_NAME
    assert entry.second_name == OK_SECOND_NAME
    assert entry.message == OK_MESSAGE
    assert entry.user_id == OK_USER_ID


def test_parse_logs_no_logs(logs_no_logs):
    '''Tests if LogsGetterError is raised when 'logs' key is absent.'''
    getter, logs = logs_no_logs
    with pytest.raises(LogsGetterError):
        getter._parse_logs(logs)


def test_parse_logs_empty_logs(logs_empty_logs):
    '''Tests if empty list is returned when 'logs' key is empty.'''
    getter, logs = logs_empty_logs
    logs = getter._parse_logs(logs)
    assert logs == []


def test_request_logs_from_server_error(getter_error_in_response):
    '''Tests if LogsGetterError is raised when 'error' key has content.'''
    with pytest.raises(LogsGetterError):
        getter_error_in_response._request_logs_from_server(TEST_DATE)
