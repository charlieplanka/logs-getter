from logsgetter import LogsGetterError
from conftest import OK_CREATED, OK_FIRST_NAME, OK_MESSAGE, OK_SECOND_NAME, OK_USER_ID
from datetime import date, datetime
import pytest


def test_request_logs_from_server_error(getter_error_in_response):
    logs_date = date(2021, 1, 23)
    with pytest.raises(LogsGetterError):
        getter_error_in_response._request_logs_from_server(logs_date)


def test_parse_logs_ok(logs_ok):
    getter, logs = logs_ok
    parsed = getter._parse_logs(logs)
    entry = parsed[0]
    assert entry.created == datetime.fromisoformat(OK_CREATED)
    assert entry.first_name == OK_FIRST_NAME
    assert entry.second_name == OK_SECOND_NAME
    assert entry.message == OK_MESSAGE
    assert entry.user_id == OK_USER_ID


def test_parse_logs_no_logs(logs_no_logs):
    getter, logs = logs_no_logs
    with pytest.raises(LogsGetterError):
        getter._parse_logs(logs)
