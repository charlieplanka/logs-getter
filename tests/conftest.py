from logsgetter import LogsGetter, DB_CONN_STR, LOGS_URL
import pytest
from datetime import date

OK_CREATED = '2022-01-23T06:04:27'
OK_FIRST_NAME = '\u0410\u0440\u0442\u0443\u0440'
OK_SECOND_NAME = '\u0428\u0435\u0441\u0442\u0430\u043a\u043e\u0432'
OK_MESSAGE = 'Leave now and never come back'
OK_USER_ID = '530527'

TEST_DATE = date(2021, 1, 1)

mock_content_error = '{"error":"Something went wrong","logs":[]}'
mock_content_ok = '{"error":"","logs":[{"created_at":"%s","first_name":"%s", \
"message":"%s","second_name":"%s","user_id":"%s"}]}' % (
    OK_CREATED,
    OK_FIRST_NAME,
    OK_MESSAGE,
    OK_SECOND_NAME,
    OK_USER_ID
    )

mock_content_no_logs = '{"error":""}'
mock_content_empty_logs = '{"error":"", "logs": []}'


class MockHttpRequestor():
    def __init__(self, content):
        self._content = content

    def get_content(self, url):
        return self._content


def contruct_getter_with_mock(mock_content):
    mock = MockHttpRequestor(mock_content)
    getter = LogsGetter(LOGS_URL, DB_CONN_STR)
    getter._http_requester = mock
    return getter


@pytest.fixture
def logs_ok(request):
    getter = contruct_getter_with_mock(mock_content_ok)
    return getter, getter._request_logs_from_server(TEST_DATE)


@pytest.fixture
def logs_no_logs(request):
    getter = contruct_getter_with_mock(mock_content_no_logs)
    return getter, getter._request_logs_from_server(TEST_DATE)


@pytest.fixture
def logs_empty_logs(request):
    getter = contruct_getter_with_mock(mock_content_empty_logs)
    return getter, getter._request_logs_from_server(TEST_DATE)


@pytest.fixture
def getter_error_in_response(request):
    getter = contruct_getter_with_mock(mock_content_error)
    return getter
