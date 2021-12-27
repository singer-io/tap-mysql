import unittest
import pymysql
from tap_mysql.sync_strategies.common import execute_query
import tap_mysql.connection as connection
from unittest import mock

class MockParseArgs:
    config = {}
    def __init__(self, config):
        self.config = config

def get_args(config):
    return MockParseArgs(config)

class MockedConnection:
    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def ping(*args, **kwargs):
        pass

@mock.patch("singer.utils.parse_args")
class TestTimeoutValue(unittest.TestCase):

    def test_timeout_int_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": 100}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = connection.get_request_timeout()

        # verify that we got expected timeout value
        self.assertEquals(100.0, timeout)
        self.assertTrue(isinstance(timeout, int))

    def test_timeout_value_not_in_config(self, mocked_parse_args):

        mock_config = {}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = connection.get_request_timeout()

        # verify that we got expected timeout value
        self.assertEquals(3600, timeout)
        self.assertTrue(isinstance(timeout, int))

    def test_timeout_decimal_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": 100.1}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        with self.assertRaises(Exception):
            # get the timeout value for assertion
            timeout = connection.get_request_timeout()

    def test_timeout_string_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": "100"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = connection.get_request_timeout()

        # verify that we got expected timeout value
        self.assertEquals(100.0, timeout)
        self.assertTrue(isinstance(timeout, int))

    def test_timeout_string_decimal_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": "100.1"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        with self.assertRaises(Exception):
            # get the timeout value for assertion
            timeout = connection.get_request_timeout()

    def test_timeout_empty_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": ""}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = connection.get_request_timeout()

        # verify that we got expected timeout value
        self.assertEquals(3600, timeout)
        self.assertTrue(isinstance(timeout, int))

    def test_timeout_0_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": 0.0}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = connection.get_request_timeout()

        # verify that we got expected timeout value
        self.assertEquals(3600, timeout)
        self.assertTrue(isinstance(timeout, int))

    def test_timeout_string_0_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": "0"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        # get the timeout value for assertion
        timeout = connection.get_request_timeout()

        # verify that we got expected timeout value
        self.assertEquals(3600, timeout)
        self.assertTrue(isinstance(timeout, int))

    def test_timeout_string_0_decimal_value_in_config(self, mocked_parse_args):

        mock_config = {"request_timeout": "0.0"}
        # mock parse args
        mocked_parse_args.return_value = get_args(mock_config)

        with self.assertRaises(Exception):
            # get the timeout value for assertion
            timeout = connection.get_request_timeout()

@mock.patch("time.sleep")
class TestTimeoutBackoff(unittest.TestCase):

    def test_timeout_backoff(self, mocked_sleep):

        # create mock class of cursor
        cursor = mock.MagicMock()
        # raise timeout error for "cursor.execute"
        cursor.execute.side_effect = pymysql.err.OperationalError(2013, 'Lost connection to MySQL server during query (timed out)')

        try:
            # function call
            execute_query(cursor, "SELECT * from Test", None, MockedConnection)
        except pymysql.err.OperationalError:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(cursor.execute.call_count, 5)

    def test_timeout_error_not_occurred(self, mocked_sleep):

        # create mock class of cursor
        cursor = mock.MagicMock()
        # raise any error other than timeout error for "cursor.execute"
        cursor.execute.side_effect = pymysql.err.OperationalError(2003, 'Can\'t connect to MySQL server on \'localhost\' (111)')

        try:
            # function call
            execute_query(cursor, "SELECT * from Test", None, MockedConnection)
        except pymysql.err.OperationalError:
            pass

        # verify that we did not backoff as timeout error has not occurred
        self.assertEquals(cursor.execute.call_count, 1)
