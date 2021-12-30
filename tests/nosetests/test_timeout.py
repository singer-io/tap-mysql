import unittest
import pymysql
import tap_mysql.connection as connection
from tap_mysql.sync_strategies.common import backoff_timeout_error
from unittest import mock

class MockParseArgs:
    config = {}
    def __init__(self, config):
        self.config = config

def get_args(config):
    return MockParseArgs(config)

class MockedConnection:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def ping(*args, **kwargs):
        pass

    def show_warnings(*args, **kwargs):
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
@mock.patch("singer.utils.parse_args")
class TestTimeoutBackoff(unittest.TestCase):

    @mock.patch("pymysql.cursors.SSCursor.execute")
    def test_timeout_backoff(self, mocked_cursor_execute, mocked_parse_args, mocked_sleep):
        # mock 'cursor.execute' and raise error
        mocked_cursor_execute.side_effect = pymysql.err.OperationalError(2013, 'Lost connection to MySQL server during query (timed out)')
        # add decorator on 'cursor.execute'
        pymysql.cursors.SSCursor.execute = backoff_timeout_error(pymysql.cursors.SSCursor.execute)
        # initialize cursor
        cursor = pymysql.cursors.SSCursor(MockedConnection)
        try:
            # function call
            cursor.execute("SELECT * FROM test")
        except pymysql.err.OperationalError:
            pass

        # verify that we backoff for 5 times
        self.assertEquals(mocked_cursor_execute.call_count, 5)

    @mock.patch("pymysql.cursors.SSCursor.execute")
    def test_timeout_error_not_occurred(self, mocked_cursor_execute, mocked_parse_args, mocked_sleep):
        # mock 'cursor.execute' and raise error
        mocked_cursor_execute.side_effect = pymysql.err.OperationalError(2003, 'Can\'t connect to MySQL server on \'localhost\' (111)')
        # add decorator on 'cursor.execute'
        pymysql.cursors.SSCursor.execute = backoff_timeout_error(pymysql.cursors.SSCursor.execute)
        # initialize cursor
        cursor = pymysql.cursors.SSCursor(MockedConnection)
        try:
            # function call
            cursor.execute("SELECT * FROM test")
        except pymysql.err.OperationalError:
            pass

        # verify that we did not backoff as timeout error has not occurred
        self.assertEquals(mocked_cursor_execute.call_count, 1)
