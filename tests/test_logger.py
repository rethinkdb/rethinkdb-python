import logging

import pytest
from mock import call, patch, ANY
from rethinkdb.logger import DriverLogger


@pytest.mark.unit
class TestDriverLogger(object):
    driver_logger = DriverLogger(logging.DEBUG)
    logger = logging.getLogger()

    def test_converter(self):
        expected_message = 'converted message'

        message_types = [
            Exception(expected_message),
            expected_message
        ]

        for message in message_types:
            converted_message = self.driver_logger._convert_message(message)
            assert converted_message == expected_message

    @patch('rethinkdb.logger.sys.stdout')
    def test_log_write_to_stdout(self, mock_stdout):
        expected_message = 'message'
        log_levels = [logging.DEBUG, logging.INFO, logging.WARNING]
        self.driver_logger.write_to_console = True

        with patch.object(self.logger, 'log') as mock_log:
            for level in log_levels:
                self.driver_logger._log(level, expected_message)
                mock_stdout.write.assert_has_calls([
                    call(expected_message)
                ])

    @patch('rethinkdb.logger.sys.stderr')
    def test_log_write_to_stderr(self, mock_stderr):
        expected_message = 'message'
        self.driver_logger.write_to_console = True

        with patch.object(self.logger, 'log') as mock_log:
            self.driver_logger._log(logging.ERROR, expected_message)
            mock_stderr.write.assert_has_calls([
                call(expected_message)
            ])

    def test_log_debug(self):
        expected_message = 'debug message'

        with patch.object(self.logger, 'log') as mock_log:
            self.driver_logger.debug(expected_message)
            mock_log.assert_called_once_with(logging.DEBUG, expected_message, ANY, ANY)

    def test_log_info(self):
        expected_message = 'info message'

        with patch.object(self.logger, 'log') as mock_log:
            self.driver_logger.info(expected_message)
            mock_log.assert_called_once_with(logging.INFO, expected_message, ANY, ANY)

    def test_log_warning(self):
        expected_message = 'warning message'

        with patch.object(self.logger, 'log') as mock_log:
            self.driver_logger.warning(expected_message)
            mock_log.assert_called_once_with(logging.WARNING, expected_message, ANY, ANY)

    def test_log_error(self):
        expected_message = 'error message'

        with patch.object(self.logger, 'log') as mock_log:
            self.driver_logger.error(expected_message)
            mock_log.assert_called_once_with(logging.ERROR, expected_message, ANY, ANY)

    @patch('rethinkdb.logger.DriverLogger._convert_message')
    def test_log_exception(self, mock_converter):
        expected_message = 'exception message'
        expected_exception = Exception(expected_message)
        mock_converter.return_value = expected_message

        with patch.object(self.logger, 'log') as mock_log:
            try:
                raise expected_exception
            except Exception as exc:
                self.driver_logger.exception(exc)

            mock_converter.assert_called_once_with(expected_exception)
            mock_log.assert_called_once_with(logging.ERROR, expected_message, ANY, {'exc_info':1})
