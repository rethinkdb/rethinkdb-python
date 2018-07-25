import logging

import pytest
from mock import patch
from rebirthdb.logger import DriverLogger


@pytest.mark.unit
class TestDriverLogger(object):
    driver_logger = DriverLogger(logging.DEBUG)
    logger = logging.getLogger()

    def test_log_debug(self):
        expected_message = 'debug message'

        with patch.object(self.logger, 'debug') as mock_debug:
            self.driver_logger.debug(expected_message)
            mock_debug.assert_called_once_with(expected_message)

    def test_log_info(self):
        expected_message = 'info message'

        with patch.object(self.logger, 'info') as mock_info:
            self.driver_logger.info(expected_message)
            mock_info.assert_called_once_with(expected_message)

    def test_log_warning(self):
        expected_message = 'warning message'

        with patch.object(self.logger, 'warning') as mock_warning:
            self.driver_logger.warning(expected_message)
            mock_warning.assert_called_once_with(expected_message)

    def test_log_error(self):
        expected_message = 'error message'

        with patch.object(self.logger, 'error') as mock_error:
            self.driver_logger.error(expected_message)
            mock_error.assert_called_once_with(expected_message)

    def test_log_exception(self):
        expected_message = 'exception message'

        with patch.object(self.logger, 'exception') as mock_exception:
            try:
                raise Exception(expected_message)
            except Exception as e:
                self.driver_logger.exception(str(e))

            mock_exception.assert_called_once_with(expected_message)
