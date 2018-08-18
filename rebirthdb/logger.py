# Copyright 2018 RebirthDB
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Wrap logging package to not repeat general logging steps.
"""


import logging


class DriverLogger(object):
    """
    DriverLogger is a wrapper for logging's debug, info, warning and error functions.
    """

    def __init__(self, level=logging.INFO):
        """
        Initialize DriverLogger

        :param level: Minimum logging level
        :type level: int
        """

        super(DriverLogger, self).__init__()
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(format=log_format)

        self.log = logging.getLogger()
        self.log.setLevel(level)

    @staticmethod
    def _convert_message(message):
        """
        Convert any message to string.

        :param message: Message to log
        :type message: any
        :return: String representation of the message
        :rtype: str
        """

        return str(message)

    def debug(self, message):
        """
        Log debug messages.

        :param message: Debug message
        :type message: str
        :rtype: None
        """

        self.log.debug(message)

    def info(self, message):
        """
        Log info messages.

        :param message: Info message
        :type message: str
        :rtype: None
        """

        self.log.info(message)

    def warning(self, message):
        """
        Log warning messages.

        :param message: Warning message
        :type message: str
        :rtype: None
        """

        self.log.warning(message)

    def error(self, message):
        """
        Log error messages.

        :param message: Error message
        :type message: str
        :rtype: None
        """

        self.log.error(message)

    def exception(self, message):
        """
        Log an exception with its traceback and the message if possible.

        :param message: Exception message
        :type message: str
        :rtype: None
        """

        self.log.exception(self._convert_message(message))


default_logger = DriverLogger()
