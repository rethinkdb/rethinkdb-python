import logging


class DriverLogger(object):
    def __init__(self, level=logging.INFO):
        super(DriverLogger, self).__init__()
        log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(format=log_format)

        self.log = logging.getLogger()
        self.log.setLevel(level)

    def debug(self, message):
        self.log.debug(message)

    def info(self, message):
        self.log.info(message)

    def warning(self, message):
        self.log.info(message)

    def error(self, message):
        self.log.info(message)


default_logger = DriverLogger()
