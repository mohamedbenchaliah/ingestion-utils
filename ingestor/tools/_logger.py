import logging
import sys
from io import StringIO


class Log:
    def __init__(self, level="INFO", app_name=__name__):
        """Logging configuration.
        - Default logging level set to INFO
        - Application name is filename by default.
        - Logs will be streamed to stdout
        - Log formatting is [LOGGER] DATETIME LEVELNAME APPNAME MESSAGE
        - An instance must be created after import in the target module."""

        # clear the existing handlers if any
        self.logger = logging.getLogger()

        # setup _logger
        logformat = "[LOGGER] %(asctime)-s %(levelname)-8s %(name)-50s %(message)s"
        dateformat = "%Y-%m-%d %H:%M:%S"
        logging.basicConfig(level=level.upper(), stream=sys.stdout, format=logformat, datefmt=dateformat)
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(level.upper())

        # add stringIO as a handler for PyTest
        self.log_string_io = StringIO()
        self.handler = logging.StreamHandler(self.log_string_io)
        self.handler.setFormatter(logging.Formatter(logformat, dateformat))
        self.logger.addHandler(self.handler)

    def set_level(self, level):
        """Change the level. Correct usage is to use upper case."""

        self.logger.setLevel(level.upper())

    def set_app_name(self, app_name):
        """Change the application name while maintaining the level."""

        level = self.logger.level
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(level)

    def critical(self, message_text):
        """Log Critical level."""

        self.logger.critical(message_text)
        self.logger.critical("Stop execution due to CRITICAL level..")
        sys.exit(1)

    def error(self, message_text):
        """Log Error level."""

        self.logger.error(message_text)

    def warning(self, message_text):
        """Log Warning level."""

        self.logger.warning(message_text)

    def info(self, message_text):
        """Log Info level."""

        self.logger.info(message_text)

    def debug(self, message_text):
        """DEBUG level needs to be identified explicitly in order to be logged."""

        self.logger.debug(message_text)
