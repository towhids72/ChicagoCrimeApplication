import logging


class LogUtils:
    """A class to get logging utils"""
    @staticmethod
    def get_logger(
            logger_name: str,
            level=logging.INFO,
            log_format: str = '[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
    ):
        """Set a python logging object with given arguments.

        Args:
            logger_name (str): A name for the logger
            level: Logging level, default is logging.INFO
            log_format: A string that shows how to log, default is [%(levelname) 5s/%(asctime)s] %(name)s: %(message)s

        Return:
            A logger object
        """

        logging.basicConfig(level=level, format=log_format)
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)
        return logger
