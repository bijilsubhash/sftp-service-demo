import logging

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


class Logger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message, exc_info=True)
