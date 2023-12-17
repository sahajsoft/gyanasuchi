import logging


def setup_logging() -> None:
    return logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s - %(message)s', level=logging.INFO)
