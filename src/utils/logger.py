"""Simple logging configuration helpers."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a standard library logger."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return logging.getLogger(name)

