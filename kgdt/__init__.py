"""Top-level package for Knowledge Graph Data Tool."""
import logging

logger = logging.getLogger('kgdt')
if not logger.handlers:  # To ensure reload() doesn't add another one
    logger.addHandler(logging.NullHandler())

__author__ = """Software Engineering Laboratory of Fudan University"""
__email__ = 'lmwtclmwtc@outlook.com'
__version__ = '0.2.0'
