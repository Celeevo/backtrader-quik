"""Публичный API backtrader-quik."""
from importlib.resources import files
from .QuikPy import Event, QuikPy
from .QKStore import QKStore
from .QKData import QKData
from .QKBroker import QKBroker
from .logger_config import (
    configure_console_logging,
    logger,
    set_console_logging,
    set_file_logging,
)

__all__ = [
    'Event', 'QuikPy', 'QKStore', 'QKData', 'QKBroker',
    'configure_console_logging', 'set_console_logging',
    'logger', 'set_file_logging', 'quik_connector_path',
]


def quik_connector_path():
    """Return the packaged QUIK connector directory as a Traversable object."""
    return files(__package__).joinpath('QUIK')

__version__ = '1.0.0'
