"""Logging helpers with no import-time filesystem side effects."""
from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

logger = logging.getLogger('backtrader_quik')
provider_logger = logging.getLogger('QuikPy')

for _logger in (logger, provider_logger):
    if not _logger.handlers:
        _logger.addHandler(logging.NullHandler())

_file_handlers: dict[Path, TimedRotatingFileHandler] = {}
_console_handler: logging.Handler | None = None


def _coerce_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    value = logging.getLevelName(str(level).upper())
    if not isinstance(value, int):
        raise ValueError(f'Неизвестный уровень логирования: {level!r}')
    return value


def configure_console_logging(level: int | str = logging.INFO) -> logging.Handler:
    """Enable one package-owned console handler and return it."""
    global _console_handler
    numeric_level = _coerce_level(level)
    if _console_handler is None:
        handler = logging.StreamHandler()
        handler._btq_console = True  # type: ignore[attr-defined]
        handler.setFormatter(logging.Formatter(
            '{asctime} - {name} - {levelname} - {message}',
            style='{', datefmt='%d-%m-%y %H:%M:%S',
        ))
        _console_handler = handler
    _console_handler.setLevel(numeric_level)
    for lg in (logger, provider_logger):
        lg.setLevel(logging.DEBUG)
        if _console_handler not in lg.handlers:
            lg.addHandler(_console_handler)
        lg.propagate = False
    return _console_handler


def set_console_logging(
    enable: bool = True,
    level: int | str = logging.INFO,
) -> logging.Handler | None:
    """Backward-compatible console logging switch.

    ``configure_console_logging`` remains the preferred explicit API.
    """
    global _console_handler
    if enable:
        return configure_console_logging(level)
    if _console_handler is not None:
        for lg in (logger, provider_logger):
            if _console_handler in lg.handlers:
                lg.removeHandler(_console_handler)
        _console_handler.close()
        _console_handler = None
    return None


def _resolve_log_path(
    path: str | Path | None,
    logs_dir: str | Path | None,
) -> Path:
    if logs_dir is not None:
        return Path(logs_dir).expanduser() / 'app.log'
    if path is None:
        return Path.home() / '.backtraderquik' / 'logs' / 'app.log'
    candidate = Path(path).expanduser()
    # Compatibility with the developer build where the second positional
    # argument represented a directory rather than a file path.
    if candidate.suffix == '' or (candidate.exists() and candidate.is_dir()):
        candidate = candidate / 'app.log'
    return candidate


def set_file_logging(
    enable: bool = True,
    path: str | Path | None = None,
    level: int | str = logging.DEBUG,
    *,
    logs_dir: str | Path | None = None,
) -> Path | None:
    """Enable or disable package-owned rotating file logging.

    The default is ``~/.backtraderquik/logs/app.log``. Files/directories are
    created only after an explicit call with ``enable=True``.

    ``logs_dir=...`` and a directory passed as the second positional argument
    are supported for compatibility with the developer ``1.0.0a1`` API.
    """
    log_path = _resolve_log_path(path, logs_dir).resolve()
    numeric_level = _coerce_level(level)
    if enable:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = _file_handlers.get(log_path)
        if handler is None:
            handler = TimedRotatingFileHandler(
                log_path, when='midnight', encoding='utf-8',
            )
            handler._btq_file = True  # type: ignore[attr-defined]
            handler.setFormatter(logging.Formatter(
                '{asctime} - {name} - {filename}:{lineno} - {message}',
                style='{', datefmt='%d-%m-%y %H:%M:%S',
            ))
            _file_handlers[log_path] = handler
        handler.setLevel(numeric_level)
        for lg in (logger, provider_logger):
            lg.setLevel(logging.DEBUG)
            if handler not in lg.handlers:
                lg.addHandler(handler)
            lg.propagate = False
        return log_path

    for registered_path, handler in tuple(_file_handlers.items()):
        if path is not None or logs_dir is not None:
            if registered_path != log_path:
                continue
        for lg in (logger, provider_logger):
            if handler in lg.handlers:
                lg.removeHandler(handler)
        handler.close()
        _file_handlers.pop(registered_path, None)
    return None
