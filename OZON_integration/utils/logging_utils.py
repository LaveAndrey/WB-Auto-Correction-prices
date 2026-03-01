#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Утилиты для логирования
"""

import logging
import logging.handlers
import sys
from datetime import datetime

import pytz


class MoscowTimeFormatter(logging.Formatter):
    """Форматтер логов с московским временем"""

    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt, datefmt)
        self.moscow_tz = pytz.timezone('Europe/Moscow')

    def formatTime(self, record, datefmt=None):
        dt_utc = datetime.utcfromtimestamp(record.created).replace(tzinfo=pytz.utc)
        dt_moscow = dt_utc.astimezone(self.moscow_tz)
        if datefmt:
            return dt_moscow.strftime(datefmt)
        else:
            return dt_moscow.isoformat()


def setup_logger(name: str, log_file: str = None) -> logging.Logger:
    """
    Настройка логгера с файловым и консольным выводом

    Args:
        name: Имя логгера
        log_file: Путь к файлу логов (опционально)

    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Удаляем существующие хендлеры
    logger.handlers.clear()

    formatter = MoscowTimeFormatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Консольный хендлер
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # Файловый хендлер (если указан)
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,
            backupCount=10,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)

    return logger