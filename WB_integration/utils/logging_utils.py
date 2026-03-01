import logging
import logging.handlers
import sys
from datetime import datetime
from config import Config
import pytz
import os


class MoscowTimeFormatter(logging.Formatter):
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


def setup_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    """
    Настройка логгера с безопасной обработкой файлов для Windows
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Очищаем существующие обработчики
    logger.handlers.clear()

    # Форматтер
    formatter = logging.Formatter(
        Config.LOG_FORMAT,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Консольный вывод
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Файловый вывод (без ротации для Windows)
    if log_file:
        try:
            # Для Windows используем простой FileHandler вместо RotatingFileHandler
            # чтобы избежать проблем с блокировкой файлов
            file_handler = logging.FileHandler(
                log_file,
                encoding='utf-8',
                mode='a'  # append mode
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

            # Создаем отдельный файл для даты, чтобы логи не смешивались
            dated_log_file = log_file.replace('.log', f'_{datetime.now().strftime("%Y%m%d")}.log')
            if not os.path.exists(dated_log_file):
                # Если есть старый лог-файл, копируем его содержимое в датированный
                if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
                    try:
                        with open(log_file, 'r', encoding='utf-8') as src:
                            content = src.read()
                        with open(dated_log_file, 'w', encoding='utf-8') as dst:
                            dst.write(content)
                        # Очищаем основной файл
                        open(log_file, 'w').close()
                    except:
                        pass

        except Exception as e:
            logger.warning(f"Не удалось создать файловый логгер: {e}")

    return logger


def log_separator(logger: logging.Logger, title: str = "", char: str = "=", length: int = 80):
    """Разделитель с заголовком"""
    try:
        logger.info(char * length)
        if title:
            logger.info(f"{title.center(length)}")
            logger.info(char * length)
    except Exception:
        # Игнорируем ошибки логирования
        pass


def log_table(logger: logging.Logger, title: str, data: dict):
    """Логирование таблицы данных"""
    try:
        logger.info(f"📊 {title}:")
        if data:
            max_key_len = max(len(str(k)) for k in data.keys())
            for key, value in data.items():
                value_str = f"{value:,.2f}" if isinstance(value, (int, float)) else str(value)
                logger.info(f"  {key:<{max_key_len}} : {value_str}")
        log_separator(logger, "-", "-")
    except Exception:
        # Игнорируем ошибки логирования
        pass