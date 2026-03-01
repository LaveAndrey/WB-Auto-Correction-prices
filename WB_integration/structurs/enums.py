from enum import Enum


class ProcessingStatus(Enum):
    SUCCESS = "success"
    SKIPPED_NO_DATA = "skipped_no_data"
    SKIPPED_MIN_PRICE = "skipped_min_price"
    SKIPPED_MIN_CHANGE = "skipped_min_change"
    SKIPPED_INVALID = "skipped_invalid"
    ERROR = "error"
    SKIPPED_PROMOTION = "skipped_promotion"