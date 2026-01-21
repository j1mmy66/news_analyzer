from enum import Enum


class SourceType(str, Enum):
    RBC = "rbc"


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


class ClassLabel(str, Enum):
    POLITICS = "politics"
    ECONOMY = "economy"
    BUSINESS = "business"
    SOCIETY = "society"
    TECHNOLOGY = "technology"
    SPORTS = "sports"
    OTHER = "other"
