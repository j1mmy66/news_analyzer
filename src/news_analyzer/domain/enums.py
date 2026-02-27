from enum import Enum


class SourceType(str, Enum):
    RBC = "rbc"


class ProcessingStatus(str, Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"


class ClassLabel(str, Enum):
    CLIMATE = "climate"
    CONFLICTS = "conflicts"
    CULTURE = "culture"
    POLITICS = "politics"
    ECONOMY = "economy"
    GLOSS = "gloss"
    HEALTH = "health"
    SCIENCE = "science"
    SOCIETY = "society"
    SPORTS = "sports"
    TRAVEL = "travel"
    OTHER = "other"
