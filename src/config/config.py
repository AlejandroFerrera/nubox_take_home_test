import os
from dataclasses import dataclass

@dataclass
class Config:
    """
    Configuration class for application settings.
    """

    # Database settings
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "")

    # API settings
    OPENAQ_API_BASE_URL: str = os.getenv(
        "OPENAQ_API_BASE_URL", "https://api.openaq.org"
    )

    OPENAQ_API_KEY: str = os.getenv("OPENAQ_API_KEY", "")
    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "30"))

    # Application settings
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")


config = Config()
