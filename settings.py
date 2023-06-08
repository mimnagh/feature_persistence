from pydantic import BaseSettings

class AppSettings(BaseSettings):
    persistence_type: str

def get_settings() -> AppSettings:
    return AppSettings(".env")
