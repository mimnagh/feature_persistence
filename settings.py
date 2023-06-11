from pydantic import BaseSettings

class AppSettings(BaseSettings):
    persistence_type: str = "local_file"
    storage_path: str = "data"

def get_settings() -> AppSettings:
    return AppSettings()
