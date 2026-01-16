from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings"""
    
    # API Settings
    taxi_zones_base_url : str = "https://data.cityofnewyork.us/resource/8meu-9t5y.json"

    base_url: str = Field(
        default="https://data.cityofnewyork.us/resource/u253-aew4.json"
    )
    offset: int = Field(default=0)
    read_offset_from_db: bool = False
    autoupdate_offset: bool = False
    limit: int = Field(default=1000)
    timeout: int = Field(default=40)
    app_token: str | None = None
    
    # Database Settings
    db_host: str
    db_port: int = Field(default=25060)
    db_database: str
    db_user: str
    db_password: str
    
    # Retry Settings
    retry_attempts: int = Field(default=3)
    retry_wait: int = Field(default=2)
    
    # Logging
    log_level: str = Field(default="INFO")
    
    model_config = SettingsConfigDict(
        env_file="/home/stanislav/Desktop/Data Science projects/de-nyc-taxi-etl/.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow"
    )
    
    @property
    def database_url(self) -> str:
        """Generate database connection string"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}?sslmode=require"

    def db_conn_params(self) -> dict:
        """Get connection parameters for psycopg2"""
        return {
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_database,
            "user": self.db_user,
            "password": self.db_password,
            "sslmode": "require"
        }

settings = Settings()



