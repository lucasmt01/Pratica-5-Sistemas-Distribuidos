import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    host: str = "localhost"
    port: int = 5672
    user: str = "cidade"
    password: str = "cidade123"
    virtual_host: str = "/"
    db_path: Path = Path(__file__).resolve().parent.parent / "data" / "cidade.db"

    @classmethod
    def from_env(cls):
        return cls(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            user=os.getenv("RABBITMQ_USER", "cidade"),
            password=os.getenv("RABBITMQ_PASSWORD", "cidade123"),
            virtual_host=os.getenv("RABBITMQ_VHOST", "/"),
            db_path=Path(os.getenv("CIDADE_DB", str(cls.db_path))),
        )
