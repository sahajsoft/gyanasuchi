import os

from sqlalchemy import Column, String, Engine, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class YouTubePlaylist(Base):
    __tablename__ = 'YOUTUBE_PLAYLISTS'

    id = Column(String(40), primary_key=True)
    name = Column(String(100))

    def __repr__(self):
        return f'YouTubePlaylist(id="{self.id}", name="{self.name}")'


def env(key: str, default: str = None) -> str:
    return os.environ.get(key, default)


def db_engine() -> Engine:
    user = env("DATABASE_USERNAME")
    password = env("DATABASE_PASSWORD")
    host = env("DATABASE_HOST")
    database_name = env("DATABASE_NAME")

    db_connection = f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{database_name}"
    return create_engine(db_connection, echo=True)
