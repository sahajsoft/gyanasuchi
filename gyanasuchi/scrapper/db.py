import logging
import os

from sqlalchemy import Column, String, Engine, create_engine, PrimaryKeyConstraint, DateTime
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()
logger = logging.getLogger(__name__)


class YouTubePlaylist(Base):
    __tablename__ = 'YOUTUBE_PLAYLISTS'

    id = Column(String(40), primary_key=True)
    name = Column(String(100))

    def __repr__(self):
        return f'YouTubePlaylist(id="{self.id}", name="{self.name}")'


class YouTubePlaylistVideo(Base):
    __tablename__ = "YOUTUBE_VIDEO"

    playlist_id = Column(String(40), primary_key=True)
    video_id = Column(String(20), primary_key=True)
    first_inserted_at_run = Column(DateTime, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('playlist_id', 'video_id'),
    )

    def __repr__(self):
        return f'YouTubePlaylistVideo(playlist_id="{self.playlist_id}", video_id="{self.video_id}")'


def env(key: str, default: str = None) -> str:
    return os.environ.get(key, default)


def db_engine() -> Engine:
    user = env("DATABASE_USERNAME")
    password = env("DATABASE_PASSWORD")
    host = env("DATABASE_HOST")
    database_name = env("DATABASE_NAME")

    return create_engine(
        url=f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{database_name}",
        echo=True
    )


def insert_if_not_dupe(session: Session, obj: Base) -> bool:
    try:
        session.add(obj)
        session.commit()
    except IntegrityError as e:
        logger.warning(f"Error in inserting {obj}: {e}")
        session.rollback()
        return False

    return True
