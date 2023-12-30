import logging
import os
from typing import Any, Iterator

import nanoid
from dotenv import load_dotenv
from sqlalchemy import Column, String, Engine, create_engine, PrimaryKeyConstraint, DateTime, Float, ForeignKey
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()
logger = logging.getLogger(__name__)


def typed_str(value: Any) -> str:
    if value is None:
        return "None"
    elif type(value) in [float, int, Iterator]:
        return str(value)

    return f'"{value}"'


class YouTubePlaylist(Base):
    __tablename__ = 'YOUTUBE_PLAYLISTS'

    id = Column(String(40), primary_key=True)
    name = Column(String(100))
    first_inserted_at_run = Column(DateTime, nullable=False)

    def __repr__(self):
        return f'YouTubePlaylist(id={typed_str(self.id)}, name={typed_str(self.name)})'


class YouTubeVideo(Base):
    __tablename__ = "YOUTUBE_VIDEO"

    id = Column(String(20), primary_key=True)
    first_inserted_at_run = Column(DateTime, nullable=False)
    fetched_transcripts_at_run = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return 'YouTubeVideo(' \
               f'id={typed_str(self.id)}, ' \
               f'first_inserted_at_run={typed_str(self.first_inserted_at_run)}, ' \
               f'fetched_transcripts_at_run={typed_str(self.fetched_transcripts_at_run)})'


class YouTubePlaylistVideo(Base):
    __tablename__ = "YOUTUBE_PLAYLIST_TO_VIDEOS"

    playlist_id = Column(String(40), ForeignKey('YOUTUBE_PLAYLISTS.id'), primary_key=True)
    video_id = Column(String(20), ForeignKey('YOUTUBE_VIDEO.id'), primary_key=True)
    first_inserted_at_run = Column(DateTime, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint('playlist_id', 'video_id'),
    )

    def __repr__(self):
        return f'YouTubePlaylistVideo(playlist_id={typed_str(self.playlist_id)}, video_id={typed_str(self.video_id)})'


class YouTubeTranscriptLine(Base):
    __tablename__ = "YOUTUBE_TRANSCRIPT_LINE"

    id = Column(String(40), primary_key=True, default=lambda: nanoid.generate(size=12))
    video_id = Column(String(20), index=True)
    text = Column(String(1000))
    start = Column(Float(2), nullable=False)
    duration = Column(Float(2), nullable=False)
    first_inserted_at_run = Column(DateTime, nullable=False)

    def __repr__(self):
        return 'YouTubeTranscriptLine(' \
               f'video_id={typed_str(self.video_id)}, ' \
               f'text={typed_str(self.text)}, ' \
               f'start={typed_str(self.start)}, ' \
               f'duration={typed_str(self.duration)})'


def env(key: str, default: str = None) -> str:
    return os.environ.get(key, default)


def db_engine(echo: bool = True) -> Engine:
    load_dotenv()
    user = env("DATABASE_USERNAME")
    password = env("DATABASE_PASSWORD")
    host = env("DATABASE_HOST")
    database_name = env("DATABASE_NAME")

    return create_engine(
        url=f"mysql+mysqlconnector://{user}:{password}@{host}:3306/{database_name}",
        echo=echo,
        echo_pool=True,
    )


def insert_if_not_dupe(session: Session, obj: Base) -> bool:
    try:
        session.add(obj)
        session.commit()
    except IntegrityError as e:
        logger.warning(f"Unable to insert {obj}: {e}")
        session.rollback()
        return False

    return True
