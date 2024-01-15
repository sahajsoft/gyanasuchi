import logging
from pathlib import Path

from modal import NetworkFileSystem
from peewee import DateTimeField
from peewee import FixedCharField
from peewee import FloatField
from peewee import ForeignKeyField
from peewee import ManyToManyField
from peewee import Model
from peewee import SqliteDatabase

from gyanasuchi.common import data_volume_dir

logger = logging.getLogger(__name__)
data_volume = NetworkFileSystem.persisted("data")
raw_db = SqliteDatabase(Path(data_volume_dir, "raw.db"))


class BaseModel(Model):
    class Meta:
        database = raw_db


class YouTubePlaylist(BaseModel):
    id = FixedCharField(40, primary_key=True)
    name = FixedCharField(1000)
    first_inserted_at_run = DateTimeField()


class YouTubeVideo(BaseModel):
    id = FixedCharField(20, primary_key=True)
    first_inserted_at_run = DateTimeField()
    fetched_transcripts_at_run = DateTimeField(null=True)
    playlists = ManyToManyField(YouTubePlaylist, backref="videos")


class YouTubeTranscriptLine(BaseModel):
    video = ForeignKeyField(YouTubeVideo, backref="transcript_lines")
    text = FixedCharField(1000)
    start = FloatField()
    duration = FloatField()
    first_inserted_at_run = DateTimeField()
