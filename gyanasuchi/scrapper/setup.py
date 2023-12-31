import logging
from datetime import datetime
from typing import List, TypedDict

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub, nfs_mapping
from gyanasuchi.scrapper.db import YouTubePlaylist, raw_db, YouTubeVideo, YouTubeTranscriptLine

logger = logging.getLogger(__name__)
stub = create_stub(__name__)


class Playlists(TypedDict):
    id: str
    name: str


@stub.function(network_file_systems=nfs_mapping())
def initiate(playlists: List[Playlists]) -> List[YouTubePlaylist]:
    setup_logging()
    run_id = datetime.now()
    models_to_create = [
        YouTubePlaylist, YouTubeVideo, YouTubeVideo.playlists.get_through_model(),
        YouTubeTranscriptLine,
    ]

    logger.info("Connecting to the database")
    raw_db.connect()

    logger.info(f"Dropping tables to be created tables {models_to_create}")
    raw_db.drop_tables(models_to_create)

    logger.info(f"Creating tables {models_to_create}")
    raw_db.create_tables(models_to_create)

    logger.info("Creating playlists")
    return [
        YouTubePlaylist.create(id=playlist["id"], name=playlist["name"], first_inserted_at_run=run_id)
        for playlist in playlists
    ]


@stub.local_entrypoint()
def main() -> None:
    initiate.remote([
        {"id": "PLarGM64rPKBnvFhv7Zgvj2t_q399POBh7", "name": "DevDay_"},
        {"id": "PL1T8fO7ArWleyIqOy37OVXsP4hFXymdOZ", "name": "LLM Bootcamp - Spring 2023"},
    ])
