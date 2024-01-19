import logging
import os
from datetime import datetime
from typing import List
from typing import TypedDict

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.modal import nfs_mapping
from gyanasuchi.scrapper.db import raw_db
from gyanasuchi.scrapper.db import YouTubePlaylist
from gyanasuchi.scrapper.db import YouTubeTranscriptLine
from gyanasuchi.scrapper.db import YouTubeVideo

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
        YouTubePlaylist,
        YouTubeVideo,
        YouTubeVideo.playlists.get_through_model(),
        YouTubeTranscriptLine,
    ]

    if os.path.exists(raw_db.database):
        logger.info("Removing existing database")
        raw_db.database.unlink()

    logger.info("Connecting to the database")
    raw_db.connect()

    logger.info(f"Creating tables {models_to_create}")
    raw_db.create_tables(models_to_create)

    logger.info("Creating playlists")
    return [
        YouTubePlaylist.create(
            id=playlist["id"],
            name=playlist["name"],
            first_inserted_at_run=run_id,
        )
        for playlist in playlists
    ]


@stub.local_entrypoint()
def main() -> None:
    initiate.remote(
        [
            {"id": "PLarGM64rPKBnvFhv7Zgvj2t_q399POBh7", "name": "DevDay_"},
            {
                "id": "PL1T8fO7ArWleyIqOy37OVXsP4hFXymdOZ",
                "name": "LLM Bootcamp - Spring 2023",
            },
        ],
    )
