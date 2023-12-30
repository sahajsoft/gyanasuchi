from datetime import datetime
from typing import List

from sqlalchemy import Engine
from sqlalchemy.orm import Session

from gyanasuchi.common import setup_logging
from gyanasuchi.scrapper.db import YouTubePlaylist, Base, insert_if_not_dupe, db_engine


def initiate(engine: Engine, session: Session) -> List[YouTubePlaylist]:
    run_id = datetime.now()
    Base.metadata.create_all(bind=engine)
    playlists = [
        YouTubePlaylist(id="PLarGM64rPKBnvFhv7Zgvj2t_q399POBh7", name="DevDay_", first_inserted_at_run=run_id),
        YouTubePlaylist(id="PL1T8fO7ArWleyIqOy37OVXsP4hFXymdOZ", name="LLM Bootcamp - Spring 2023",
                        first_inserted_at_run=run_id)
    ]

    [insert_if_not_dupe(session, playlist) for playlist in playlists]

    return playlists


if __name__ == "__main__":
    setup_logging()
    e = db_engine()

    with Session(e) as s:
        initiate(e, s)
