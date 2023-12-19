import logging
from datetime import datetime
from typing import List, TypedDict, Type

from dotenv import load_dotenv
from sqlalchemy import Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import Session
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.scrapper.db import YouTubeTranscriptLine, db_engine, YouTubeVideo

stub = create_stub(__name__)
logger = logging.getLogger(__name__)


class TranscriptLine(TypedDict):
    text: str
    start: float
    duration: float


def fetch_transcript(video_id: str) -> List[TranscriptLine]:
    logger.info(f'fetching transcripts for {video_id}')
    try:
        return YouTubeTranscriptApi.get_transcript(video_id)
    except (TranscriptsDisabled, NoTranscriptFound):
        return []


def fetch_transcript_if_not_in_db(video_id: str, run_id: datetime, session: Session) -> List[TranscriptLine]:
    transcripts_from_db = session.query(YouTubeTranscriptLine).filter_by(video_id=video_id).all()

    if transcripts_from_db:
        logger.warning(f"Skipping {video_id} because transcripts already exists")
        return []

    return [
        YouTubeTranscriptLine(video_id=video_id, text=line['text'], start=line['start'], duration=line['duration'],
                              first_inserted_at_run=run_id)
        for line in fetch_transcript(video_id)
    ]


def fetch_videos_without_transcripts(engine: Engine) -> List[Type[YouTubeVideo]]:
    with Session(engine) as session:
        return session.query(YouTubeVideo) \
            .filter_by(fetched_transcripts_at_run=None) \
            .all()


@stub.local_entrypoint()
def main() -> None:
    load_dotenv()
    setup_logging()
    engine = db_engine()
    run_id = datetime.now()

    for video in fetch_videos_without_transcripts(engine):
        with Session(engine) as session:
            transcripts = fetch_transcript_if_not_in_db(video.id, run_id, session)
            logger.info(f"Fetched {len(transcripts)=} for {video.id=}")

            session.add_all(transcripts)
            logger.info(f"Added all {len(transcripts)=} to the session")

            logger.info(f"Updating the run id for the video. Existing value {video.fetched_transcripts_at_run=}")
            video.fetched_transcripts_at_run = run_id
            logger.info(f"Updated {video=}")

            try:
                logger.info(f"Committing transaction for {video.id=}")
                session.commit()
                logger.info(f"Transaction committed for {video.id=}")
            except DatabaseError as e:
                logger.warning(
                    f"Could not complete the transaction for {video.id=} because {e}. "
                    "This transaction will be retried in the next run."
                )
                session.rollback()
