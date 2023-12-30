import logging
from datetime import datetime
from typing import List, TypedDict, Type

from modal import Secret
from sqlalchemy import Engine
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import Session
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.scrapper.db import YouTubeTranscriptLine, db_engine, YouTubeVideo

stub = create_stub(__name__)
logger = logging.getLogger(__name__)
engine = db_engine(echo=False)


class TranscriptUpdateStatus(TypedDict):
    video_id: str
    status: bool


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
    transcripts_from_db = session.query(YouTubeTranscriptLine) \
        .filter(YouTubeTranscriptLine.video_id == video_id) \
        .all()

    if transcripts_from_db:
        logger.warning(f"Deleting {len(transcripts_from_db)} for {video_id=} because transcripts already exists")
        session.query(YouTubeTranscriptLine) \
            .filter(YouTubeTranscriptLine.video_id == video_id) \
            .delete()

    return [
        YouTubeTranscriptLine(video_id=video_id, text=line['text'], start=line['start'], duration=line['duration'],
                              first_inserted_at_run=run_id)
        for line in fetch_transcript(video_id)
    ]


def fetch_videos_without_transcripts(e: Engine) -> List[Type[YouTubeVideo]]:
    with Session(e) as session:
        return session.query(YouTubeVideo) \
            .filter(YouTubeVideo.fetched_transcripts_at_run.is_(None)) \
            .all()


@stub.function(secret=Secret.from_name("planetscale-database"))
def process_video(video_id: str, run_id: datetime) -> TranscriptUpdateStatus:
    setup_logging()
    max_chunk_size = 100

    with Session(engine) as session:
        transcripts = fetch_transcript_if_not_in_db(video_id, run_id, session)
        logger.info(f"Fetched {len(transcripts)=} for {video_id=}")

    transcript_chunks = [transcripts[i:i + max_chunk_size] for i in range(0, len(transcripts), max_chunk_size)]
    logger.debug(f"Created {len(transcript_chunks)} chunks for {video_id=} with {max_chunk_size=}")

    for chunk_number, transcript_chunk in enumerate(transcript_chunks):
        with Session(engine) as session:
            session.add_all(transcript_chunk)
            logger.debug(f"Added all {len(transcript_chunk)=} to the session for {chunk_number=}")

            try:
                logger.debug(f"Committing transaction for {video_id=}")
                session.commit()
                logger.debug(f"Transaction committed for {video_id=}")
            except DatabaseError as e:
                logger.warning(
                    f"Could not complete the transaction for {video_id=} because {e}. "
                    "This transaction will be retried in the next run."
                )
                session.rollback()
                return {"video_id": video_id, "status": False}

    with Session(engine) as session:
        logger.info(f"Updating fetched_transcripts_at_run={run_id} for {video_id=}")
        session.query(YouTubeVideo) \
            .filter(YouTubeVideo.id == video_id) \
            .update({'fetched_transcripts_at_run': run_id}, synchronize_session=False)

    logger.info(f"Updated fetched_transcripts_at_run={run_id} for {video_id=}")
    return {"video_id": video_id, "status": True}


@stub.local_entrypoint()
def main() -> None:
    setup_logging()
    run_id = datetime.now()

    transcripts = fetch_videos_without_transcripts(db_engine())

    logger.info(f"fetched {len(transcripts)} videos which need transcripts")
    logger.info(list(process_video.map(
        map(lambda v: v.id, transcripts),
        kwargs={'run_id': run_id}
    )))
