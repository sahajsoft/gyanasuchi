import logging
from datetime import datetime
from typing import List, TypedDict

from dotenv import load_dotenv
from sqlalchemy.orm import Session
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.scrapper.db import YouTubeTranscriptLine, db_engine

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


@stub.local_entrypoint()
def main() -> None:
    load_dotenv()
    setup_logging()
    engine = db_engine()
    run_id = datetime.now()

    video_id = "kqBB-Z-yrcs"
    with Session(engine) as session:
        session.add_all(fetch_transcript_if_not_in_db(video_id, run_id, session))
        session.commit()
