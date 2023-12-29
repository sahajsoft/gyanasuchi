import logging
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.scrapper.db import YouTubeTranscriptLine, db_engine, YouTubePlaylistVideo

stub = create_stub(__name__)
logger = logging.getLogger(__name__)

# TODO: add data_dump_path to .env or create a config
data_dump_path = Path('gyanasuchi/artifacts/transcripts')


def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def write_transcript_to_file(video_id, transcript, path):
    with open(os.path.join(path, f"{video_id}.txt"), "w") as file:
        file.write(transcript)


def get_transcript_from_db(video_id: str, session: Session):
    transcripts_from_db = session.query(YouTubeTranscriptLine.text).filter_by(video_id=video_id).order_by('start')
    transcript_available = True

    if transcripts_from_db.count() == 0:
        transcript_available = False
        logger.warning(f"Transcript does not exists for {video_id}")
        return transcript_available, ''

    transcript = ' '.join(row.text for row in transcripts_from_db)

    return transcript_available, transcript


def get_transcripts_of_all_videos(session: Session, path_to_dump: Path):
    # TODO: write a util func to get all video_id
    all_videos = session.query(YouTubePlaylistVideo).all()

    logger.info("Extracting transcripts for all videos")

    for row in all_videos:
        logger.info(f"Extracting transcript for {row.video_id}")
        transcript_available, transcript = get_transcript_from_db(row.video_id, session)
        if transcript_available:
            logger.info(f"Transcript for {row.video_id} extracted")
            logger.info(f"Transcript - {transcript}")
            write_transcript_to_file(row.video_id, transcript, path_to_dump)


@stub.local_entrypoint()
def main() -> None:
    # if __name__ == "__main__":
    load_dotenv()
    setup_logging()
    engine = db_engine()
    ensure_directory_exists(data_dump_path)
    with Session(engine) as session:
        get_transcripts_of_all_videos(session, data_dump_path)
        logger.info(f"Extraction completed, dumping data to {data_dump_path}")
