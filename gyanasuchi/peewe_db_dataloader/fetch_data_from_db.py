import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from tqdm import tqdm

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.modal import nfs_mapping
from gyanasuchi.scrapper.db import artifacts_dir
from gyanasuchi.scrapper.db import YouTubeTranscriptLine
from gyanasuchi.scrapper.db import YouTubeVideo

stub = create_stub(__name__)
logger = logging.getLogger(__name__)


def ensure_directory_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def write_transcript_to_file(video_id, transcript, path):
    with open(os.path.join(path, f"{video_id}.txt"), "w") as file:
        file.write(transcript)


def get_transcript_from_db(video_id: str):
    transcripts_from_db = (
        YouTubeTranscriptLine.select(YouTubeTranscriptLine.text)
        .where(YouTubeTranscriptLine.video == video_id)
        .order_by("start")
    )
    transcript_available = True

    if transcripts_from_db.count() == 0:
        transcript_available = False
        logger.warning(f"Transcript does not exists for {video_id}")
        return transcript_available, ""

    transcript = " ".join(row.text for row in transcripts_from_db)

    return transcript_available, transcript


def get_transcripts_of_all_videos(path_to_dump: Path):
    # get id for all videos
    all_videos = YouTubeVideo.select(YouTubeVideo.id)

    logger.info("Extracting transcripts for all videos")

    for video_id in tqdm(all_videos):
        logger.info(f"Extracting transcript for {video_id}")
        transcript_available, transcript = get_transcript_from_db(video_id)
        if transcript_available:
            logger.info(f"Transcript for {video_id} extracted")
            write_transcript_to_file(video_id, transcript, path_to_dump)


@stub.function(network_file_systems=nfs_mapping())
def main() -> None:
    setup_logging()
    load_dotenv()
    transcripts_dir = os.path.join(artifacts_dir, "transcripts")
    ensure_directory_exists(transcripts_dir)
    get_transcripts_of_all_videos(transcripts_dir)
    logger.info(f"Extraction completed, dumping data to {transcripts_dir}")
    logger.info(
        f"List of files in transcripts directory: {os.listdir(transcripts_dir)}",
    )
    logger.info(f"Total transcripts extracted: {len(os.listdir(transcripts_dir))}")


@stub.local_entrypoint()
def run_on_cloud() -> None:
    main.remote()
