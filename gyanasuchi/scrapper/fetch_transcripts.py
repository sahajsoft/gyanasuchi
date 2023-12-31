import logging
from datetime import datetime
from typing import List, TypedDict

from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub, nfs_mapping
from gyanasuchi.scrapper.db import YouTubeTranscriptLine, YouTubeVideo

stub = create_stub(__name__)
logger = logging.getLogger(__name__)


class TranscriptUpdateStatus(TypedDict):
    video_id: str
    status: bool


class TranscriptLine(TypedDict):
    text: str
    start: float
    duration: float


def fetch_transcript(video_id: str) -> List[TranscriptLine]:
    logger.info(f"Fetching transcripts for {video_id}")
    try:
        lines = YouTubeTranscriptApi.get_transcript(video_id)
        logger.info(f"Fetched {len(lines)} transcript lines from YouTube")
        return lines
    except (TranscriptsDisabled, NoTranscriptFound):
        return []


def fetch_transcripts_if_not_in_db(video_id: str, run_id: datetime) -> List[YouTubeTranscriptLine]:
    transcripts_from_db = YouTubeTranscriptLine.select().where(YouTubeTranscriptLine.video == video_id)

    if transcripts_from_db:
        logger.warning(f"Deleting {len(transcripts_from_db)} for {video_id=} because transcripts already exists")
        YouTubeTranscriptLine.delete().where(YouTubeTranscriptLine.video == video_id)

    return [
        YouTubeTranscriptLine(video_id=video_id, text=line['text'], start=line['start'], duration=line['duration'],
                              first_inserted_at_run=run_id)
        for line in fetch_transcript(video_id)
    ]


@stub.function(network_file_systems=nfs_mapping())
def process_video(video_id: str, run_id: datetime) -> TranscriptUpdateStatus:
    setup_logging()
    batch_size = 100

    lines = fetch_transcripts_if_not_in_db(video_id, run_id)
    logger.info(f"Inserting {len(lines)} transcript lines for {video_id=} with {batch_size=}")
    YouTubeTranscriptLine.bulk_create(lines, batch_size=batch_size)

    logger.info(f"Updating fetched_transcripts_at_run={run_id} for {video_id=}")
    YouTubeVideo.update({'fetched_transcripts_at_run': run_id}).where(YouTubeVideo.id == video_id).execute()
    logger.info(f"Updated fetched_transcripts_at_run={run_id} for {video_id=}")

    return {"video_id": video_id, "status": True}


@stub.function(network_file_systems=nfs_mapping())
def main() -> None:
    setup_logging()
    run_id = datetime.now()

    videos_without_transcripts = YouTubeVideo.select().where(YouTubeVideo.fetched_transcripts_at_run.is_null())

    logger.info(f"fetched {len(videos_without_transcripts)} videos which need transcripts")
    # logger.info([process_video(video.id, run_id) for video in videos_without_transcripts])
    logger.info(list(process_video.map(
        map(lambda v: v.id, videos_without_transcripts),
        kwargs={'run_id': run_id}
    )))


@stub.local_entrypoint()
def run_on_cloud() -> None:
    main.remote()
