import logging

from langchain.schema.document import Document
from peewee import DoesNotExist

from gyanasuchi.scrapper.db import YouTubeTranscriptLine
from gyanasuchi.scrapper.db import YouTubeVideo

logger = logging.getLogger(__name__)


def fetch_transcript_from_db(video_id: str) -> tuple[bool, str]:
    try:
        transcripts_from_db = (
            YouTubeTranscriptLine.select(YouTubeTranscriptLine.text)
            .where(YouTubeTranscriptLine.video == video_id)
            .order_by(YouTubeTranscriptLine.start)
        )

        transcript = " ".join([line.text for line in transcripts_from_db])
        return True, transcript
    except DoesNotExist:
        logger.warning(f"Transcript does not exist for {video_id}")
        return False, ""


def fetch_video_title_from_db(video_id: str) -> str:
    try:
        video_title = YouTubeVideo.get(YouTubeVideo.id == video_id).title
        return video_title
    except DoesNotExist:
        logger.warning(f"Video title does not exist for {video_id}")
        return ""


def get_transcipts_data() -> list[Document]:
    all_video_ids = YouTubeVideo.select(YouTubeVideo.id)
    documents = []

    for video_id in all_video_ids:
        transcript_available, transcript = fetch_transcript_from_db(video_id)
        if transcript_available:
            # video_title = fetch_video_title_from_db(video_id)
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            # document = Document(page_content=transcript, metadata={"source": video_url, "title": video_title, "video_id": video_id})
            document = Document(
                page_content=transcript,
                metadata={"source": video_url, "video_id": str(video_id)},
            )
            documents.append(document)

    return documents
