import logging
from typing import List

from langchain_core.documents import Document
from peewee import DoesNotExist

from gyanasuchi.bot.qa_pipeline import clean_data
from gyanasuchi.bot.qa_pipeline import fill_qdrant_collection_with_data
from gyanasuchi.bot.qa_pipeline import generate_text_chunks
from gyanasuchi.bot.qa_pipeline import load_embeddings
from gyanasuchi.common import setup_logging
from gyanasuchi.common import vector_collection_names
from gyanasuchi.modal import create_stub
from gyanasuchi.modal import nfs_mapping
from gyanasuchi.scrapper.db import YouTubeTranscriptLine
from gyanasuchi.scrapper.db import YouTubeVideo

stub = create_stub(__name__, "qdrant-gyanasuchi")
logger = logging.getLogger(__name__)


def _fetch_transcript_from_db(video_id: str) -> str | None:
    try:
        transcripts_from_db = (
            YouTubeTranscriptLine.select(YouTubeTranscriptLine.text)
            .where(YouTubeTranscriptLine.video == video_id)
            .order_by(YouTubeTranscriptLine.start)
        )

        return " ".join([line.text for line in transcripts_from_db])
    except DoesNotExist:
        logger.warning(f"Transcript does not exist for {video_id}")
        return None


def _load_document_data() -> List[Document]:
    all_video_ids = YouTubeVideo.select(YouTubeVideo.id)
    documents = []
    for video_id in all_video_ids:
        transcript = _fetch_transcript_from_db(video_id)
        if transcript is not None:
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            document = Document(
                page_content=transcript,
                metadata={"source": video_url, "video_id": str(video_id)},
            )
            documents.append(document)
    return documents


@stub.function(network_file_systems=nfs_mapping())
async def create_and_push_data_into_vector_db(collection_name: str):
    text_chunks = generate_text_chunks(clean_data(_load_document_data()))

    try:
        embeddings_model = load_embeddings()
        fill_qdrant_collection_with_data(collection_name, text_chunks, embeddings_model)
    except Exception as error:
        logger.error("Saving to Vector DB failed", error)


@stub.local_entrypoint()
def main() -> None:
    setup_logging()
    create_and_push_data_into_vector_db.remote(vector_collection_names["youtube"])
