import logging

from gyanasuchi.app.qa_pipeline import clean_data
from gyanasuchi.app.qa_pipeline import generate_text_chunks
from gyanasuchi.app.qa_pipeline import load_document_data
from gyanasuchi.app.qa_pipeline import QuestionAnswerPipeline
from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.modal import nfs_mapping

stub = create_stub(__name__, "qdrant-gyanasuchi")
setup_logging()
logger = logging.getLogger(__name__)


@stub.function(network_file_systems=nfs_mapping())
async def load_database(collection_name: str):
    pipeline = QuestionAnswerPipeline(collection_name=collection_name)
    text_chunks = generate_text_chunks(clean_data(load_document_data()))

    try:
        embeddings_model = pipeline.load_embeddings()
        pipeline.create_qdrant_database(text_chunks, embeddings_model)
    except Exception as error:
        logger.error("Saving to Vector DB failed", error)


@stub.local_entrypoint()
def main() -> None:
    load_database.remote("youtube_transcripts")
