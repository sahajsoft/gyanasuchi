import logging

from gyanasuchi.app.qa_pipeline import clean_data
from gyanasuchi.app.qa_pipeline import create_qdrant_database
from gyanasuchi.app.qa_pipeline import generate_text_chunks
from gyanasuchi.app.qa_pipeline import load_document_data
from gyanasuchi.app.qa_pipeline import load_embeddings
from gyanasuchi.common import setup_logging
from gyanasuchi.common import vector_collection_names
from gyanasuchi.modal import create_stub
from gyanasuchi.modal import nfs_mapping

stub = create_stub(__name__, "qdrant-gyanasuchi")
logger = logging.getLogger(__name__)


@stub.function(network_file_systems=nfs_mapping())
async def create_and_push_data_into_vector_db(collection_name: str):
    text_chunks = generate_text_chunks(clean_data(load_document_data()))

    try:
        embeddings_model = load_embeddings()
        create_qdrant_database(collection_name, text_chunks, embeddings_model)
    except Exception as error:
        logger.error("Saving to Vector DB failed", error)


@stub.local_entrypoint()
def main() -> None:
    setup_logging()
    create_and_push_data_into_vector_db.remote(vector_collection_names["youtube"])
