import logging

import modal
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from modal import asgi_app
from pydantic import BaseModel

from .qa_pipeline import QuestionAnswerPipeline
from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.modal import nfs_mapping

stub = create_stub(__name__)
setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI()

DATA_DUMP_PATH = "/artifacts/transcripts"

origins = [
    "http://localhost:8000",
    "http://localhost:3000",
    "http://127.0.0.1:8000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class UserQuery(BaseModel):
    query: str


@app.get("/")
async def root():
    logger.info("Endpoint accessed: /")
    return {"message": "Server is up and running!"}


@app.get("/api/crate_db")
async def load_database():
    try:
        pipeline = QuestionAnswerPipeline(collection_name="youtube_transcripts")
        documents = pipeline.load_document_data(DATA_DUMP_PATH)
        cleaned_documents = pipeline.clean_data(documents)
        text_chunks = pipeline.generate_text_chunks(cleaned_documents)
        embeddings_model = pipeline.load_embeddings()
        pipeline.create_qdrant_database(text_chunks, embeddings_model)
        return JSONResponse(
            content={
                "status": "success",
                "result": "The vector database is created with the given data",
            },
            status_code=200,
        )
    except Exception as error:
        return JSONResponse(
            content={"status": "error", "message": str(error)},
            status_code=400,
        )


@app.post("/api/qa")
async def qa_retrieve(input_query: UserQuery):
    pipeline = QuestionAnswerPipeline(collection_name="youtube_transcripts")
    try:
        received_ans = pipeline.qa_from_qdrant(input_query.query)
        return JSONResponse(
            content={"status": "success", "result": received_ans},
            status_code=200,
        )
    except Exception as error:
        return JSONResponse(
            content={"status": "error", "message": str(error)},
            status_code=400,
        )


@stub.function(
    network_file_systems=nfs_mapping(),
    secret=modal.Secret.from_name("gyanasuchi-secret"),
)
@asgi_app()
def fastapi_app():
    return app
