from qa_pipeline import QuestionAnswerPipeline
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
import uvicorn

from gyanasuchi.common import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI()

DATA_DUMP_PATH = "gyanasuchi/artifacts/transcripts"

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


@app.get('/api/crate_db')
def load_database():
    try:
        pipeline = QuestionAnswerPipeline(collection_name="youtube_transcripts")
        documents = pipeline.load_document_data(DATA_DUMP_PATH)
        cleaned_documents = pipeline.clean_data(documents)
        text_chunks = pipeline.generate_text_chunks(cleaned_documents)
        embeddings_model = pipeline.load_embeddings()
        pipeline.create_qdrant_database(text_chunks, embeddings_model)
        return JSONResponse(
            content={'status': 'success', 'result': "The vector database is created with the given data"},
            status_code=200)
    except Exception as error:
        return JSONResponse(content={'status': 'error', 'message': str(error)}, status_code=400)


@app.post('/api/qa')
def qa_retrieve(input_query: UserQuery):
    pipeline = QuestionAnswerPipeline(collection_name="youtube_transcripts")
    try:
        received_ans = pipeline.qa_from_qdrant(input_query.query)
        return JSONResponse(content={'status': 'success', 'result': received_ans}, status_code=200)
    except Exception as error:
        return JSONResponse(content={'status': 'error', 'message': str(error)}, status_code=400)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", reload=True, port=8000)
