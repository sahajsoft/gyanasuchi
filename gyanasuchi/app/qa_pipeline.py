import logging
import re
from typing import List
from typing import Type

from dotenv import load_dotenv
from langchain.chains import RetrievalQA
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.chat_models import ChatOpenAI
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores.qdrant import Qdrant
from langchain_core.documents import Document
from langchain_core.prompts import PromptTemplate
from qdrant_client import QdrantClient

from gyanasuchi.app.fetch_data_from_db import fetch_transcript_from_db
from gyanasuchi.common import data_volume_dir
from gyanasuchi.common import env
from gyanasuchi.common import setup_logging
from gyanasuchi.scrapper.db import YouTubeVideo

template = """Use the following pieces of information to answer the users question.
Include every piece of information in the answer. Do not miss anything from the given context to include in answer.
If you don't know the answer, please just say that you don't know the answer. Don't try to make up an answer.
Context:{context}
question:{question}
Only returns the helpful answer below and nothing else.
"""

load_dotenv()
setup_logging()


class QuestionAnswerPipeline:
    def __init__(
        self,
        collection_name: str,
        embeddings_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    ):
        self.qdrant_client = QdrantClient(
            url=env("QDRANT_URL"),
            api_key=env("QDRANT_API_KEY"),
        )
        self.embeddings_model = embeddings_model
        self.collection_name = collection_name
        self.logger = logging.getLogger(__name__)

    def load_embeddings(self, device: str = "cpu") -> HuggingFaceEmbeddings:
        return HuggingFaceEmbeddings(
            model_name=self.embeddings_model,
            model_kwargs={"device": device},
            cache_folder=f"{data_volume_dir}/embeddings/",
        )

    def create_qdrant_database(
        self,
        documents: List[Document],
        embeddings: HuggingFaceEmbeddings,
        recreate_collection: bool = True,
    ) -> Qdrant:
        self.logger.info("Qdrant database creation started")
        self.logger.info(f"Number of documents: {len(documents)}")
        self.logger.info(f"Embeddings model: {self.embeddings_model}")
        self.logger.info(f"Collection name: {self.collection_name}")
        try:
            # TODO: Use client.update_collection_aliases to rename and make this operation 0 downtime
            qdrant_db = Qdrant.from_documents(
                documents=documents,
                embedding=embeddings,
                collection_name=self.collection_name,
                url=env("QDRANT_URL"),
                api_key=env("QDRANT_API_KEY"),
                force_recreate=recreate_collection,
            )

            self.logger.info("Qdrant database has been created successfully!!")
            return qdrant_db
        except Exception as e:
            self.logger.error(f"Error creating Qdrant database: {e}")
            raise

    def qa_from_qdrant(self, query: str) -> str:
        self.logger.info(f"Querying Qdrant with query: {query}")
        try:
            prompt_template = PromptTemplate.from_template(template)
            vector_store = Qdrant(
                client=self.qdrant_client,
                collection_name=self.collection_name,
                embeddings=self.load_embeddings(),
            )
            qa = RetrievalQA.from_chain_type(
                llm=load_language_model(),
                chain_type="stuff",
                retriever=vector_store.as_retriever(
                    search_type="similarity",
                    search_kwargs={"k": 5},
                ),
                return_source_documents=True,
                chain_type_kwargs={"prompt": prompt_template},
            )
            response = qa({"query": query})
            result = response["result"]
            self.logger.info(f"Response from Qdrant: {response}")
            return result
        except Exception as e:
            self.logger.error(f"Error querying Qdrant database: {e}")
            raise


def load_language_model(temperature: int = 0) -> ChatOpenAI:
    return ChatOpenAI(
        openai_api_key=env("OPENAI_API_KEY"),
        model_name="gpt-3.5-turbo-16k",
        temperature=temperature,
    )


def load_document_data() -> List[Document]:
    all_video_ids = YouTubeVideo.select(YouTubeVideo.id)
    documents = []
    for video_id in all_video_ids:
        transcript_available, transcript = fetch_transcript_from_db(video_id)
        if transcript_available:
            # video_title = fetch_video_title_from_db(video_id)
            video_url = f"https://www.youtube.com/watch?v={video_id}"
            # document = Document(page_content=transcript, metadata={"source": video_url, "title": video_title,
            # "video_id": video_id})
            document = Document(
                page_content=transcript,
                metadata={"source": video_url, "video_id": str(video_id)},
            )
            documents.append(document)
    return documents


def clean_data(documents: List[Document]) -> List[Document]:
    cleaned_documents = []
    for document in documents:
        # Remove extra spaces and newlines
        document.page_content = re.sub(r"\s+", " ", document.page_content.strip())
        cleaned_documents.append(document)
    return cleaned_documents


def generate_text_chunks(
    documents: List[Document],
    chunk_size: int = 500,
    chunk_overlap: int = 10,
    splitter: Type[RecursiveCharacterTextSplitter] = RecursiveCharacterTextSplitter,
) -> List[Document]:
    text_splitter = splitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    return text_splitter.split_documents(documents)
