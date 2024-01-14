import logging
import re
import warnings

from dotenv import load_dotenv
from langchain.chains import RetrievalQA
from langchain.chat_models import ChatOpenAI
from langchain.document_loaders import DirectoryLoader
from langchain.document_loaders import TextLoader
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.prompts import PromptTemplate
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import Qdrant
from qdrant_client import QdrantClient

from gyanasuchi.common import env
from gyanasuchi.common import setup_logging

warnings.filterwarnings("ignore")

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
        collection_name,
        embeddings_model="sentence-transformers/all-MiniLM-L6-v2",
    ):
        self.qdrant_client = QdrantClient(
            url=env("QDRANT_URL"),
            api_key=env("QDRANT_API_KEY"),
        )
        self.embeddings_model = embeddings_model
        self.collection_name = collection_name
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def load_language_model(temperature=0) -> ChatOpenAI:
        return ChatOpenAI(
            openai_api_key=env("OPENAI_API_KEY"),
            model_name="gpt-3.5-turbo-16k",
            temperature=temperature,
        )

    @staticmethod
    def load_document_data(path: str) -> list:
        return DirectoryLoader(path, glob="**/*.txt", loader_cls=TextLoader).load()

    @staticmethod
    def clean_data(documents: list) -> list:
        cleaned_documents = []
        for document in documents:
            # Remove extra spaces and newlines
            document.page_content = re.sub(r"\s+", " ", document.page_content.strip())
            cleaned_documents.append(document)
        return cleaned_documents

    @staticmethod
    def generate_text_chunks(
        documents: list,
        chunk_size: int = 500,
        chunk_overlap: int = 10,
        splitter=RecursiveCharacterTextSplitter,
    ) -> list:
        text_splitter = splitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
        return text_splitter.split_documents(documents)

    def load_embeddings(self, device: str = "cpu") -> HuggingFaceEmbeddings:
        return HuggingFaceEmbeddings(
            model_name=self.embeddings_model,
            model_kwargs={"device": device},
        )

    def create_qdrant_database(
        self,
        documents: list,
        embeddings: HuggingFaceEmbeddings,
        recreate_collection: bool = True,
    ) -> Qdrant:
        self.logger.info("Qdrant database creation started")
        self.logger.info(f"Number of documents: {len(documents)}")
        self.logger.info(f"Embeddings model: {self.embeddings_model}")
        self.logger.info(f"Collection name: {self.collection_name}")
        try:
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

    def qa_from_qdrant(self, query: str) -> list:
        self.logger.info(f"Querying Qdrant with query: {query}")
        try:
            prompt_template = PromptTemplate.from_template(template)
            vector_store = Qdrant(
                client=self.qdrant_client,
                collection_name=self.collection_name,
                embeddings=self.load_embeddings(),
            )
            qa = RetrievalQA.from_chain_type(
                llm=self.load_language_model(),
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
