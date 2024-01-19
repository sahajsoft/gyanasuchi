import logging
import os

vector_collection_names = {"youtube": "youtube_transcripts"}


def setup_logging() -> None:
    return logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        level=logging.INFO,
    )


def env(key: str, default: str = None) -> str:
    return os.environ.get(key, default)


def data_volume_dir() -> str:
    return env("DATA_VOLUME_DIR")
