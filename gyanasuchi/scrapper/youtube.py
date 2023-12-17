import logging
from datetime import datetime
from typing import TypedDict, List, Dict, Iterator

from dotenv import load_dotenv
from modal import Stub, Image
from pytube import Playlist
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from gyanasuchi.scrapper.db import YouTubePlaylist, db_engine, Base, YouTubePlaylistVideo, insert_if_not_dupe

stub = Stub(
    name="fetch-transcripts",
    image=Image.debian_slim()
    .apt_install(
        "python3-dev",
        "default-libmysqlclient-dev",
        "build-essential",
        "pkg-config"
    )
    .poetry_install_from_file('pyproject.toml')
)
PlaylistId = str
VideoId = str


class TranscriptLine(TypedDict):
    text: str
    start: float
    duration: float


def fetch_transcript(video_id: str) -> List[TranscriptLine]:
    print(f'fetching transcripts for {video_id}')
    try:
        return YouTubeTranscriptApi.get_transcript(video_id)
    except (TranscriptsDisabled, NoTranscriptFound):
        return []


def videos_from_playlist(db_playlist: YouTubePlaylist) -> List[VideoId]:
    yt_playlist = Playlist(f'https://www.youtube.com/playlist?list={db_playlist.id}')
    print(f'Fetching all videos from playlist named {yt_playlist.title} ({db_playlist.name})')

    return [video_url.split('v=')[1] for video_url in yt_playlist.video_urls]


@stub.function()
async def process_playlists(playlists: Iterator[YouTubePlaylist]) -> Dict[PlaylistId, List[VideoId]]:
    return {
        playlist.id: videos_from_playlist(playlist)
        for playlist in playlists
    }


@stub.local_entrypoint()
def main() -> None:
    load_dotenv()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s - %(message)s', level=logging.INFO)
    run_id = datetime.now()
    engine = db_engine()
    playlists = initiate(engine)

    playlist_videos: Dict[PlaylistId, List[VideoId]] = process_playlists.remote(playlists)

    with Session(engine) as session:
        [
            insert_if_not_dupe(session, YouTubePlaylistVideo(
                playlist_id=playlist_id,
                video_id=video_id,
                first_inserted_at_run=run_id
            ))
            for playlist_id, video_ids in playlist_videos.items()
            for video_id in video_ids
        ]


def initiate(engine: Engine) -> List[YouTubePlaylist]:
    Base.metadata.create_all(bind=engine)
    playlists = [
        YouTubePlaylist(id="PLarGM64rPKBnvFhv7Zgvj2t_q399POBh7", name="DevDay_"),
        YouTubePlaylist(id="PL1T8fO7ArWleyIqOy37OVXsP4hFXymdOZ", name="LLM Bootcamp - Spring 2023")
    ]

    with Session(engine) as session:
        [insert_if_not_dupe(session, playlist) for playlist in playlists]

    return playlists
