from datetime import datetime
from typing import TypedDict, List, Dict, Iterator

from dotenv import load_dotenv
from pytube import Playlist
from sqlalchemy.orm import Session
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub
from gyanasuchi.scrapper.db import YouTubePlaylist, db_engine, YouTubePlaylistVideo, insert_if_not_dupe

stub = create_stub(__name__)
PlaylistId = str
VideoId = str
PlaylistToVideos = Dict[PlaylistId, List[VideoId]]


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


def _videos_from_playlist(db_playlist: YouTubePlaylist) -> List[VideoId]:
    yt_playlist = Playlist(f'https://www.youtube.com/playlist?list={db_playlist.id}')
    print(f'Fetching all videos from playlist named {yt_playlist.title} ({db_playlist.name})')

    return [video_url.split('v=')[1] for video_url in yt_playlist.video_urls]


def _map_playlists_to_videos(playlist_videos: PlaylistToVideos, session: Session, run_id: datetime) -> None:
    [
        insert_if_not_dupe(session, YouTubePlaylistVideo(
            playlist_id=playlist_id,
            video_id=video_id,
            first_inserted_at_run=run_id
        ))
        for playlist_id, video_ids in playlist_videos.items()
        for video_id in video_ids
    ]


@stub.function()
async def fetch_videos_for_playlist(playlists: Iterator[YouTubePlaylist]) -> PlaylistToVideos:
    return {
        playlist.id: _videos_from_playlist(playlist)
        for playlist in playlists
    }


@stub.local_entrypoint()
def main() -> None:
    load_dotenv()
    setup_logging()
    engine = db_engine()
    run_id = datetime.now()

    with Session(engine) as session:
        playlists = session.query(YouTubePlaylist).all()
        playlist_videos: PlaylistToVideos = fetch_videos_for_playlist.remote(playlists)
        _map_playlists_to_videos(playlist_videos, session, run_id)
