import logging
from datetime import datetime
from typing import List, Dict, Iterator

from peewee import IntegrityError
from pytube import Playlist

from gyanasuchi.common import setup_logging
from gyanasuchi.modal import create_stub, nfs_mapping
from gyanasuchi.scrapper.db import YouTubePlaylist, YouTubeVideo

stub = create_stub(__name__)
logger = logging.getLogger(__name__)
PlaylistId = str
VideoId = str
PlaylistToVideos = Dict[PlaylistId, List[VideoId]]


def _videos_from_playlist(db_playlist: YouTubePlaylist) -> List[VideoId]:
    yt_playlist = Playlist(f'https://www.youtube.com/playlist?list={db_playlist.id}')
    logger.info(f'Fetching all videos from playlist named {yt_playlist.title} ({db_playlist.name})')

    return [video_url.split('v=')[1] for video_url in yt_playlist.video_urls]


def _add_youtube_videos_from_playlist(playlist_videos: PlaylistToVideos, run_id: datetime):
    [
        create_or_get_video(video_id, playlist_id, run_id)
        for playlist_id, video_ids in playlist_videos.items()
        for video_id in video_ids
    ]


def create_or_get_video(video_id: str, playlist_id: str, run_id: datetime) -> YouTubeVideo:
    existing_video: YouTubeVideo = YouTubeVideo.get_or_none(id=video_id)

    if existing_video is not None:
        logger.debug(f"Adding playlist ID {playlist_id} to {video_id=}")
        try:
            existing_video.playlists.add(playlist_id)
            logger.info(f"Added playlist ID {playlist_id} to {video_id=}")
        except IntegrityError:
            logger.debug(f"{playlist_id=} already mapped to {video_id=}. Ignoring the mapping operation")

        return existing_video

    return created_video(video_id, playlist_id, run_id)


def created_video(video_id: str, playlist_id: str, run_id: datetime) -> YouTubeVideo:
    logger.info(f"Creating {video_id=} at {run_id=} for {playlist_id=}")
    video: YouTubeVideo = YouTubeVideo.create(id=video_id, first_inserted_at_run=run_id)
    video.playlists.add(playlist_id)
    video.save()
    return video


def fetch_videos_for_playlist(playlists: Iterator[YouTubePlaylist]) -> PlaylistToVideos:
    return {
        playlist.id: _videos_from_playlist(playlist)
        for playlist in playlists
    }


@stub.function(network_file_systems=nfs_mapping())
def main() -> None:
    setup_logging()
    run_id = datetime.now()

    playlists = YouTubePlaylist.select()
    # TODO: Parallel process this to improve performance
    playlist_videos = fetch_videos_for_playlist(playlists)
    _add_youtube_videos_from_playlist(playlist_videos, run_id)


@stub.local_entrypoint()
def run_on_cloud() -> None:
    main.remote()
