from typing import TypedDict, List

from modal import Stub, Image
from pytube import Playlist
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

stub = Stub(name="fetch-transcripts", image=Image.debian_slim().poetry_install_from_file('pyproject.toml'))


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


def videos_from_playlist(playlist_id: str) -> List[str]:
    playlist = Playlist(f'https://www.youtube.com/playlist?list={playlist_id}')
    print(f'Fetching all videos from playlist named {playlist.title}')

    return [video_url.split('v=')[1] for video_url in playlist.video_urls]


@stub.function()
def fetch_transcripts_for_playlist(playlist_id: str) -> List[List[TranscriptLine]]:
    return [fetch_transcript(video_id) for video_id in videos_from_playlist(playlist_id)]


@stub.local_entrypoint()
def main(playlist_id: str) -> None:
    transcripts = fetch_transcripts_for_playlist.remote(playlist_id)

    for t in transcripts:
        print(t)
        print()
