from typing import TypedDict, List

from youtube_transcript_api import YouTubeTranscriptApi


class TranscriptLine(TypedDict):
    text: str
    start: float
    duration: float


def fetch_transcript(video_id: str) -> List[TranscriptLine]:
    return YouTubeTranscriptApi.get_transcript(video_id)

