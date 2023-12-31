# Gyanasuchi

_The one who indexes knowledge.._

This is a bot that will collect information from various sources, process it and make it searchable on Sahaj's Slack.

## Setup

```commandline
poetry install
poetry run modal run gyanasuchi/scrapper/setup.py
```

## Run
```commandline
poetry run modal run gyanasuchi/scrapper/fetch_playlist_videos.py
poetry run modal run gyanasuchi/scrapper/fetch_transcripts.py
```
