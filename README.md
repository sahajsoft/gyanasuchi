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

## run a FastAPI app - Baseline RAG with Quadrant Cloud DB **on remote**
```commandline
poetry run modal run gyanasuchi/app/main.py
```
### test app at FastAPI - eg. - https://ajinkyak-sahaj--gyanasuchi-app-main-fastapi-app-dev.modal.run/docs (replace with your app url)

_Following keys should be there in modal secret - https://modal.com/docs/guide/secrets#using-secrets

* OPENAI_API_KEY
* QDRANT_API_KEY
* QDRANT_URL
