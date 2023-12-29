# Gyanasuchi

_The one who indexes knowledge.._

This is a bot that will collect information from various sources, process it and make it searchable on Sahaj's Slack.

## Setup

```commandline
poetry install
poetry run python gyanasuchi/scrapper/setup.py
```

## Fetch playlist videos
```commandline
poetry run modal run gyanasuchi/scrapper/fetch_playlist_videos.py
```

## scrape transcripts for playlist videos
```commandline
poetry run modal run gyanasuchi/scrapper/fetch_transcripts.py.py
```

## fetch transcripts for all videos from PlanetScale DB
```commandline
poetry run modal run gyanasuchi/planetscale_dataloader/fetch_data_from_db.py
```

## run a FastAPI app on local - Baseline RAG with Quadrant Cloud DB **on local**
```commandline
poetry run python gyanasuchi/app/main.py
```
### test app at FastAPI Swagger - http://0.0.0.0:8000/docs

_Following keys should be there in .env file_

* DATABASE_HOST
* DATABASE_USERNAME
* DATABASE_PASSWORD
* DATABASE_NAME
* OPENAI_API_KEY
* QDRANT_API_KEY
* QDRANT_URL
