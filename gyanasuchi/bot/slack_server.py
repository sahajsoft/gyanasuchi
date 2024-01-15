from builtins import Exception

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi import Request
from modal import asgi_app
from slack_bolt import App
from slack_bolt import Say
from slack_bolt.adapter.fastapi import SlackRequestHandler

from gyanasuchi.app.qa_pipeline import QuestionAnswerPipeline
from gyanasuchi.modal import create_stub

load_dotenv()
stub = create_stub(
    __name__,
    "slack-gyanasuchi",
    "open-ai-gyanasuchi",
    "qdrant-gyanasuchi",
)
web_app = FastAPI()
slack_app = App()
app_handler = SlackRequestHandler(slack_app)


@web_app.get("/")
async def simple_home_responder():
    return "Hello! You shouldn't be here :)"


@web_app.post("/slack/events")
async def events_handler(request: Request):
    return await app_handler.handle(request)


@slack_app.event("app_mention")
def handle_app_mentions(body, say: Say):
    message = body["event"]["text"]
    print(f"Requested question through a mention with {message=}")
    pipeline = QuestionAnswerPipeline(collection_name="youtube_transcripts")

    try:
        say(pipeline.qa_from_qdrant(message))
    except Exception:
        say(
            "Not ready to provide _gyana_ yet but I will be soon! Hold your :horse: :horse:",
        )


@slack_app.event("message")
def handle_message(body, say) -> None:
    print(f"message {body=}")
    say(f"got a message {body}")


@stub.function()
@asgi_app()
def slack_responder_app() -> FastAPI:
    return web_app
