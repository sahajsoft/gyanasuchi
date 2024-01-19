from dataclasses import dataclass
from typing import List

from datasets import Metric
from dotenv import load_dotenv

from gyanasuchi.bot.qa_pipeline import qa_from_qdrant
from gyanasuchi.common import vector_collection_names


def test_pipeline() -> None:
    load_dotenv()

    rag_response = qa_from_qdrant(
        "What is terraform?",
        vector_collection_names["youtube"],
    )

    for test_case in checks():
        evaluator = test_case.to_evaluator()
        metric_name = evaluator.metric.name

        assert (
            evaluator(rag_response)[f"{metric_name}_score"]
            >= test_case.expected_min_threshold
        ), metric_name


def checks() -> List:
    from ragas.langchain import RagasEvaluatorChain
    from ragas.metrics import faithfulness, answer_relevancy, context_relevancy

    @dataclass
    class TestCase:
        metric: Metric
        expected_min_threshold: float

        def to_evaluator(self) -> RagasEvaluatorChain:
            return RagasEvaluatorChain(metric=self.metric)

    return [
        TestCase(metric=faithfulness, expected_min_threshold=1.0),
        TestCase(metric=answer_relevancy, expected_min_threshold=0.9),
        TestCase(metric=context_relevancy, expected_min_threshold=1.0),
    ]
