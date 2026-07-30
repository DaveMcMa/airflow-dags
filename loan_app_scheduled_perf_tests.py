"""
loan_app_scheduled_perf_tests

Every 4 hours, submits a fixed performance-test prompt to a Langflow agent
via its REST API and logs the agent's summarized reply.

Requires Airflow Variables:
  - LANGFLOW_API_KEY: API key for the Langflow instance
  - LANGFLOW_FLOW_ID: the flow/agent id to run
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta

import requests
import urllib3
from airflow.sdk import Variable, dag, task

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LANGFLOW_BASE_URL = "https://langflow.hpepcai2.demo.local/api/v1/run"
PROMPT = (
    "Run four performance tests: 10 VUs for 5 seconds, 15 VUs for 5 seconds, "
    "10 VUs for 10 seconds, and 20 VUs for 5 seconds. Summarize the results"
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="loan_app_scheduled_perf_tests",
    schedule="0 */4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["langflow", "performance-testing"],
)
def loan_app_scheduled_perf_tests():
    """Every 4 hours, ask a Langflow agent to run the loan-app perf test suite and summarize the results."""

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def run_langflow_perf_test() -> None:
        api_key = Variable.get("LANGFLOW_API_KEY")
        flow_id = Variable.get("LANGFLOW_FLOW_ID")
        langflow_url = f"{LANGFLOW_BASE_URL}/{flow_id}"

        payload = {
            "output_type": "chat",
            "input_type": "chat",
            "input_value": PROMPT,
            "session_id": str(uuid.uuid4()),
        }
        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
        }

        # verify=False: self-signed cert in this environment.
        response = requests.post(
            langflow_url,
            json=payload,
            headers=headers,
            verify=False,
            timeout=600,
        )
        response.raise_for_status()

        data = response.json()
        try:
            reply = data["outputs"][0]["outputs"][0]["artifacts"]["message"]
            logger.info("Langflow agent reply: %s", reply)
        except (KeyError, IndexError, TypeError) as exc:
            # Don't fail the DAG over an unexpected (but successful) response shape.
            logger.warning(
                "Unexpected Langflow response shape (%s); raw response: %s",
                exc,
                data,
            )

    run_langflow_perf_test()


loan_app_scheduled_perf_tests()
