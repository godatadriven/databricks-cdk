from typing import List, Optional

import requests
from pydantic import BaseModel


class JobArgs(BaseModel):
    job_id: int
    idempotency_token: Optional[str]
    jar_params: Optional[List[str]]
    notebook_params: Optional[dict]
    python_params: Optional[List[str]]
    spark_submit_params: Optional[List[str]]
    python_named_params: Optional[dict]


class SubmitJobEvent(BaseModel):
    workspace_url: str
    job_args: JobArgs


class ResponseRunNow(BaseModel):
    run_id: int
    number_in_job: Optional[int]


def handler(event, context):
    parsed_event = SubmitJobEvent.parse_obj(event)
    url = f"{parsed_event.workspace_url}/api/2.1/jobs/run-now"
    response = requests.post(url, json=parsed_event.job_args.json())
    response.raise_for_status()
    result = ResponseRunNow.parse_raw(response.text)
    return result.dict()
