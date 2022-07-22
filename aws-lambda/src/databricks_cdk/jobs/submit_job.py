from typing import List, Optional

from pydantic import BaseModel

from databricks_cdk.utils import post_request


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


def handler(event, context):
    parsed_event = SubmitJobEvent.parse_obj(event)
    url = f"{parsed_event.workspace_url}/api/2.1/jobs/run-now"
    return post_request(url, body=parsed_event.job_args.dict())
