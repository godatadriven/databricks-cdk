import os


def handler(event, context):
    lambda_method = os.environ.get("LAMBDA_METHOD", "cnf-deploy")

    if lambda_method == "cfn-deploy":
        from databricks_cdk.resources.handler import handler

        handler(event, context)
    elif lambda_method == "submit-job":
        from databricks_cdk.jobs.submit_job import handler

        handler(event, context)
    elif lambda_method == "job-status":
        from databricks_cdk.jobs.job_status import handler

        handler(event, context)
    else:
        raise RuntimeError(f"Lambda method does not exists: {lambda_method}")
