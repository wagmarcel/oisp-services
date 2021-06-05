import os
import re
import requests

FLINK_URL = os.getenv("OISP_FLINK_REST") or f"http://flink-jobmanager-rest.{namespace}:8081"


def get_job_status(logger, job_id):
    logger.debug(f"Requestion status for {job_id} from flink job-manager")
    job_request = requests.get(
        f"{FLINK_URL}/jobs/{job_id}").json()
    logger.debug(f"Received job status: {job_request}")
    return job_request

