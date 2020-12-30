import os
import os.path
import ftplib
import time
import uuid
import json
import asyncio
from datetime import datetime

import kopf
import logging
import requests

import util

namespace = os.environ["OISP_NAMESPACE"]
FLINK_URL = os.environ["OISP_FLINK_REST"]
JOB_STATUS_UNKNOWN = "UNKNOWN"
MAX_RETRY = os.getenv("OISP_BEAMOPERATOR_RETRY") or 20

################################################################
# Operator functions
# All event function MUST be async to avoid race conditions ...
################################################################
@kopf.on.startup()
async def configure(settings: kopf.OperatorSettings, **_):
    settings.posting.level = logging.INFO

@kopf.on.create("oisp.org", "v1", "beamservices")
async def create(body, patch, spec, **kwargs):
    kopf.info(body, reason="Creating", message="Creating beamservices"+str(spec))
    reset_status(patch)
    return {"createdOn": str(datetime.now())}

@kopf.timer("oisp.org", "v1", "beamservices", interval=5)
async def monitoring(stopped, patch, logger, body, spec, status, **kwargs):
    """Monitor operator states and Flink state regularly. Set the respective triggers if problems are found."""

    # State checking
    # if not deployed and not deploying => deploying
    # if deploying => exit and wait
    # if deploying and deployed => should not happen/deploying handler died unfinished
    # same procedure with jobCreated and jobCreating
    if status.get('deployed') is None:
        kopf.info(body, reason="Status None", message="Object not created")
        raise Exception("Object not created")
    if not status.get("deployed") and not status.get("deploying"):
        kopf.info(body, reason="JarDeploying", message="Requesting to deploy jar")
        patch.status['deploying'] = True
        return {"updatedOn": str(datetime.now())}
    if status.get("deployed") and status.get("deploying"):
        kopf.info(body, reason="JarDeploying", message="Inconsistent state. Fixing")
        patch.status['deploying'] = False
        return {"updatedOn": str(datetime.now())}
    if status.get("deploying"):
        kopf.info(body, reason="Deploying", message="Waiting for deploying Task")
        return
    if not status.get("jobCreated") and not status.get("jobCreating"):
        kopf.info(body, reason="JobSubmitting", message="Requesting job submission Task")
        patch.status['jobCreating'] = True
        return {"updatedOn": str(datetime.now())}
    if status.get("jobCreating") and status.get("jobCreated"):
        kopf.info(body, reason="JobSubmitting", message="Inconsistent state. Fixing")
        patch.status['jobCreating'] = False
        return {"updatedOn": str(datetime.now())}
    if status.get("jobCreating"):
        count = None
        monitor_status = status.get("monitoring")
        if monitor_status is not None:
            count = monitor_status.get("jobCreatingCount")
            if count is not None:
                count += 1
            else:
                count = 1
        if count > MAX_RETRY * 10:
            patch.status['jobCreating'] = False
            patch.status['jobCreated'] = False
            return {'jobCreatingCount': 0}
        kopf.info(body, reason="JobSubmitting", message="Waiting for ongoing Task")
        return {'jobCreatingCount': count}

    # Actual monitoring part.
    # Try to get status of a job. React only if Job xyz not found is showing up explicit in the error message
    # Reset deployed and jobcreated state to None   
    kopf.info(body, reason="Monitoring", message="Checking Flink state.")
    try:
        job_status = requests.get(
            f"{FLINK_URL}/jobs/{status.get('jobId')}")
        status_code = job_status.status_code
        
        # 404 - job not found
        # 200 - job found, but check whether the state is FAILED (because failed jobs are not restarted automatically)
        if status_code == 404:
            kopf.info(body, reason="Job monitor", message="Job not found, triggering redeploy.")
            patch.status['deployed'] = False
            patch.status['jobCreated'] = False
            return {"updatedOn": str(datetime.now())}
        elif status_code == 200:
            pipeline_state = job_status.json().get("state")
            if pipeline_state is not None:
                patch.status['state'] = pipeline_state
            if pipeline_state == 'FAILED':
                kopf.info(body, reason="Job monitor", message="Job failed, triggering redeploy.")
                cancel_job(body, status)
                delete_jar(status)
                reset_status(patch)
                patch.status['state'] = "RESETTING BY OPERATOR"
                return {"updatedOn": str(datetime.now())}
    except requests.exceptions.RequestException as e:
            kopf.info(body, reason="monitor job", message="Exception while trying to query job state. Reason: " + str(e))
    return


@kopf.on.field("oisp.org", "v1", "beamservices", field="spec.reset")
async def reset(old, new, status, patch, body, spec, retry, **kwargs):
    """Convenience hook. Normally a reset should happen by deleting and deploying the service. However, some of the 
    services have lot of autogenerated cutom values and thus changing the reset value to arbitrary number is triggering
    a reset of the service.
    """
    if new is not None:
        kopf.info(body, reason="Reset triggered", message="Reset triggered in spec.")
        cancel_job(body, status)
        delete_jar(status)
        reset_status(patch)    
        return {"updatedOn": str(datetime.now())}

@kopf.on.field("oisp.org", "v1", "beamservices", field="status.jobCreating")
async def jobCreating(old, new, status, patch, body, spec, retry, **kwargs):
    """Submit jobs to Flink when status.jobCreating is triggered by monitor."""
    kopf.info(body, reason="debugging", message="Handler enters jobCreating with status " +\
        str(new) + ", previous status " + str(old))
    if retry > MAX_RETRY:
        kopf.info(body, reason="jobCreating", message="Handler reached maximum retries. Reset states.")
        cancel_job(body, status)
        delete_jar(status)
        reset_status(patch)
        return {"updatedOn": str(datetime.now())}
    job_id = None
    if new:
        job_id = create_job(body, spec, status.get("jarId"))
        if job_id is not None:
            patch.status['jobId'] = job_id
            patch.status['jobCreating'] = False
            patch.status['jobCreated'] = True
            return {"updatedOn": str(datetime.now())}
        raise kopf.TemporaryError("No job returned. Try later again.", delay=5)

@kopf.on.field("oisp.org", "v1", "beamservices", field="status.deploying")
async def deploying(old, new, status, patch, body, spec, retry, **kwargs):
    """Deploy files when triggered by monitor."""
    kopf.info(body, reason="debugging", message="Handler enters deploying with status " +\
        str(new) + ", previous status " + str(old))
    if retry > MAX_RETRY:
        kopf.info(body, reason="deploying", message="Handler reached maximum retries. Reset states.")
        cancel_job(body, status)
        delete_jar(status)
        reset_status(patch)
        return {"updatedOn": str(datetime.now())}
    jar_id = None
    if new:
        jar_path = status.get('jarPath')
        if jar_path is not None:
            delete_jar(status)
            jar_path = None
            patch.status['jarPath'] = None
        jar_path = fetch_jar(body, spec)
        if jar_path is not None:
            patch.status['jarPath'] = jar_path
            jar_id = deploy(body, spec, jar_path)
            if jar_id is not None:
                patch.status['deployed'] = True
                patch.status['deploying'] = False
                patch.status['jarId'] = jar_id
                return {"updatedOn": str(datetime.now())}
        raise kopf.TemporaryError("No jar_id or jar_path returned. Try later again.", delay=5)

@kopf.on.delete("oisp.org", "v1", "beamservices")
async def delete(body, status, **kwargs):
    cancel_job(body, status)
    delete_jar(status)

####################
# helper functions
####################
def cancel_job(body, status):
    job_id = status.get("jobId")
    if job_id is not None:
        try:
            resp = requests.patch(
                f"{FLINK_URL}/jobs/{job_id}", params={"mode": "cancel"})
        except requests.exceptions.RequestException as e:
            kopf.info(body, reason="cancel job", message="Exception after trying to cancel a job. Reason: " + str(e))
        if resp.status_code is not 200:
            kopf.info(body, reason="cancel job", message="Could not cancel job. Reason: " + str(resp.text))

def download_file_via_http(url):
    """Download the file and return the saved path."""
    try:
        response = requests.get(url)
        path = "/tmp/" + str(uuid.uuid4()) + ".jar"
        with open(path, "wb") as f:
            f.write(response.content)
        return path
    except requests.exceptions.RequestException as e:
        kopf.info(body, reason="download jar", message="Exception while trying to download jar. Reason: " + str(e))
    return None

def download_file_via_ftp(url, username, password):
    local_path = "/tmp/" + str(uuid.uuid4()) + ".jar"
    url_without_protocol = url[6:]
    addr = url_without_protocol.split("/")[0]
    remote_path = "/".join(url_without_protocol.split("/")[1:])
    with open(local_path, "wb") as f:
        with ftplib.FTP(addr, username, password) as ftp:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    return local_path


def fetch_jar(body, spec):
    """Fetch the file from the spec.package"""
    package = spec["package"]
    url = package["url"]
    kopf.info(body, reason="Jar download",
              message=f"Downloading from {url}")
    if url.startswith("http"):
        jarfile_path = download_file_via_http(url)
    elif url.startswith("ftp"):
        jarfile_path = download_file_via_ftp(url, package["username"], package["password"])
    else:
        kopf.warn(body, reason="BeamDeploymentFailed",
                   message="Invalid url (must start with http or ftp)")
        raise kopf.PermanentError("Jar download failed due to wrong url format.")
    return jarfile_path

def deploy(body, spec, jarfile_path):
    # TODO Create schema for spec in CRD
    try:
        response = requests.post(
            f"{FLINK_URL}/jars/upload", files={"jarfile": open(jarfile_path, "rb")})
        if response.status_code != 200:
            kopf.warn(body, reason="BeamDeploymentFailed",
                    message="Could not submit jar, server returned:" +
                    response.request.body.decode("utf-8"))
            raise kopf.TemporaryError("Jar submission failed.", delay=10)

        jar_id = response.json()["filename"].split("/")[-1]

        kopf.info(body, reason="BeamDeploymentSuccess",
                message=f"Submitted jar with id: {jar_id}")
        return jar_id
    except requests.exceptions.RequestException as e:
        kopf.info(body, reason="download jar", message="Exception while trying to download jar. Reason: " + str(e))
    return None


def build_args(args_dict, tokens):
    args_str = ""
    for key, val in args_dict.items():
        if isinstance(val, str):
            args_str += f"--{key}={val} "
            continue
        assert isinstance(val, dict), "Values should be str or dict."
        assert "format" in val, "'format' is mandatory"
        val = util.format_template(val["format"], tokens=tokens, encode=val.get("encode"))
        args_str += f"--{key}={val} "
    return args_str


def create_job(body, spec, jar_id):
    kopf.info(body, reason="createJob Task", message="starting job with jar_id " + str(jar_id))
    if check_free_slots(body) == 0:
        raise kopf.TemporaryError("No free task slots for submission. Try later again.", delay=5)
    entry_class = spec["entryClass"]
    tokens = util.get_tokens(spec.get("tokens", []))
    args = build_args(spec["args"], tokens)
    kopf.info(body, reason="Args Parsed",
              message=args)
    try:
        response = requests.post(f"{FLINK_URL}/jars/{jar_id}/run",
                                json={"entryClass": entry_class,
                                    "programArgs": args})
    except requests.exceptions.RequestException as e:
            kopf.info(body, reason="create job", message="Exception while trying to create a job. Reason: " + str(e))
    if response.status_code != 200:
        kopf.info(body, reason="BeamExecutionFailed",
                  message="Could not run job, server returned:\n" +
                  response.content.decode("utf-8"))
        return None
    job_id = response.json().get("jobid")
    kopf.info(body, reason="Job created", message=f"Job id: {job_id}")
    return job_id

def check_free_slots(body):
    try:
        response = requests.get(f"{FLINK_URL}/overview")
        if response.status_code == 200:
            free_slots = response.json().get("slots-available")
            return free_slots
    except requests.exceptions.RequestException as e:
            kopf.info(body, reason="jobmanager overview", message="Exception while trying to check cluster state. Reason: " + str(e))

def reset_status(patch):
    patch.status['jobCreating'] = False
    patch.status['jobCreated'] = False
    patch.status['deploying'] = False
    patch.status['deployed'] = False
    patch.status['jobStatus'] = None
    patch.status['jobId'] = None
    patch.status['jarId'] = None
    patch.status['jarPath'] = None

def delete_jar(status):
    jar_path = status.get("jarPath")
    if jar_path is not None:
        if os.path.isfile(jar_path):
            os.remove(jar_path)