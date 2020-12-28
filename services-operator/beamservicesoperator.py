import os
import ftplib
import time
import uuid
import json
import asyncio

import kopf
import requests

import util

namespace = os.environ["OISP_NAMESPACE"]
FLINK_URL = f"http://flink-jobmanager-rest.{namespace}:8081"
JOB_STATUS_UNKNOWN = "UNKNOWN"

@kopf.on.create("oisp.org", "v1", "beamservices")
def create(body, patch, spec, **kwargs):
    kopf.info(body, reason="Creating", message="Creating beamservices"+str(spec))
    patch.status['jobCreating'] = False
    patch.status['jobCreated'] = False
    patch.status['deploying'] = False
    patch.status['deployed'] = False
    patch.status['jobStatus'] = None
    patch.status['jobId'] = None
    patch.status['jarId'] = None
    return {"createdOn": str(time.time())}


# TODO make this async
@kopf.timer("oisp.org", "v1", "beamservices", interval=5)
def monitoring(stopped, patch, logger, body, spec, status, **kwargs):
    if status.get('deployed') is None:
        kopf.info(body, reason="Status None", message="Object not created")
        raise Exception("Object no created")
    if not status.get("deployed"):
        jar_id = deploy(body, spec)
        patch.status['deployed'] = True
        patch.status['jarId'] = jar_id
        return {"updatedOn": str(time.time())}
    elif not status.get("jobCreated") and not status.get("jobCreating"):
        kopf.info(body, reason="JobSubmitting", message="Requesting job submission Task")
        patch.status['jobCreating'] = True
        return {"updatedOn": str(time.time())}
    elif status.get("jobCreating"):
        kopf.info(body, reason="JobSubmitting", message="Waiting job submission Task")
        return
    
    kopf.info(body, reason="Monitoring", message="Checking Flink state.")
    job_status = requests.get(
        f"{FLINK_URL}/jobs/{status.get('jobId')}").json()
    errors = job_status.get("errors", [])
    print("hello" + str(status.get('jobId')))
    if not isinstance(errors, list):
        matching = f"Job {status.get('jobId')} not found" in errors
    else:
        matching = [element for element in errors if (f"Job {status.get('jobId')} not found" in element)]
    print("element " + str(bool(matching)))
    if bool(matching):
        kopf.info(body, reason="Job not found", message="Job not found, triggering redeploy.")
        patch.status['deployed'] = False
        patch.status['jobCreated'] = False
        return {"updatedOn": str(time.time())}
    #recreate = f"FAILED" in job_status.get("state")
    #print("Marcel532 " + str(recreate) + job_status.get("state"))
    #if recreate:
    #    kopf.info(body, reason="Job failed", message="Job not found, triggering recreation.")
    #    return {"submitting": True}
    #except (ConnectionRefusedError, requests.ConnectionError):
    #    return {"jobStatus": JOB_STATUS_UNKNOWN}
    return

@kopf.on.field("oisp.org", "v1", "beamservices", field="status.jobCreating")
def jobCreating(old, new, status, patch, body, spec, **kwargs):
    kopf.info(body, reason="JobCreating", message="JobCreating triggered with value " + str(new))
    job_id = None
    if new:
        try:
            job_id = create_job(body, spec, status.get("jarId"))
        except:
            raise kopf.TemporaryError("Exception while creating a job. Try later again.", delay=5)
        if job_id is not None:
            patch.status['jobId'] = job_id
            patch.status['jobCreating'] = False
            patch.status['jobCreated'] = True
            return {"updatedOn": str(time.time())}
        raise kopf.TemporaryError("No job returned. Try later again.", delay=5)

@kopf.on.delete("oisp.org", "v1", "beamservices")
def delete(body, **kwargs):
    try:
        update_status = body["status"].get("updates")
    except KeyError:
        return
    if not update_status:
        return
    if update_status.get("jobId"):
        resp = requests.patch(
            f"{FLINK_URL}/jobs/{update_status['jobId']}", params={"mode": "cancel"})


def download_file_via_http(url):
    """Download the file and return the saved path."""
    response = requests.get(url)
    path = "/tmp/" + str(uuid.uuid4()) + ".jar"
    with open(path, "wb") as f:
        f.write(response.content)
    return path


def download_file_via_ftp(url, username, password):
    local_path = "/tmp/" + str(uuid.uuid4()) + ".jar"
    url_without_protocol = url[6:]
    addr = url_without_protocol.split("/")[0]
    remote_path = "/".join(url_without_protocol.split("/")[1:])
    with open(local_path, "wb") as f:
        with ftplib.FTP(addr, username, password) as ftp:
            ftp.retrbinary(f"RETR {remote_path}", f.write)
    return local_path

def deploy(body, spec):
    # TODO Create schema for spec in CRD
#    url = spec["url"]
    package = spec["package"]
    url = package["url"]
    kopf.info(body, reason="Jar download",
              message=f"Downloading from {url}")
    if url.startswith("http"):
        jarfile_path = download_file_via_http(url)
    elif url.startswith("ftp"):
        jarfile_path = download_file_via_ftp(url, package["username"], package["password"])
    else:
        kopf.error(body, reason="BeamDeploymentFailed",
                   message="Invalid url (must start with http or ftp)")
        raise kopf.PermanentError("Jar download failed")
    response = requests.post(
        f"{FLINK_URL}/jars/upload", files={"jarfile": open(jarfile_path, "rb")})
    if response.status_code != 200:
        kopf.error(body, reason="BeamDeploymentFailed",
                   message="Could not submit jar, server returned:" +
                   response.request.body.decode("utf-8"))
        raise kopf.TemporaryError("Jar submission failed.", delay=10)

    jar_id = response.json()["filename"].split("/")[-1]

    kopf.info(body, reason="BeamDeploymentSuccess",
              message=f"Submitted jar with id: {jar_id}")
    return jar_id


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
    entry_class = spec["entryClass"]
    tokens = util.get_tokens(spec.get("tokens", []))
    kopf.info(body, reason="Got tokens", message=str(tokens))
    args = build_args(spec["args"], tokens)
    kopf.info(body, reason="Args Parsed",
              message=args)
    response = requests.post(f"{FLINK_URL}/jars/{jar_id}/run",
                             json={"entryClass": entry_class,
                                   "programArgs": args})
    if response.status_code != 200:
        kopf.info(body, reason="BeamExecutionFailed",
                  message="Could not run job, server returned:\n" +
                  response.content.decode("utf-8"))
        return None
    job_id = response.json().get("jobid")
    kopf.info(body, reason="Job created", message=f"Job id: {job_id}")
    return job_id
