import os
import os.path
import ftplib
import time
import uuid

import kopf
import requests

import util

namespace = os.environ["OISP_NAMESPACE"]
FLINK_URL = os.getenv("OISP_FLINK_REST") or f"http://flink-jobmanager-rest.{namespace}:8081"
JOB_STATUS_UNKNOWN = "UNKNOWN"
JOB_STATUS_FAILED = "FAILED"
JOB_STATUS_FIXING = "OPERATOR TRIES TO FIX"

@kopf.on.create("oisp.org", "v1", "beamservices")
def create(body, spec, patch, **kwargs):
    kopf.info(body, reason="Creating", message="Creating beamservices"+str(spec))
    return {"createdOn": str(time.time())}


# TODO make this async
@kopf.timer("oisp.org", "v1", "beamservices", interval=5)
async def updates(stopped, patch, logger, body, spec, status, **kwargs):
    update_status = status.get("updates")
    if update_status is None:
        kopf.info(body, reason="Status None", message="Status is none")
        return {"deployed": False, "jobCreated": False, "jobStatus": {}}
    if not update_status.get("deployed"):
        jar_id = deploy(body, spec, patch)
        return {"deployed": True, "jarId": jar_id}
    elif not update_status.get("jobCreated"):
        job_id = create_job(body, spec, update_status["jarId"])
        if job_id is not None:
            return {"jobCreated": True, "jobId": job_id}
        else:
            return
    try:
        job_status = requests.get(
            f"{FLINK_URL}/jobs/{update_status['jobId']}")
        status_code = job_status.status_code
        jar_file = status.get("jarfile")
        if status_code == 404:
            kopf.info(body, reason="Job not found", message="Job not found, triggering redeploy.")
            delete_jar(body, jar_file)
            patch.status["jarfile"] = None
            return {"deployed": False, "jobCreated": False, "jobStatus": {}, "redeployed": True}
        if status_code == 200 and job_status.json().get("state") == JOB_STATUS_FAILED:
            patch.status['jobState'] = JOB_STATUS_FIXING

            delete_jar(body, jar_file)
            patch.status["jarfile"] = None
            return {"deployed": False, "jobCreated": False, "jobStatus": {}, "redeployed": True}
    except (ConnectionRefusedError, requests.ConnectionError):
        patch.status['jobState'] = JOB_STATUS_UNKNOWN
        return
    patch.status['jobState'] = job_status.json().get("state")
    return #{"jobStatus": job_status.json().get("state")}


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

def deploy(body, spec, patch):
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
        raise kopf.PermanentError("Jar download failed. Invalid url (must start with http or ftp)")
    patch.status["jarfile"] = jarfile_path
    try:
        response = requests.post(
            f"{FLINK_URL}/jars/upload", files={"jarfile": open(jarfile_path, "rb")})
        if response.status_code != 200:
            delete_jar(body, jarfile_path)
            raise kopf.TemporaryError(f"Jar submission failed. Status code: {response.status_code}", delay=10) 
    except requests.exceptions.RequestException as e:
        delete_jar(body, jarfile_path)
        raise kopf.TemporaryError(f"Jar submission failed. Error: {e}", delay=10)
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

def delete_jar(body, jar_path):
    print(f"Marcel352 {jar_path}", flush=True)
    if jar_path is not None:
        kopf.info(body, reason="Jar delete", message=f"Trying to delete file: {jar_path}")
        if os.path.isfile(jar_path):
            print(f"Marcel353 {jar_path}", flush=True)
            try:
                os.remove(jar_path)
            except OSError as e:
                print(f"Marcel354 {jar_path}, {e}", flush=True)
            kopf.info(body, reason="Jar deleted", message=f"Jar file: {jar_path}")


#def cancel_job(body, status):
#    update_status = status.get("updates")
#    if not update_status:
#        return
#    if update_status.get("jobId"):
#        kopf.info(body, reason="cancel job", message="Trying to cancel job " + update_status.get("jobId"))
#        try:
#            resp = requests.patch(
#                f"{FLINK_URL}/jobs/{update_status['jobId']}")
#        except requests.exceptions.RequestException as e:
#            kopf.info(body, reason="cancel job", message="Exception while trying to cancel a job. Reason: " + str(e))
#            return
#        if resp.status_code != 200:
#            kopf.info(body, reason="cancel job", message="Could not cancel job. Reason: " + str(resp.text))