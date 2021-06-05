import kopf
import time
import os
import requests
from enum import Enum
import flink_util

namespace = os.environ["OISP_NAMESPACE"] or "oisp"
FLINK_URL = os.getenv("OISP_FLINK_REST") or f"http://flink-jobmanager-rest.{namespace}:8081"
FLINK_SQL_GATEWAY = os.getenv("OISP_FLINK_SQL_GATEWAY") or f"http://flink-sql-gateway.{namespace}:9000"
timer_interval_seconds = os.getenv("TIMER_INTERVAL") or 10
timer_backoff_seconds = os.getenv("TIMER_BACKOFF_INTERVAL") or 10
timer_backoff_temporary_failure_seconds = os.getenv("TIMER_BACKOFF_TEMPORARY_FAILURE_INTERVAL") or 30

class States(Enum):
    INITIALIZED = "INITIALIZED",
    DEPLOYING = "DEPLOYING",
    DEPLOYMENT_FAILURE = "DEPLOYMENT_FAILURE",
    RUNNING = "RUNNING",
    FAILED = "FAILED",
    CANCELED = "CANCELED",
    CANCELING = "CANCELING",
    UNKNOWN = "UNKNOWN"


JOB_ID = "job_id"
STATE = "state"

@kopf.on.create("oisp.org", "v1alpha1", "beamsqlstatementsets")
async def create(body, spec, patch, logger, **kwargs):
    name = body["metadata"].get("name")
    namespace = body["metadata"].get("namespace")
    kopf.info(body, reason="Creating", message=f"Creating beamsqlstatementsets {name} in namespace {namespace}")
    logger.info(f"Created beamsqlstatementsets {name} in namespace {namespace}")
    patch.status[STATE] = States.INITIALIZED.name
    patch.status[JOB_ID] = None
    return {"createdOn": str(time.time())}

@kopf.on.delete("oisp.org", "v1alpha1", "beamsqlstatementsets", retries=10)
async def delete(body, spec, patch, logger, **kwargs):
    """
    Deleting beamsqlstatementsets

    If state is not CANCELING and CANCELED, trigger cancelation of job and set state to CANCELING
    If Canceling, refresh state, when canceled, allow deletion,otherwise wait
    """
    name = body["metadata"].get("name")
    namespace = body["metadata"].get("namespace")
    state = body['status'].get(STATE)
    job_id = body['status'].get(JOB_ID)
    if not state == States.CANCELED.name and not state == States.CANCELING.name:
        try:
            flink_util.cancel_job(logger, job_id)
        except Exception as err:
            raise kopf.TemporaryError(f"Error trying to cancel {namespace}/{name} with message {err}. Trying again later", 10)    
        patch.status[STATE] = States.CANCELING.name
        raise kopf.TemporaryError(f"Waiting for confirmation of cancelation for {namespace}/{name}", 5)
    elif state == States.CANCELING.name:
        refresh_state(body, patch, logger)
        if not patch.status[STATE] == States.CANCELED.name:
            raise kopf.TemporaryError(f"Canceling, waiting for final confirmation of cancelation for {namespace}/{name}", 5)
    kopf.info(body, reason="deleting", message=f" {namespace}/{name} cancelled and ready for deletion")
    logger.info(f" {namespace}/{name} cancelled and ready for deletion")

@kopf.index('oisp.org', "v1alpha1", "beamsqltables")
async def beamsqltables(name: str, namespace: str, body: kopf.Body, **_):
    return {(namespace, name): body}


@kopf.timer("oisp.org", "v1alpha1", "beamsqlstatementsets", interval=timer_interval_seconds, backoff=timer_backoff_seconds)
async def updates(beamsqltables: kopf.Index, stopped, patch, logger, body, spec, status, **kwargs):
    """
    Managaging the main lifecycle of the beamsqlstatementset crd
    Current state is stored under
    status:
        state: STATE
        job_id: string
    STATE can be
        - INITIALIZED - resource is ready to deploy job. job_id: None
        - DEPLOYING - resource has been deployed, job_id: flink_id
        - RUNNING - resource is running job_id: flink_id
        - FAILED - resource is in failed state
        - DEPLOYMENT_FAILURE - something went wrong while operator tried to deploy (e.g. server returned 500)
        - CANCELED - resource has been canceled
        - CANCELING - resource is in cancelling process
        - UNKNOWN - resource cannot be monitored
    
    Transitions:
        - undefined/INITIALIZED => DEPLOYING
        - INITIALIZED => DEPLOYMENT_FAILURE
        - DEPLOYMENT_FAILURE => DEPLOYING
        - DEPLOYING/UNKNOWN => FLINK_STATES(RUNNING/FAILED/CANCELLED/CANCELLING)
        - delete resource => CANCELLING
        - delete done => CANCELLED
    Currently, cancelled state is not recovered
    """
    namespace = body['metadata'].get("namespace")
    name = body['metadata'].get("name")
    state = body['status'].get(STATE)
    logger.debug(f"Triggered updates for {namespace}/{name} with state {state}")
    
    if state == States.INITIALIZED.name or state == States.DEPLOYMENT_FAILURE.name:
        # deploying
        logger.debug(f"Deyploying {namespace}/{name}")
        
        # get first all table ddls
        # get inputTable and outputTable
        ddls = f"SET pipeline.name = '{namespace}/{name}';\n"
        try:
            table_names = spec.get("tables")
            for table_name in table_names:
                beamsqltable, *_ = beamsqltables[(namespace, table_name)]
                ddl = create_ddl_from_beamsqltables(body, beamsqltable, logger)
                ddls += ddl + "\n"
        except:
            logger.error(f"Table DDLs could not be created for {namespace}/{name}. \
                Check the table definitions and references.")
            raise kopf.TemporaryError(f"Table DDLs could not be created for {namespace}/{name}. \
                Check the table definitions and references.", timer_backoff_temporary_failure_seconds)
        ## now create statement set
        statements = spec.get("sqlstatements")
        statementset = ddls
        statementset += "BEGIN STATEMENT SET;\n"
        for statement in statements:
            statementset += statement + "\n"
            # TODO: check for "insert into" prefix and terminating ";" in sqlstatement
        statementset += "END;"
        logger.debug(f"Now deploying statementset {statementset}")
        try:
            job_id = deploy_statementset(statementset, logger)
        except Exception as err:
            # deploying failed
            # Temporary error as we do not know the reason
            patch.status[STATE] = States.DEPLOYMENT_FAILURE.name
            patch.status[JOB_ID] = None
            logger.error(f"Could not deploy statementset {err}")
            raise kopf.TemporaryError(f"Could not deploy statement: {err}", timer_backoff_temporary_failure_seconds)
        
        patch.status[STATE] = States.DEPLOYING.name
        patch.status[JOB_ID] = job_id
        return
    
    # If state is not INITIALIZED, DEPLOYMENT_FAILURE nor CANCELED, the state is monitored
    elif not state == States.CANCELED.name and not state == States.CANCELING.name:
        #state = body['status'].get(STATE)
        #job_id = body['status'].get(JOB_ID)
        #try:
        #    job_info = flink_util.get_job_status(logger, job_id)
        #except Exception as err:
        #    patch.status[STATE] = States.UNKNOWN.name
        #    raise kopf.TemporaryError(f"Could not monitor task {job_id}", timer_backoff_temporary_failure_seconds)
        refresh_state(body, patch, logger)


def refresh_state(body, patch, logger):
    state = body['status'].get(STATE)
    job_id = body['status'].get(JOB_ID)
    try:
        job_info = flink_util.get_job_status(logger, job_id)
    except Exception as err:
        patch.status[STATE] = States.UNKNOWN.name
        raise kopf.TemporaryError(f"Could not monitor task {job_id}", timer_backoff_temporary_failure_seconds)
    patch.status[STATE] = job_info.get("state")


def create_ddl_from_beamsqltables(body, beamsqltable, logger):
    """
    creates an sql ddl object out of a beamsqltables object
    Works only with kafka connector
    column names (fields) are not expected to be escaped with ``. This is inserted explicitly.
    The value of the fields are supposed to be prepared to pass SQL parsing, e.g.
    value: STRING # will be translated into `value` STRING, value is an SQL keyword (!)
    dvalue: AS CAST(`value` AS DOUBLE) # will b translated into `dvalue` AS CAST(`value` AS DOUBLE), `value` here is epected to be masqued

    Parameters
    ----------
    body: dict
        Beamsqlstatementset which is currently processed
    beamsqltable: dict
        Beamsqltable object
    logger: log object 
        Local log object provided from framework
    """
    name = beamsqltable.metadata.name
    ddl = f"CREATE TABLE `{name}` ("
    for key, value in beamsqltable.spec.get("fields").items():
        if key == "watermark":
            insert_key = key
        else:
            insert_key = f"`{key}`" 
        ddl += f"{insert_key} {value},"
    ddl = ddl[:-1] # remove last "," from ddl, enumeration is done
    ddl += ") WITH ("
    if beamsqltable.spec.get("connector") != "kafka":
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has not supported connector.")
        return None
    ddl += "'connector' = 'kafka'"
    format = beamsqltable.spec.get("format") 
    if not format:
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has no format description.")
        return None
    ddl += f",'format' = '{format}'"
    # loop through the kafka structure
    # map all key value pairs to 'key' = 'value',
    # except properties
    kafka = beamsqltable.spec.get("kafka")
    if not kafka:
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has no Kafka connector descriptor.")
        return None
    # check mandatory fields in Kafka, topic, bootstrap.server
    if not kafka.get("topic"):
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has no kafka topic.")
        return None

    if not kafka.get("properties").get("bootstrap.servers"):
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has no kafka bootstrap servers found")
        return None
    # the other fields are inserted, there is not (yet) a check for valid fields
    for kafka_key, kafka_value in kafka.items():
        # properties are iterated separately
        if kafka_key == 'properties':
            for property_key, property_value in kafka_value.items():
                ddl += f",'properties.{property_key}' = '{property_value}'"
        else:
            ddl += f", '{kafka_key}' = '{kafka_value}'"
            properties = kafka.get("properties")
    ddl += ");"
    logger.debug(f"Created table ddl for table {name}: {ddl}")
    return ddl
    

def deploy_statementset(statementset, logger):
    """
    deploy statementset to flink SQL gateway

    A statementset is deployed to the flink sql gateway. It is expected that all statements
    in a set are inserting the result into a table.
    The table schemas are defined at the beginning

    Parameter
    ---------
    statementset: string
        The fully defined statementset including table ddl
    logger: obj
        logger obj
    returns: jobid: string or exception if deployment was not successful
        id of the deployed job
        
    """
    request = f"{FLINK_SQL_GATEWAY}/v1/sessions/session/statements"
    logger.debug(f"Deployment request to SQL Gateway {request}")
    response = requests.post(request,
                             json={"statement": statementset})
    if response.status_code != 200:
        raise Exception(f"Could not deploy job to {request}, server returned: {response.status_code}")
    logger.debug(f"Response: {response.json()}")
    job_id = response.json().get("jobid")
    return job_id
