import kopf
import time
import os
import requests
from enum import Enum


namespace = os.environ["OISP_NAMESPACE"] or "oisp"
FLINK_URL = os.getenv("OISP_FLINK_REST") or f"http://flink-jobmanager-rest.{namespace}:8081"
FLINK_SQL_GATEWAY = os.getenv("OISP_FLINK_SQL_GATEWAY") or f"http://flink-sql-gateway.{namespace}:9000"
timer_interval_seconds = os.getenv("TIMER_INTERVAL") or 10
timer_backoff_seconds = os.getenv("TIMER_BACKOFF_INTERVAL") or 10
timer_backoff_temporary_failure_seconds = os.getenv("TIMER_BACKOFF_TEMPORARY_FAILURE_INTERVAL") or 30

class States(Enum):
    INITIALIZED = "INITIALIZED",
    SUBMITTED = "SUBMITTED",
    SUBMISSION_FAILURE = "SUBMISSION_FAILURE"

#states = {
#    "INITIALIZED": "INITIALIZED",
#    "SUBMITTED": "SUBMITTED",
#    "SUBMISSION_FAILURE": "SUBMISSION_FAILURE"
#}
JOB_ID = "job_id"
STATE = "state"

@kopf.on.create("oisp.org", "v1alpha1", "beamsqlstatementsets")
def create(body, spec, patch, logger, **kwargs):
    name = body["metadata"].get("name")
    namespace = body["metadata"].get("namespace")
    kopf.info(body, reason="Creating", message=f"Creating beamsqlstatementsets {name} in namespace {namespace}")
    logger.info(f"Created beamsqlstatementsets {name} in namespace {namespace}")
    patch.status[STATE] = States.INITIALIZED.name
    patch.status[JOB_ID] = None
    return {"createdOn": str(time.time())}


@kopf.index('oisp.org', "v1alpha1", "beamsqltables")
def beamsqltables(name: str, namespace: str, body: kopf.Body, **_):
    return {(namespace, name): body}


@kopf.timer("oisp.org", "v1alpha1", "beamsqlstatementsets", interval=timer_interval_seconds, backoff=timer_backoff_seconds)
def updates(beamsqltables: kopf.Index, stopped, patch, logger, body, spec, status, **kwargs):
    """
    Managaging the main lifecycle of the beamsqlstatementset crd
    Current state is stored under
    status:
        state: STATE
        job_id: string
    STATE can be
        - INITIALIZED - job_id: None
        - SUBMITTED - job_id: flink_id
        - CREATED
        - RUNNING - job_id: flink_id
        - FAILED
        - SUBMISSION_FAILURE
        - CANCELLED
        - DELETING - job_id: None
    Transitions:
        - undefined/INITIALIZED => SUBMITTED
        - INITIALIZED => SUBMISSION_FAILURE
        - SUBMISSION_FAILURE => SUBMITTED
        - SUBMITTED => RUNNING
        - SUBMITTED => FAILED
        - SUBMITTED => CANCELLED
        - FAILED => DELETING
        - CANCELLED => DELETING
        - DELETING => INITIALIZED
    
    """

    namespace = body['metadata'].get("namespace")
    name = body['metadata'].get("name")
    state = body['status'].get(STATE)
    logger.debug(f"Triggered updates for {namespace}/{name} with state {state}")
    
    if state == States.INITIALIZED.name or state == States.SUBMISSION_FAILURE.name:
        # submitting
        logger.debug(f"Submitting {namespace}/{name}")
        
        # get first all table ddls
        # get inputTable and outputTable
        ddls = ""
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
        logger.debug(f"Now submitting statementset {statementset}")
        try:
            job_id = submit_statementset(statementset, logger)
        except Exception as err:
            # submission failed
            # Temporary error as we do not know the reason
            patch.status[STATE] = States.SUBMISSION_FAILURE.name
            patch.status[JOB_ID] = None
            logger.error(f"Could not submit statementset {err}")
            raise kopf.TemporaryError(f"Could not submit statement: {err}", timer_backoff_temporary_failure_seconds)
        
        patch.status[STATE] = States.SUBMITTED.name
        patch.status[JOB_ID] = job_id
            


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
    

def submit_statementset(statementset, logger):
    """
    submit statementset to flink SQL gateway

    A statementset is submitted to the flink sql gateway. It is expected that all statements
    in a set are inserting the result into a table.
    The table schemas are defined at the beginning

    Parameter
    ---------
    statementset: string
        The fully defined statementset including table ddl
    logger: obj
        logger obj
    returns: jobid: string or exception if submission was not successful
        id of the deployed job
        
    """

    request = f"{FLINK_SQL_GATEWAY}/v1/sessions/session/statements"
    logger.debug(f"Submitting request to SQL Gateway {request}")
    response = requests.post(request,
                             json={"statement": statementset})
    if response.status_code != 200:
        raise Exception(f"Could not submit job to {request}, server returned: {response.status_code}")
    logger.debug(f"Response: {response.json()}")
    job_id = response.json().get("jobid")
    return job_id
