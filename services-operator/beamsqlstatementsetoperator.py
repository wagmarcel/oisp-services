import kopf
import time
import os


namespace = os.environ["OISP_NAMESPACE"] or "oisp"
FLINK_URL = os.getenv("OISP_FLINK_REST") or f"http://flink-jobmanager-rest.{namespace}:8081"
FLINK_SQL_GATEWAY = os.getenv("OISP_SQL_GATEWAY") or f"http://flink-sql-gateway.{namespace}:9000"

@kopf.on.create("oisp.org", "v1alpha1", "beamsqlstatementsets")
def create(body, spec, patch, **kwargs):
    kopf.info(body, reason="Creating", message="Creating beamservices"+str(spec))
    return {"createdOn": str(time.time())}


@kopf.index('oisp.org', "v1alpha1", "beamsqltables")
def beamsqltables(name: str, namespace: str, body: kopf.Body, **_):
    return {(namespace, name): body}


@kopf.timer("oisp.org", "v1alpha1", "beamsqlstatementsets", interval=10, idle=5)
async def updates(beamsqltables: kopf.Index, stopped, patch, logger, body, spec, status, **kwargs):
    kopf.info(body, reason="timer", message="triggered")
    namespace = body['metadata'].get("namespace")
    update_status = status.get("updates")
    if update_status is None:
        # Initialization of status
        print("world")
        return {"deployed": False, "jobCreated": False}
    if not update_status.get("deployed"):
        # get first all table ddls
        # get inputTable and outputTable
        table_names = spec.get("tables")
        for table_name in table_names:
            beamsqltable, *_ = beamsqltables[(namespace, table_name)]
            ddl = create_ddl_from_beamsqltables(body, beamsqltable)
            print("ddl", ddl)


"""
creates an sql ddl object out of a beamsqltables object
Works only with kafka connector
column names (fields) are not expected to be escaped with ``. This is inserted explicitly.
The value of the fields are supposed to be prepared to pass SQL parsing, e.g.
value: STRING # will be translated into `value` STRING, value is an SQL keyword (!)
dvalue: AS CAST(`value` AS DOUBLE) # will b translated into `dvalue` AS CAST(`value` AS DOUBLE), `value` here is epected to be masqued
"""
def create_ddl_from_beamsqltables(body, beamsqltable):
    name = beamsqltable.metadata.name
    ddl = f"CREATE table {name} ("
    for key, value in beamsqltable.spec.get("fields").items():
        ddl += f"`{key}`: {value},"
    ddl += ") WITH ("
    if beamsqltable.spec.get("connector") != "kafka":
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has not supported connector.")
        return None
    ddl += "'connector' = 'kafka'"
    format = beamsqltable.spec.get("format") 
    if not format:
        kopf.warn(body, reason="invalid CRD", message=f"Beamsqltable {name} has no format description.")
        return None
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
    return ddl
    