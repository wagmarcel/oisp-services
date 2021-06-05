# Beam Services Operators

The Beam Services Operators are responsible to submit and monitor lifecycle of Apache Beam jobs. Currently, there are two operators:

1. Beamservicesoperator is responsible for submitting jars to a Flink controller.

2. Beamsqlstatementsetoperator is responsible for submitting streaming sql statements to a Flink SQL Gateway


# Testing

## Unittests

    PYTHONPATH=$PWD python3 test/test_beamsqlstatementsetoperator.py

## Local Operator Test

The operator can be tested outside of the target Kubernetes cluster. Two preconditions have to be met:

1. There must be a valid KUBECONFIG

2. The Flink controller and Flink sql gateways need to be exposed with `kubefwd` and the service URLs have to be configured with environment variables `OISP_FLINK_REST` and `OISP_FLINK_SQL_GATEWAY`

# Run operators

## Precondition

TODO: pip install etc.

## Kopf run & debug

The operators have to be run with Kopf framework:

    kopf run -A beamsqlstatementsetoperator.py  beamservicesoperator.py

To enable debug output, use the "-v" switch:

    kopf run -A -v beamsqlstatementsetoperator.py  beamservicesoperator.py


# CRDs

The operators are configure with CRDs.

## beamservice

    apiVersion: oisp.org/v1
    kind: BeamService
    metadata:
    name: beam-service
    spec:
    entryClass: "org.oisp.services.ComponentSplitter"
    args:
        runner: FlinkRunner
        streaming: "true"
    package:
        url: "https://arkocal.rocks/csb.jar"

## beamsqlstatementset

    apiVersion: oisp.org/v1alpha1
    kind: BeamSqlStatementSet
    metadata:
    name: beamsqlstatementset-example
    spec:
    sqlstatements: 
        - insert into `metrics-copy` select * from `metrics`;
    tables:
        - metrics
        - metrics-copy
---
**NOTE**

Always put backticks around table names in `sqlstatments`. Otherwise there is risk of parsing problems at target SQL system (e.g. the "-" in metrics-copy is misinterpreted).

---

## beamsqltable

    apiVersion: oisp.org/v1alpha1
    kind: BeamSqlTable
    metadata:
    name: metrics
    spec:
    connector: kafka
    fields:
        "aid": STRING
        "value": STRING
        "dvalue": AS CAST(`value` AS DOUBLE)
        "on": BIGINT
        "ts": AS epoch2SQL(`on`, 'Europe/Berlin')
        "cid": STRING
        "systemon": BIGINT
        "dataType": String
        "watermark": FOR `ts` AS `ts` - INTERVAL '5' SECOND
    kafka:
        topic: metrics
        properties:
        bootstrap.servers: localhost:9094
        scan.startup.mode: latest-offset
    format: json

---
**NOTE**

Always quote field keys with "". To protect from YAML parsing (e.g. try to parse "on" without quotes). All fields, tables etc. in value of the fields should be protected by backticks.

---