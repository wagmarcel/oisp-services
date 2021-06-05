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