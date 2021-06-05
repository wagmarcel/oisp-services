from unittest import TestCase
import unittest
import shlex
import subprocess
import time
from bunch import Bunch
from mock import patch, MagicMock
import kopf
from kopf.testing import KopfRunner


import beamsqlstatementsetoperator as target


"""
Mock functions
"""
class Logger():
            def info(self, message):
                pass
            def debug(self, message):
                pass
            def error(self, message):
                pass
            def warn(self, message):
                pass

def kopf_info(body, reason, message):
    pass

#def test_creation():
#    with KopfRunner(['run', '-A', '--verbose', './beamsqlstatementsetoperator.py']) as runner:
#        # do something while the operator is running.
#        #print(runner)
#        try:
#            subprocess.run("kubectl delete -f ./kubernetes/examples/beamsqlstatementset.yaml", shell=True, check=True)
#        except:
#            print("beamsqlstatementset not existing")
#        time.sleep(2)
#        subprocess.run("kubectl apply -f ./kubernetes/examples/beamsqlstatementset.yaml", shell=True, check=True)
#        time.sleep(1)  # give it some time to react and to sleep and to retry
#        subprocess.run("kubectl delete -f ./kubernetes/examples/beamsqlstatementset.yaml", shell=True, check=True)
#
#    #print(runner.stdout)
#    assert runner.exit_code == 0
#    assert runner.exception is None
#    assert 'Created beamsqlstatementsets beamsqlstatementset-example in namespace default' in runner.stdout
#    assert not 'Created table ddl' in runner.stdout

class TestInit(TestCase):
    @patch('kopf.info', kopf_info)
    def test_init(self):
        body = {
            "metadata": {
                "name": "name",
                "namespace": "namespace"
            },
            "spec": {}
        }
        patch = Bunch()
        patch.status = {}
        result = target.create(body, body["spec"], patch, Logger()) 
        assert result['createdOn'] is not None
        assert patch.status['state'] == "INITIALIZED"
        assert patch.status['job_id'] == None

class TestUpdates(TestCase):
    def create_ddl_from_beamsqltables(body, beamsqltable, logger):
        return "DDL;"
    def submit_statementset_successful(statementset, logger):
        assert statementset == "DDL;\nBEGIN STATEMENT SET;\nselect;\nEND STATEMENT SET;"
        return "job_id"
    def submit_statementset_failed(statementset, logger):
        raise Exception("Mock submission failed")
    @patch('beamsqlstatementsetoperator.create_ddl_from_beamsqltables', create_ddl_from_beamsqltables)
    @patch('beamsqlstatementsetoperator.submit_statementset', submit_statementset_successful)
    def test_update_submission(self):

        body = {
                "metadata": {
                    "name": "name",
                    "namespace": "namespace"
                },
                "spec": {
                    "sqlstatements": ["select;"],
                    "tables": ["table"]
                },
                "status": {
                    "state": "INITIALIZED",
                    "job_id": None
                }
        }
        
        patch = Bunch()
        patch.status = {}
        
        beamsqltables = {("namespace", "table"): ({}, {}) }
        target.updates(beamsqltables, None, patch,  Logger(), body, body["spec"], body["status"])
        assert patch.status['state'] == "SUBMITTED"
        assert patch.status['job_id'] == "job_id"

    @patch('beamsqlstatementsetoperator.create_ddl_from_beamsqltables', create_ddl_from_beamsqltables)
    @patch('beamsqlstatementsetoperator.submit_statementset', submit_statementset_failed)
    def test_update_submission_failure(self):

        body = {
                "metadata": {
                    "name": "name",
                    "namespace": "namespace"
                },
                "spec": {
                    "sqlstatements": ["select;"],
                    "tables": ["table"]
                },
                "status": {
                    "state": "INITIALIZED",
                    "job_id": None
                }
        }
        
        patch = Bunch()
        patch.status = {}
        
        beamsqltables = {("namespace", "table"): ({}, {})}
        try:
            target.updates(beamsqltables, None, patch,  Logger(), body, body["spec"], body["status"])
        except Exception as err:
            pass
        assert patch.status['state'] == "SUBMISSION_FAILURE"
        assert patch.status['job_id'] == None

    @patch('beamsqlstatementsetoperator.create_ddl_from_beamsqltables', create_ddl_from_beamsqltables)
    @patch('beamsqlstatementsetoperator.submit_statementset', submit_statementset_failed)
    def test_update_table_failure(self):

        body = {
                "metadata": {
                    "name": "name",
                    "namespace": "namespace"
                },
                "spec": {
                    "sqlstatements": ["select;"],
                    "tables": ["table"]
                },
                "status": {
                    "state": "INITIALIZED",
                    "job_id": None
                }
        }
        
        patch = Bunch()
        patch.status = {}
        
        beamsqltables = {}
        try:
            target.updates(beamsqltables, None, patch,  Logger(), body, body["spec"], body["status"])
        except Exception as err:
            assert type(err) == kopf.TemporaryError
            assert str(err).startswith("Table DDLs could not be created for namespace/name.")
if __name__ == '__main__':
    #test_creation()
    unittest.main()