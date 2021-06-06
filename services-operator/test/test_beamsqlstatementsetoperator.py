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
        assert statementset == "SET pipeline.name = 'namespace/name';\nDDL;\nBEGIN STATEMENT SET;\nselect;\nEND;"
        return "job_id"
    def submit_statementset_failed(statementset, logger):
        raise Exception("Mock submission failed")
    @patch('beamsqlstatementsetoperator.create_ddl_from_beamsqltables', create_ddl_from_beamsqltables)
    @patch('beamsqlstatementsetoperator.deploy_statementset', submit_statementset_successful)
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
        assert patch.status['state'] == "DEPLOYING"
        assert patch.status['job_id'] == "job_id"

    @patch('beamsqlstatementsetoperator.create_ddl_from_beamsqltables', create_ddl_from_beamsqltables)
    @patch('beamsqlstatementsetoperator.deploy_statementset', submit_statementset_failed)
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
        assert patch.status['state'] == "DEPLOYMENT_FAILURE"
        assert patch.status['job_id'] == None

    @patch('beamsqlstatementsetoperator.create_ddl_from_beamsqltables', create_ddl_from_beamsqltables)
    @patch('beamsqlstatementsetoperator.deploy_statementset', submit_statementset_failed)
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
            target.updates(beamsqltables, None, patch, Logger(), body, body["spec"], body["status"])
        except Exception as err:
            assert type(err) == kopf.TemporaryError
            assert str(err).startswith("Table DDLs could not be created for namespace/name.")
class TestDeletion(TestCase):
    def update_status_nochange(body, patch, logger):
        pass
    def update_status_change(body, patch, logger):
        patch.status["state"] = "CANCELED"
    def cancel_job(logger, job_id):
        pass
    def cancel_job_error(logger, job_id):
        raise Exception("Could not cancel job")
    
    @patch('kopf.info', kopf_info)
    def test_canceled_delete(self):
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
                    "state": "CANCELED",
                    "job_id": "job_id"
                }
        }
        patch = Bunch()
        patch.status = {}
        
        beamsqltables = {}
        target.delete(body, body["spec"], patch, Logger())
        assert patch.status == {}

    @patch('kopf.info', kopf_info)
    @patch('beamsqlstatementsetoperator.refresh_state', update_status_nochange)
    def test_canceling_delete(self):
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
                    "state": "CANCELING",
                    "job_id": "job_id"
                }
        }
        patch = Bunch()
        patch.status = {"state": ""}
        
        beamsqltables = {}
        try:
            target.delete(body, body["spec"], patch, Logger())
        except Exception as err:
            assert type(err) == kopf.TemporaryError
            assert str(err).startswith("Canceling,")

    @patch('kopf.info', kopf_info)
    @patch('beamsqlstatementsetoperator.refresh_state', update_status_change)
    def test_canceling_delete(self):
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
                    "state": "CANCELING",
                    "job_id": "job_id"
                }
        }
        patch = Bunch()
        patch.status = {"state": ""}
        
        beamsqltables = {}
        
        target.delete(body, body["spec"], patch, Logger())
    @patch('flink_util.cancel_job', cancel_job)
    @patch('beamsqlstatementsetoperator.refresh_state', update_status_nochange)
    @patch('kopf.info', kopf_info)
    def test_canceling_delete_flink_ok(self):
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
                    "state": "RUNNING",
                    "job_id": "job_id"
                }
        }
        patch = Bunch()
        patch.status = {"state": ""}
        
        beamsqltables = {}
        try:
            target.delete(body, body["spec"], patch, Logger())
        except Exception as err:
            assert type(err) == kopf.TemporaryError
            assert str(err).startswith("Waiting for")
        assert patch.status["state"] == "CANCELING"
    @patch('flink_util.cancel_job', cancel_job_error)   
    @patch('kopf.info', kopf_info)
    def test_canceling_delete_flink_error(self):
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
                    "state": "RUNNING",
                    "job_id": "job_id"
                }
        }
        patch = Bunch()
        patch.status = {"state": ""}
        
        beamsqltables = {}
        try:
            target.delete(body, body["spec"], patch, Logger())
        except Exception as err:
            assert type(err) == kopf.TemporaryError
            assert str(err).startswith("Error trying")
        assert not patch.status["state"] == "CANCELING"
if __name__ == '__main__':
    #test_creation()
    unittest.main()