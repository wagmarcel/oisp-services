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

def kopf_info(body, reason, message):
    pass
 

def test_creation():
    with KopfRunner(['run', '-A', '--verbose', './beamsqlstatementsetoperator.py']) as runner:
        # do something while the operator is running.
        #print(runner)
        try:
            subprocess.run("kubectl delete -f ./kubernetes/examples/beamsqlstatementset.yaml", shell=True, check=True)
        except:
            print("beamsqlstatementset not existing")
        time.sleep(2)
        subprocess.run("kubectl apply -f ./kubernetes/examples/beamsqlstatementset.yaml", shell=True, check=True)
        time.sleep(1)  # give it some time to react and to sleep and to retry
        subprocess.run("kubectl delete -f ./kubernetes/examples/beamsqlstatementset.yaml", shell=True, check=True)

    #print(runner.stdout)
    assert runner.exit_code == 0
    assert runner.exception is None
    assert 'Created beamsqlstatementsets beamsqlstatementset-example in namespace default' in runner.stdout
    assert not 'Created table ddl' in runner.stdout

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
        class Logger():
            def info(self, message):
                pass
        patch = Bunch()
        patch.status = {}
        result = target.create(body, body["spec"], patch, Logger()) 
        assert result['createdOn'] is not None
        assert patch.status['state'] == "INITIALIZED"
        assert patch.status['job_id'] == None

if __name__ == '__main__':
    #test_creation()
    unittest.main()