import shlex
import subprocess
import time
from kopf.testing import KopfRunner

import beamsqlstatementsetoperator as target

def test_operator():
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

    print(runner.stdout)
    assert runner.exit_code == 0
    assert runner.exception is None
    assert 'Created beamsqlstatementsets beamsqlstatementset-example in namespace default' in runner.stdout

if __name__ == '__main__':
    test_operator()