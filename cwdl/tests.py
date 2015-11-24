import os
import yaml
from cwdl.cwl.bindings import Workflow
from cwdl.engine import Engine


def test_cwl_wf():
    with open(os.path.join(os.path.dirname(__file__), '..', 'test.cwl.yaml')) as fp:
        wf = Workflow(yaml.load(fp))
    engine = Engine()
    inputs = {
        'winp1': 1,
        'winp2': 2,
    }
    job = engine.submit(wf, inputs)
    engine.run_all()
    job = engine.get(job.id)
    assert job.outputs == {'wout1': 'out', 'wout2': ['out', 'out']}


if __name__ == '__main__':
    test_cwl_wf()
