import os
from cwdl.cwl.parser import load
from cwdl.engine import Engine
from cwdl.bindings import Job


def load_cwl(name):
    return load(os.path.join(os.path.dirname(__file__), '../../cwltools', name))


def test_cwl_wf():
    wf = load_cwl('test.cwl.yaml')
    engine = Engine()
    inputs = {
        'input': {'class': 'File', 'path': 'README.md'},
    }
    job = engine.submit(wf, inputs, 'test')
    engine.run_all()
    assert engine.get(job.id).state == Job.FINISHED
    assert engine.jobs.vars['test.output']['path'].endswith('output.txt')


if __name__ == '__main__':
    test_cwl_wf()
