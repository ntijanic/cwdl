import wdl
import wdl.values
from cwdl.wdl.bindings import Workflow
from cwdl.engine import Engine
from cwdl.bindings import Job
from cwdl.wdl.runner import wdl_run

wdl_code = """
task my_task {
  File file

  command {
    cat ${file} > output.txt
  }
  output {
    File output = "output.txt"
  }
}

workflow my_wf {
  File inp

  call my_task {
    input: file=inp
  }
}
"""


def test():
    wdl_namespace = wdl.loads(wdl_code)
    wf = wdl_namespace.workflows[0]
    engine = Engine(wdl_run)
    engine.submit(Workflow(wf), {'inp': 'README.md'}, 'test')
    engine.run_all()
    assert engine.get('test').state == Job.FINISHED
    assert engine.jobs.vars['test.my_task.output'].endswith('output.txt')


if __name__ == '__main__':
    test()
