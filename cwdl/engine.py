import os
import uuid
import json
from cwdl.jobstore import JobStore
from cwdl.bindings import Job, CLITool, Workflow


def cwltool_run(job, inputs):
    with open('tool.json', 'w') as fp:
        json.dump(job.process.obj, fp)
    with open('inputs.json', 'w') as fp:
        json.dump(inputs, fp)
    if os.system('cwltool tool.json inputs.json > stdout.json'):
        raise Exception('Job failed.')
    with open('stdout.json') as fp:
        return json.load(fp)


def join_id(job_id, port_id):
    return '.'.join([job_id, port_id])


def prefix_keys(job_id, a_dict):
    return {join_id(job_id, k): v for k, v in a_dict.iteritems()}


class Engine(object):
    def __init__(self, blocking_runner=cwltool_run):
        self.runner = blocking_runner
        self.jobs = JobStore()

    def submit(self, process, inputs, job_id=None):
        job = Job(job_id or str(uuid.uuid4()), process)
        job.state = Job.READY
        self.jobs.add(job)
        self.jobs.vars.update(prefix_keys(job.id, inputs))
        return job

    def get(self, job_id):
        return self.jobs.get(job_id)

    def run_all(self):
        ready = self.jobs.get_by_state(Job.READY)
        while ready:
            for job in ready:
                self.run_job(job)
                self.update_parent(job)
            ready = self.jobs.get_by_state(Job.READY)

    def update_parent(self, finished_job):
        if not finished_job.parent_id:
            return
        parent = self.jobs.get(finished_job.parent_id)
        workflow = parent.process
        for candidate in workflow.successors(finished_job.step_id):
            prereqs = [self.jobs.get_for_step(parent.id, s.id) for s in workflow.predecessors(candidate.id)]
            if all(job.state == Job.FINISHED for job in prereqs):
                self.job_ready(self.jobs.get_for_step(parent.id, candidate.id))
        if all(j.state == Job.FINISHED for j in self.jobs.get_children(parent.id)):
            outputs = {out_id: self.value_for_port(parent, out_id) for out_id in parent.process.outputs}
            self.jobs.vars.update(prefix_keys(parent.id, outputs))
            self.jobs.set_state(parent.id, Job.FINISHED)

    def run_job(self, job):
        self.jobs.set_state(job.id, Job.ACTIVE)
        if isinstance(job.process, CLITool):
            self.run_cli_tool(job)
        if isinstance(job.process, Workflow):
            self.run_workflow(job)

    def run_cli_tool(self, job):
        inputs = {k: self.jobs.vars.get(join_id(job.id, k)) for k in job.process.inputs}
        outputs = self.runner(job, inputs)
        self.jobs.vars.update(prefix_keys(job.id, outputs))
        self.jobs.set_state(job.id, Job.FINISHED)

    def run_workflow(self, job):
        for step in job.process.steps.itervalues():
            step_job = Job('.'.join([job.id, step.id]), step.process, step_id=step.id, parent_id=job.id)
            self.jobs.add(step_job)
            if not job.process.predecessors(step.id):
                self.job_ready(step_job)

    def value_for_port(self, wf_job, port_id):
        workflow = wf_job.process
        deps = {k: self.jobs.vars[join_id(wf_job.id, k)] for k in workflow.dependencies(port_id)}
        return workflow.value_for_port(port_id, deps)

    def job_ready(self, job):
        self.jobs.set_state(job.id, Job.READY)
        if not job.step_id:
            return
        parent = self.jobs.get(job.parent_id)
        step = parent.process.steps[job.step_id]
        inputs = {join_id(parent.id, inp_id): self.value_for_port(parent, inp_id) for inp_id in step.inputs}
        self.jobs.vars.update(inputs)
