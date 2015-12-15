import os
import uuid
import json
from cwdl.jobstore import JobStore
from cwdl.bindings import Job, CLITool, Workflow


def mock_run(job):
    return {out_id: out_id for out_id in job.process.outputs}


def cwltool_run(job, inputs):
    with open('tool.json', 'w') as fp:
        json.dump(job.process.obj, fp)
    with open('inputs.json', 'w') as fp:
        json.dump(inputs, fp)
    if os.system('cwltool tool.json inputs.json > stdout.json'):
        raise Exception('Job failed.')
    with open('stdout.json') as fp:
        return json.load(fp)


def local_id(full_id):
    last = full_id.split('.')[-1]
    return last[1:] if last.startswith('#') else last


def global_id(job_id, port_id, step_id=None):
    port_id = local_id(port_id)
    return '.'.join([job_id, port_id] if step_id is None else [job_id, step_id, port_id])


def global_outputs(job_id, outputs):
    return {global_id(job_id, k): v for k, v in outputs.iteritems()}


def global_inputs(job_id, inputs):
    return {global_id(job_id, k): v for k, v in inputs.iteritems()}


class Engine(object):
    def __init__(self, blocking_runner=cwltool_run):
        self.runner = blocking_runner
        self.jobs = JobStore()

    def submit(self, process, inputs, job_id=None):
        job = Job(job_id or str(uuid.uuid4()), process)
        job.state = Job.READY
        self.jobs.add(job)
        self.jobs.vars.update(global_inputs(job.id, inputs))
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
        assert finished_job.step_id, 'Only steps have parents for the moment. Scatter TBD.'
        assert isinstance(parent.process, Workflow), 'Parent not workflow: %s' % parent.id
        workflow = parent.process
        for candidate in workflow.successors(finished_job.step_id):
            prereqs = [self.jobs.get_for_step(parent.id, s.id) for s in workflow.predecessors(candidate.id)]
            if all(job.state == Job.FINISHED for job in prereqs):
                self.job_ready(self.jobs.get_for_step(parent.id, candidate.id))
        if all(j.state == Job.FINISHED for j in self.jobs.get_children(parent.id)):
            self.jobs.set_state(parent.id, Job.FINISHED)
            self.set_workflow_outputs(parent)

    def run_job(self, job):
        self.jobs.set_state(job.id, Job.ACTIVE)
        if isinstance(job.process, CLITool):
            self.run_cli_tool(job)
        if isinstance(job.process, Workflow):
            self.run_workflow(job)

    def run_cli_tool(self, job):
        inputs = {k: self.jobs.vars.get(global_id(job.id, k)) for k in job.process.inputs}
        outputs = self.runner(job, inputs)
        self.jobs.vars.update(global_outputs(job.id, outputs))
        self.jobs.set_state(job.id, Job.FINISHED)

    def run_workflow(self, job):
        for step in job.process.steps.itervalues():
            step_job = Job('.'.join([job.id, step.id]), step.process, step_id=step.id, parent_id=job.id)
            self.jobs.add(step_job)
            if not job.process.predecessors(step.id):
                self.job_ready(step_job)

    def set_workflow_outputs(self, job):
        outputs = {out_id: self.value_for_port(job, out_id) for out_id in job.process.outputs}
        self.jobs.vars.update(global_outputs(job.id, outputs))

    def value_for_port(self, wf_job, port_id):
        links = sorted(wf_job.process.incoming_links(port_id), key=lambda x: x.pos)
        if not links:
            return wf_job.process.inputs.get(port_id)
        ids = [global_id(wf_job.id, l.src, l.src_step_id) for l in links]
        val = [self.jobs.vars[k] for k in ids]
        return val if len(val) > 1 else val[0]

    def job_ready(self, job):
        self.jobs.set_state(job.id, Job.READY)
        if not job.step_id:
            return
        parent = self.jobs.get(job.parent_id)
        step = parent.process.steps[job.step_id]
        inputs = {global_id(job.id, inp_id): self.value_for_port(parent, inp_id) for inp_id in step.inputs}
        self.jobs.vars.update(inputs)
