import uuid

from cwdl.jobstore import JobStore
from cwdl.bindings import Job, CLITool, Workflow


def mock_run(job):
    return {out_id: out_id for out_id in job.process.outputs}


def local_id(full_id):
    last = full_id.split('.')[-1]
    return last[1:] if last.startswith('#') else last


class Engine(object):
    def __init__(self, blocking_runner=mock_run):
        self.runner = blocking_runner
        self.jobs = JobStore()

    def submit(self, process, inputs):
        job = Job(str(uuid.uuid4()), process, inputs)
        job.state = Job.READY
        self.jobs.add_or_update(job)
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
        assert isinstance(parent.process, Workflow), 'Parent not workflow: %s' % parent.id
        assert finished_job.step_id, 'Only steps have parents for the moment. Scatter TBD.'
        workflow = parent.process
        for candidate in workflow.successors(finished_job.step_id):
            prereqs = [self.jobs.get_for_step(parent.id, s.id) for s in workflow.predecessors(candidate.id)]
            if all(j.state == Job.FINISHED for j in prereqs):
                ready = self.jobs.get_for_step(parent.id, candidate.id)
                ready.state = Job.READY
                self.set_inputs(ready)
                self.jobs.add_or_update(ready)
        if all(j.state == Job.FINISHED for j in self.jobs.get_children(parent.id)):
            parent.state = Job.FINISHED
            self.set_workflow_outputs(parent)
            self.jobs.add_or_update(parent)

    def run_job(self, job):
        job.state = Job.ACTIVE
        self.jobs.add_or_update(job)
        if isinstance(job.process, CLITool):
            self.run_cli_tool(job)
        if isinstance(job.process, Workflow):
            self.run_workflow(job)

    def run_cli_tool(self, job):
        job.outputs = self.runner(job)
        job.state = Job.FINISHED
        self.jobs.add_or_update(job)

    def run_workflow(self, job):
        for step in job.process.steps.itervalues():
            step_job = Job('.'.join([job.id,  step.id]), step.process, step_id=step.id, parent_id=job.id)
            if not job.process.predecessors(step.id):
                step_job.state = Job.READY
            self.jobs.add_or_update(step_job)

    def set_workflow_outputs(self, job):
        workflow = job.process
        for out_id in workflow.outputs:
            job.outputs[out_id] = self.value_for_port(job, out_id)

    def value_for_port(self, wf_job, port_id):
        links = sorted(wf_job.process.incoming_links(port_id), key=lambda x: x.pos)
        if not links:
            return wf_job.process.ports[port_id].val
        val = [self.jobs.get_for_step(wf_job.id, l.src_step_id).outputs.get(local_id(l.src)) for l in links]
        return val if len(val) > 1 else val[0]

    def set_inputs(self, job):
        parent = self.jobs.get(job.parent_id)
        workflow = parent.process
        step = workflow.steps[job.step_id]
        for inp_id in step.inputs:
            val = self.value_for_port(parent, inp_id)
            job.inputs[local_id(inp_id)] = val
