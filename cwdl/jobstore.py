class JobStore(object):
    def __init__(self):
        self.jobs = {}
        self.vars = {}

    def add(self, job):
        self.jobs[job.id] = job

    def set_state(self, job_id, state):
        self.jobs[job_id].state = state

    def get(self, job_id):
        return self.jobs.get(job_id)

    def get_for_step(self, wf_job_id, step_id):
        result = [job for job in self.jobs.itervalues() if job.parent_id == wf_job_id and job.step_id == step_id]
        if not result:
            raise Exception('Job not found for step %s of %s' % (step_id, wf_job_id))
        return result[0]

    def get_by_state(self, state):
        return [j for j in self.jobs.itervalues() if j.state == state]

    def get_children(self, job_id):
        return [job for job in self.jobs.itervalues() if job.parent_id == job_id]
