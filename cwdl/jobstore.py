from collections import defaultdict

from cwdl.bindings import Job


class JobStore(object):
    def __init__(self):
        self.jobs = {}
        self.vars = defaultdict(dict)

    def set_var(self, job_id, key, val):
        self.vars[job_id][key] = val

    def get_var(self, job_id, key):
        return self.vars[job_id][key]

    def update_job_vars(self, job_id, variables):
        self.vars[job_id].update(variables)

    def add_or_update(self, job):
        self.jobs[job.id] = job

    def get(self, job_id):
        return self.jobs.get(job_id)

    def get_for_step(self, wf_job_id, step_id):
        result = [job for job in self.jobs.itervalues() if job.parent_id == wf_job_id and job.step_id == step_id]
        return result[0] if result else None

    def all_done(self):
        return not [j for j in self.jobs.itervalues() if j.state in Job.NON_TERMINATED]

    def get_by_state(self, state):
        return [j for j in self.jobs.itervalues() if j.state == state]

    def get_children(self, job_id):
        return [job for job in self.jobs.itervalues() if job.parent_id == job_id]
