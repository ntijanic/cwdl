class Step(object):
    def __init__(self):
        self.inputs = {}
        self.outputs = {}


class MapStep(Step):
    pass


class CLITool(object):
    pass


class Workflow(object):
    def __init__(self):
        self.inputs = {}
        self.outputs = {}
        self.steps = {}
        
    def successors(self, step_id):
        pass

    def predecessors(self, step_id):
        pass

    def dependencies(self, port_id):
        pass

    def value_for_port(self, port_id, deps):
        pass


class Job(object):
    PENDING, READY, ACTIVE, FAILED, FINISHED, ABORTED = \
        'pending', 'ready', 'active', 'failed', 'finished', 'aborted'

    TERMINATED = FAILED, FINISHED, ABORTED
    NON_TERMINATED = PENDING, READY, ACTIVE

    def __init__(self, job_id, process, step_id=None, parent_id=None):
        self.id = job_id
        self.process = process
        self.step_id = step_id
        self.parent_id = parent_id
        self.state = Job.PENDING

    def __repr__(self):
        return 'Job[%s %s %s]' % (self.state, self.process.__class__.__name__, self.id)
