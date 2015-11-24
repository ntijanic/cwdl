class Port(object):
    pass


class Process(object):
    def __init__(self):
        self.inputs = {}
        self.outputs = {}


class Step(object):
    def __init__(self):
        self.inputs = {}


class MapStep(Step):
    pass


class DataLink(object):
    def __init__(self):
        self.src = ''
        self.dst = ''
        self.src_port_id = None
        self.dst_port_id = None


class CLITool(Process):
    pass


class Workflow(Process):
    def __init__(self):
        super(Workflow, self).__init__()
        self.ports = {}
        self.steps = {}
        
    def successors(self, step_id):
        return []

    def predecessors(self, step_id):
        return []

    def incoming_links(self, port_id):
        return []


class Job(object):
    PENDING, READY, ACTIVE, FAILED, FINISHED, ABORTED = \
        'pending', 'ready', 'active', 'failed', 'finished', 'aborted'

    NON_TERMINATED = PENDING, READY, ACTIVE
    TERMINATED = FAILED, FINISHED, ABORTED

    def __init__(self, job_id, process, inputs=None, options=None, step_id=None, parent_id=None):
        self.id = job_id
        self.process = process
        self.inputs = inputs or {}
        self.options = options
        self.step_id = step_id
        self.parent_id = parent_id
        self.outputs = {}
        self.state = Job.PENDING

    def __repr__(self):
        return 'Job(%s, %s, %s)' % (self.id, self.process.__class__, self.state)
