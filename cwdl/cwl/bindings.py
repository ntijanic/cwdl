import cwdl.bindings as base


def as_list(val):
    if isinstance(val, list):
        return val
    if val is None:
        return []
    return [val]


def new_process(obj):
    cls = obj.get('class')
    if cls == 'Workflow':
        return Workflow(obj)
    if cls == 'CommandLineTool':
        return CLITool(obj)
    raise NotImplementedError('Process type %s unknown' % cls)


class Process(object):
    def __init__(self, obj):
        self.obj = obj
        self.inputs = {i['id']: None for i in obj.get('inputs', [])}
        self.outputs = {o['id']: None for o in obj.get('outputs', [])}


class Step(object):
    def __init__(self, step_id, inputs, outputs, process):
        self.id = step_id
        self.inputs = inputs
        self.outputs = outputs
        self.process = process


class DataLink(object):
    def __init__(self, src, dst, src_step_id=None, dst_step_id=None, pos=0):
        self.__dict__.update(locals())


class CLITool(Process, base.CLITool):
    def __init__(self, obj):
        super(CLITool, self).__init__(obj)


class Workflow(Process, base.Workflow):
    def __init__(self, obj):
        super(Workflow, self).__init__(obj)
        self.steps = {}
        self.links = []
        for s in obj.get('steps', []):
            inputs = {i['id']: i.get('default') for i in s.get('inputs', [])}
            outputs = {o['id']: None for o in s.get('outputs', [])}
            step = Step(s['id'], inputs, outputs, new_process(s['run']))
            self.steps[step.id] = step
        # Create DataLinks
        for s in self.obj.get('steps', []):
            for i in s.get('inputs', []):
                for pos, src in enumerate(as_list(i.get('source'))):
                    src_step_id = src.split('.')[0] if '.' in src else None
                    self.links.append(DataLink(src, i['id'], src_step_id, s['id'], pos))
        for o in self.obj.get('outputs', []):
            for pos, src in enumerate(as_list(o.get('source'))):
                src_step_id = src.split('.')[0] if '.' in src else None
                self.links.append(DataLink(src, o['id'], src_step_id, pos=pos))

    def successors(self, step_id):
        ids = {l.dst_step_id for l in self.links if l.src_step_id == step_id}
        return [s for s in self.steps.itervalues() if s.id in ids]

    def predecessors(self, step_id):
        ids = {l.src_step_id for l in self.links if l.dst_step_id == step_id}
        return [s for s in self.steps.itervalues() if s.id in ids]

    def dependencies(self, port_id):
        return [l.src for l in self.links if l.dst == port_id]

    def value_for_port(self, port_id, deps):
        links = sorted([l for l in self.links if l.dst == port_id], key=lambda x: x.pos)
        if not links:
            return None  # TODO: Return default
        ids = [l.src for l in links]
        val = [deps[k] for k in ids]
        return val if len(val) > 1 else val[0]
