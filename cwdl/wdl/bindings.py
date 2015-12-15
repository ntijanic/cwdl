def join(sep, *args):
    return sep.join(args)


class Step(object):
    def __init__(self, step_id, inputs, outputs, process):
        self.id = step_id
        self.inputs = inputs
        self.outputs = outputs
        self.process = process


class DataLink(object):
    def __init__(self, src, dst, src_step_id=None, dst_step_id=None):
        self.__dict__.update(locals())


class CLITool(object):
    def __init__(self, task):
        self.task = task
        self.inputs = {d.name: None for d in task.declarations}
        self.outputs = {d.name: None for d in task.outputs}


class Workflow(object):
    def __init__(self, wf):
        self.wf = wf
        self.inputs = {d.name: None for d in wf.declarations}
        self.outputs = {}
        self.steps = {}
        self.links = []
        for call in wf.calls():
            proc = CLITool(call.task)
            inputs = {join('.', call.name, k): v for k, v in call.inputs}
            outputs = {join('.', call.name, k): v for k, v in proc.outputs}
            self.steps[call.name] = Step(call.name, inputs, outputs, proc)
            for k, v in inputs.iteritems():
                src = v.ast.source_string  # Simplest case. TODO: Extract variables from ast?
                src_step = src.split('.')[0] if '.' in src else None
                link = DataLink(src, k, src_step, call.name)
                self.links.append(link)

    def successors(self, step_id):
        ids = {l.dst_step_id for l in self.links if l.src_step_id == step_id}
        return [s for s in self.steps.itervalues() if s.id in ids]

    def predecessors(self, step_id):
        ids = {l.src_step_id for l in self.links if l.dst_step_id == step_id}
        return [s for s in self.steps.itervalues() if s.id in ids]

    def dependencies(self, port_id):
        return [l.src for l in self.links if l.dst == port_id]

    def value_for_port(self, port_id, deps):
        return deps.values()[0]  # Simplest case. TODO: Evaluate expression?
