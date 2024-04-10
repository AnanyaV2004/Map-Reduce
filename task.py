class task:
    def __init__(self, id, line):
        self.id = id
        self.line = line
        self.worker = None
        self.status = "unassigned"
        self.start_time = None
        self.end_time = None

        # possible status: unassigned, assigned, completed