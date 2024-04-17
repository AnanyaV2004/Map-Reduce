class task:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.worker = None
        self.status = "unassigned" # possible status: unassigned, assigned, completed
        self.start_time = None
        self.end_time = None