class task:
    def __init__(self, key, value):
        self.id = None
        self.key = key
        self.value = value
        self.worker = -1
        self.status = "unassigned" # possible status: unassigned, assigned, completed
        self.start_time = None
        self.end_time = None
        self.last_ping_time = None