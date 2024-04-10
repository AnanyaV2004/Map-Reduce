from task import task

class task_handler:

    def __init__(self, comm):
        self.comm = comm
        self.task_pointer = 0
        pass

    def assign_tasks(self, tasks, size):

        for i in range(1, size):
            self.comm.send(tasks[self.task_pointer], dest=i)
            self.task_pointer += 1