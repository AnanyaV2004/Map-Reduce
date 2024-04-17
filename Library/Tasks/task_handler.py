from Tasks.task import task

class tasks_handler:

    def __init__(self, comm):
        self.comm = comm

    def assign_tasks(self, tasks, size):

        for i in range(1, size):
            self.comm.send(tasks[self.task_pointer], dest=i)
            self.task_pointer += 1