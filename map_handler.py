from task_handler import task_handler
from mpi4py import MPI
from input_handler import input_handler

class map_handler:
    def __init__(self, input_dir):
        self.input_dir = input_dir

    def run(self, mapper_fn):
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        # all outputs will be appended to this file
        # this will be the input for reducer
        output_file = "out.txt"

        tasks = input_handler(self.input_dir).make_tasks_array()

        # root process
        if(rank == 0):
            task_handler(comm).assign_tasks(tasks, size)

        # non root
        else:
            my_task = comm.recv(source=0)
            mapper_fn.execute(my_task, output_file)

    

        