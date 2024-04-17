from Tasks.task_handler import task_handler
from mpi4py import MPI
from Mapper.map_input_handler import input_handler
from Library.storage import store

class map_handler:
    def __init__(self, input_store, intermediate_store):
        self.__map_tasks = []
        self.intermediate_store = intermediate_store 
        self.input_store = input_store
        # all outputs will be appended to the intermediate storage
        # this will be the input for reducer and output of mapper

    def run(self, mapper_fn):
        comm = MPI.COMM_WORLD
        size = comm.Get_size()
        rank = comm.Get_rank()

        # root process
        if(rank == 0):

            map_tasks_pending = 0;
            failed_tasks = 0;

            input_tasks = input_handler(self.input_store)
            task_handler = task_handler(comm)
            task_handler.assign_tasks(input_tasks, size)

        # non root
        else:
            my_task = comm.recv(source=0)
            mapper_fn.execute(my_task, output_file)

    

        