# mapreduce.py

from mpi4py import MPI
import os
from Tasks.task import task
# from store import store

class input_handler:

    def __init__ (self,store):
        self.__tasks = []
        self.__make_tasks_array(store)
        self.__task_ptr = -1

    def __make_tasks_array(self,store):
        keys = store.get_keys()
        for key in keys:
            value = store.get_key_values(key)
            task = task(key,value)
        self.__tasks.push(task)
    
    def get_next_task(self):
        self.__task_ptr+=1
        if(len(self.__tasks) > self.__task_ptr):
            return self.__tasks[self.__task_ptr]
        else:
            return None
    
 
        
        





