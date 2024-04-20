# mapreduce.py

from mpi4py import MPI
import os
from task import task
# from store import store
import tags

class map_input_handler:

    def __init__ (self,store):
        self.__tasks = []
        self.__make_tasks_array(store)
        self.__task_ptr = -1

    def __make_tasks_array(self,store):
        keys = store.get_keys()
        for key in keys:
            value = store.get_key_values(key)
            key_ = []
            key_.append(key)
            value_ = []
            value_.append(value)
            task_ = task(key_,value_)
            self.__tasks.append(task_)
        # print("tasks:",self.__tasks)
    
    def get_next_task(self):
        # print("on task pointer ",self.__task_ptr)
        self.__task_ptr+=1
        if(len(self.__tasks) > self.__task_ptr):
            return self.__tasks[self.__task_ptr]
        else:
            return None
    
    def get_all_tasks(self):
        return self.__tasks
    
 
        
        





