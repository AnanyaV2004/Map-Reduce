from mpi4py import MPI
from Mapper.map_input_handler import map_input_handler
from Library.storage import store
from datetime import datetime, timedelta
import threading
import time
from enum import Enum
from task import task

class message_tags(Enum):
        StartMapPhase = 0
        AssignedMapTask = 1
        CompletedMapTask = 2
        EndMapPhase = 3
        MapPhasePing = 4
        
class map_handler:

    def __init__(self, input_store, intermediate_store, specs):
        self.__map_tasks = []
        self.__intermediate_store = intermediate_store 
        self.__input_store = input_store
        self.__specs = specs
        # self.__failed_works = []
        

        # all outputs will be appended to the intermediate storage
        # this will be the input for reducer and output of mapper

    def run(self, mapper_fn):
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()

        assert(comm.Get_size() >= self.__specs.get_num_mappers()+1)

        # root process
        if(rank == 0):

            for i in range(1, self.__specs.get_num_mappers()+1):
                print("sending map phase begin to", i)
                comm.send(message_tags.StartMapPhase,dest=i)

            map_tasks_pending = 0
            failed_tasks = 0
            input_tasks = map_input_handler(self.__input_store)

            # assigns task to a node with rank=rank andif reassign = true, then reassigns the task
            def assign_task(rank, task, reassign=False):
                if task == None:
                    return

                task.worker = rank
                task.status = "assigned"
                task.start_time = datetime.now()
                task.last_ping_time = datetime.now()

                print("assigned task", task.id, "to process", rank)
                comm.send(task, dest=rank, tag=message_tags.AssignedMapTask)

                if not reassign:
                    task.id = len(self.__map_tasks)
                    self.__map_tasks.append(task)
                    map_tasks_pending += 1

            # Initially assign work to all nodes
            for i in range(1, self.__specs.get_num_mappers()+1):
                task = input_tasks.get_next_task()
                if(task == None):
                    break
                assign_task(i, task)


            while map_tasks_pending:
                status = comm.Iprobe()
                if not status:
                    # No incoming messages, sleep for ping frequency 
                    time.sleep(self.__specs.ping_frequency)  
                            
                    # Check for failures
                    for task in self.__map_tasks:
                        cur_time = datetime.now()
                        last_ping_time = task.last_ping_time
                        if cur_time - last_ping_time > self.__specs.ping_failure_time and task.worker != -1 and task.status != "completed":
                            print((cur_time - last_ping_time).total_seconds() * 1000, "ms delay")
                            print(comm, "worker", task.worker, "has failed; saving tasks for re-execution")
                            
                            for t2 in self.__map_tasks:
                                if t2.worker == task.worker:
                                    t2.worker = -1
                                    failed_tasks += 1
                                    
                            # self.__failed_workers.append(task.worker)
                else:
                    msg = status.Get()
                    if msg.tag == message_tags.MapPhasePing:
                        task_id = None
                        comm.recv([task_id, MPI.INT], source=msg.source, tag=message_tags.MapPhasePing)
                        self.__map_tasks[task_id].last_ping_time = datetime.now()
                        print(comm, "received MapPhasePing from", msg.source, "for task_id", task_id)
                    
                    elif msg.tag == message_tags.CompletedMapTask:

                        task_id = None
                        comm.recv([task_id, MPI.INT], source=msg.source, tag=message_tags.CompletedMapTask)
                        print(comm, "received MapTaskCompletion from", msg.source, "for task_id", task_id)
                        
                        map_tasks_pending -= 1
                        self.__map_tasks[task_id].status = "completed"
                        self.__map_tasks[task_id].end_time= datetime.now()

                        next_task = input_tasks.get_next_task()
                        if next_task:
                            assign_task(msg.source, next_task)

                        else:
                            if failed_tasks:
                                for i in range(len(self.__map_tasks)):
                                    task = self.__map_tasks[i]

                                    # re assign tasks
                                    if task.worker == -1:
                                        assign_task(msg.source, i, True)
                                        failed_tasks -= 1

            # after finishing all map tasks, send EndMapPhase message to all workers
            for i in range(1, self.__specs.get_num_mappers() + 1):

                print(comm, "sending EndMapPhase to", i)
                comm.send(None, dest=i, tag=message_tags.EndMapPhase)

        # non root processes
        else:
            my_task = comm.recv(source=0)
            mapper_fn.execute(my_task, self.__intermediate_store)
            ping_flag = True
            current_task_id = -1
            current_task = None

            def ping_root():
                while ping_flag != False:
                   if current_task_id != -1:
                       comm.send("ping", dest=0)
                       time.sleep(self.__specs.ping_frequency) 

            ping_thread = threading.Thread(target = ping_root)
            ping_thread.start()

            # receive task
            comm.recv(current_task, 0, message_tags.StartMapPhase)

            while True:
                # Probe for a message
                msg = comm.recv(source=0, tag=MPI.ANY_TAG)

                # Check message tag
                if msg.tag == message_tags.AssignedMapTask:
                    task_id, inputs = msg.data
                    current_task_id = task_id

                    print("Process", rank, "received message_tags.AssignedMapTask with task id", task_id)
                    for key, values in inputs.items():
                        print("Process", rank, "executing map function on key", key, "with", len(values), "values.")
                        for value in values:
                            mapper_fn.execute(key, value, self.__input_store)

                    current_task_id = -1
                    print("Process", rank, "sent MapTaskCompletion with task id", task_id)
                    comm.send(task_id, dest=0, tag=message_tags.CompletedMapTask)
                    
                elif msg.tag == message_tags.EndMapPhase:
                    print("Process", rank, "received MapPhaseEnd")
                    break

                else:
                    assert(0)

            ping_flag = False
            ping_thread.join()

            

            
            

        

    

        