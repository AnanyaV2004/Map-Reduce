from typing import TypeVar, Generic
import mpi4py as MPI
import heapq
from storage import store

comm = MPI.COMM_WORLD

# Define type variables
Datasource = TypeVar('Datasource')
MapFunc = TypeVar('MapFunc')
IntermediateStore = TypeVar('IntermediateStore')
CombinerFunc = TypeVar('CombinerFunc')
ReduceFunc = TypeVar('ReduceFunc')
OutputStore = TypeVar('OutputStore')
MapPhaseBegin = 0
MapTaskAssignment = 1
MapTaskCompletion = 2
MapPhasePing = 3
MapPhaseEnd = 4

ShufflePhaseBegin = 5
ShuffleIntermediateCounts = 6
ShuffleDistributionMap = 7
ShufflePayloadDelivery = 8
ShufflePayloadDeliveryComplete = 9
ShufflePhaseEnd = 10

# Define Specifications class
class Specifications(Generic[Datasource, MapFunc, IntermediateStore, CombinerFunc, ReduceFunc, OutputStore]):
    def __init__(self):
        self.ping_frequency = 50
        self.ping_failure_time = 2000
        self.ping_check_frequency = 50

# # Define Job class
# class Job(Generic[Datasource, MapFunc, IntermediateStore, CombinerFunc, ReduceFunc, OutputStore]):
#     pass

# Define run_combine_phase function

class map_combine_handler:

    def __init__(self, intermediate_store, comm, specs):
        self.__istore = intermediate_store
        self.__comm = comm
        self.__specs = specs

    def run(self, combine_fn):
        self.run_combine_phase()
        self.__comm.barrier()
        self.run_shuffle_phase()

    def run_combine_phase(self, combine_fn):

        # if not isinstance(combiner_t, DefaultCombiner[IntermediateStore.key_t, IntermediateStore.value_t]):
        combiner_istore = store()
        for key in istore.get_keys():
            values = istore.get_key_values(key)
            combine_fn(key, values, combiner_istore)
        
        istore = combiner_istore


    def run_shuffle_phase(self):
        # workers has the list of ranks of the map processes in `comm`
        spec = self.__specs
        comm = self.__comm
        istore = self.__istore

        map_workers = list(range(1, spec.num_mappers + 1))
        assert comm.size >= spec.num_mappers + 1
    
        # workers has the list of ranks of the map processes in `comm`
        reduce_workers = list(range(1, spec.num_reducers + 1))
        assert comm.size >= spec.num_reducers + 1
    
        if comm.rank == 0:
            global_counts = {}
            for p in map_workers:
                counts = comm.recv(source=p, tag=ShuffleIntermediateCounts)
    
                for key, c in counts.items():
                    global_counts[key] = global_counts.get(key, 0) + c
    
            key_counts = [(value, key) for key, value in global_counts.items()]
            key_counts.sort(reverse=True)
    
            load_balancer_pq = [(0, i) for i in range(len(reduce_workers))]
            heapq.heapify(load_balancer_pq)
    
            process_map = {}
            for count, key in key_counts:
                min_makespan, min_reduce_worker_idx = heapq.heappop(load_balancer_pq)
                process_map[key] = reduce_workers[min_reduce_worker_idx]
                heapq.heappush(load_balancer_pq, (min_makespan + count, min_reduce_worker_idx))
    
            for p in map_workers:
                comm.send(process_map, dest=p, tag=ShuffleDistributionMap)
        else:
            new_istore = IntermediateStore()
            if comm.rank in map_workers:
                counts = istore.get_key_counts()
                comm.send(counts, dest=0, tag=ShuffleIntermediateCounts)
    
                process_map = comm.recv(source=0, tag=ShuffleDistributionMap)
    
                for key, p in process_map.items():
                    if not istore.is_key_present(key):
                        continue
                    
                    values = istore.get_key_values(key)
                    if p != comm.rank:
                        comm.send((key, values), dest=p, tag=ShufflePayloadDelivery)
                    else:
                        new_istore.emit(key, values)
    
                for p in reduce_workers:
                    # if p != comm.rank:
                    comm.send(None, dest=p, tag=ShufflePayloadDeliveryComplete)
    
            if comm.rank in reduce_workers:
                awaiting_completion = len(map_workers)
                while awaiting_completion:
                    msg = comm.probe()
                    if msg.tag == ShufflePayloadDelivery:
                        key, values = comm.recv(source=msg.source, tag=ShufflePayloadDelivery)
                        new_istore.emit(key, values)
                    elif msg.tag == ShufflePayloadDeliveryComplete:
                        comm.recv(source=msg.source, tag=ShufflePayloadDeliveryComplete)
                        awaiting_completion -= 1
                    else:
                        assert 0
    
            istore = new_istore
    
class DefaultCombiner:
    def combine(self, key, start, end, store):
        while start != end:
            store.emit(key, start)
            start += 1


