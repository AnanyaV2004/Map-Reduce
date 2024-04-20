from typing import TypeVar, Generic
from mpi4py import MPI
import heapq
from storage import store
import tags

# Define run_combine_phase function

class map_combine_handler:

    def __init__(self, intermediate_store, comm, specs):
        self.__istore = intermediate_store
        self.__comm = comm
        self.__specs = specs

    def run(self, combine_fn):
        self.run_combine_phase(combine_fn)
        self.__comm.barrier()
        print("PRINTING INTERMEDIATE STORAGE AFTER MAP COMBINE PHASE")
        for key in self.__istore.get_keys():
            print(key, self.__istore.get_key_values(key))
        self.run_shuffle_phase()

    def run_combine_phase(self, combine_fn):
          
        # if not isinstance(combiner_t, DefaultCombiner[IntermediateStore.key_t, IntermediateStore.value_t]):
        combiner_istore = store()
        
        # print(self.__istore.get_keys(),len(self.__istore.get_keys()))
        for key in self.__istore.get_keys():
            values = self.__istore.get_key_values(key)
            
            # print(key,values)
            combine_fn.execute(key, values, combiner_istore)
        
        self.__istore = combiner_istore
        # print("printing key value pairs after combine phase :\n")
        # for key in self.__istore.get_keys():
            # values = self.__istore.get_key_values(key)
            # print(key,values)
            

    def run_shuffle_phase(self):
        # workers has the list of ranks of the map processes in `comm`
        spec = self.__specs
        comm = self.__comm
        istore = self.__istore
       
        map_workers = list(range(1, spec.get_num_mappers() + 1))
        assert comm.size >= spec.get_num_mappers() + 1
    
        # workers has the list of ranks of the map processes in `comm`
        reduce_workers = list(range(1, spec.get_num_reducers() + 1))
        assert comm.size >= spec.get_num_reducers() + 1
    
        if comm.rank == 0:
            global_counts = {}
            for p in map_workers:
                counts = comm.recv(source=p, tag=tags.ShuffleIntermediateCounts)
                # print("count",counts)
                for key, c in counts.items():
                    # print("key , c",key ,c)
                    global_counts[key] = global_counts.get(key, 0) + c
            # print(global_counts)
    
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
                comm.send(process_map, dest=p, tag=tags.ShuffleDistributionMap)
        else:
            new_istore = store()
            if comm.rank in map_workers:
                counts = self.__istore.get_key_counts()
                comm.send(counts, dest=0, tag=tags.ShuffleIntermediateCounts)
    
                process_map = comm.recv(source=0, tag=tags.ShuffleDistributionMap)
    
                for key, p in process_map.items():
                    if not istore.is_key_present(key):
                        continue
                    
                    values = istore.get_key_values(key)
                    if p != comm.rank:
                        comm.send((key, values), dest=p, tag=tags.ShufflePayloadDelivery)
                    else:
                        new_istore.emit(key, values)
    
                for p in reduce_workers:
                    # if p != comm.rank:
                    comm.send(None, dest=p, tag=tags.ShufflePayloadDeliveryComplete)
    
            if comm.rank in reduce_workers:
                awaiting_completion = len(map_workers)
                while awaiting_completion:
                    status = MPI.Status()
                    if comm.probe(status=status):
                        msg = comm.recv(source=status.source, tag=status.tag)
                        if msg.tag == tags.ShufflePayloadDelivery:
                            key, values = comm.recv(source=msg.source, tag=tags.ShufflePayloadDelivery)
                            new_istore.emit(key, values)
                        elif msg.tag == tags.ShufflePayloadDeliveryComplete:
                            comm.recv(source=msg.source, tag=tags.ShufflePayloadDeliveryComplete)
                            awaiting_completion -= 1
                        else:
                            assert 0
        
            self.__istore = new_istore
    
class DefaultCombiner:
    def combine(self, key, start, end, store):
        while start != end:
            store.emit(key, start)
            start += 1


