from typing import List  # Required for type hints
from mpi4py import MPI  

def run_reduce_phase(spec, comm):
    if comm.rank() != 0:
        for key in istore.get_keys():
            values = istore.get_key_values(key)
            reduce_fn.reduce(key, values, output_store)
            log(comm, f"exec reducefn on key \"{key}\" with total of {len(values)} values.")
            
def run_gather_phase(self, spec, comm):
    MapPhaseBegin = 0
    MapTaskAssignment = 1
    MapTaskCompletion = 2
    MapPhasePing = 100  
    MapPhaseEnd = 4

    ShufflePhaseBegin = 5
    ShuffleIntermediateCounts = 6
    ShuffleDistributionMap = 7
    ShufflePayloadDelivery = 8
    ShufflePayloadDeliveryComplete = 9
    ShufflePhaseEnd = 10

    GatherPayloadDelivery = 11
    GatherPayloadDeliveryComplete = 12
    if not spec.gather_on_master:
        return

    reduce_workers = list(range(1, spec.num_reduce_workers + 1))

    if comm.rank == 0:
        awaiting_completion = len(reduce_workers)
        while awaiting_completion:
            msg = comm.probe()
            if msg.tag == GatherPayloadDelivery:
                key, values = comm.recv(msg.source, GatherPayloadDelivery)
                self.output_store.emit(key, values)
                log(comm, "recvd GatherPayloadDelivery with key", key, "from", msg.source)

            elif msg.tag == GatherPayloadDeliveryComplete:
                comm.recv(msg.source, GatherPayloadDeliveryComplete)
                log(comm, "recvd GatherPayloadDeliveryComplete from", msg.source)
                awaiting_completion -= 1

            elif msg.tag == MapPhasePing:
                comm.recv(msg.source, MapPhasePing)

            else:
                assert 0

    elif comm.rank in reduce_workers:
        for key in self.output_store.get_keys():
            values = self.output_store.get_key_values(key)
            comm.send(0, GatherPayloadDelivery, (key, values))
            log(comm, "sent GatherPayloadDelivery with key", key, "to master")

        comm.send(0, GatherPayloadDeliveryComplete)
        log(comm, "sent GatherPayloadDeliveryComplete to master")

# Example of usage
class Specifications:
    pass  # Implement as needed

class Communicator:
    def __init__(self, rank):
        self._rank = rank

    def rank(self):
        return self._rank

class IStore:
    def get_keys(self):
        return ['key1', 'key2', 'key3']  # Example keys

    def get_key_values(self, key):
        return [str(key)+"_value" ] 

class ReduceFunction:
    def reduce(self, key, values, output_store):
        
        pass

def log(comm, *args):
    # Log the message to standard output
    print("[Rank {}]".format(comm.rank()), *args)

spec = Specifications()  # Initialize specifications
comm = Communicator(1)  # Initialize communicator with rank 1
istore = IStore()  # Initialize istore
reduce_fn = ReduceFunction()  # Initialize reduce function
output_store = None  # Initialize output store (adjust as needed)

# Example usage of the function
run_reduce_phase(spec, comm)
comm = Communicator(0)
run_reduce_phase(spec, comm)
comm = Communicator(2)
run_reduce_phase(spec, comm)
