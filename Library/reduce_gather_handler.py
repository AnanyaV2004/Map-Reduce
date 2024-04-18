from typing import List  # Required for type hints
from mpi4py import MPI  
import tags

class reduce_gather_handler:
    def __init__(self, spec, comm, intermediate_store, output_store):
        self.__spec = spec
        self.__comm = comm
        # self.output_store = istore.get_store()
        self.__output_store = output_store
        self.__istore = intermediate_store

    def run(self, reduce_fn):
        self.run_reduce_phase(self.spec, self.comm, reduce_fn)
        self.__comm.barrier()
        self.run_gather_phase(self.spec, self.comm)

    def run_reduce_phase(self, reduce_fn):
        if self.__comm.rank() != 0:
            for key in self.__istore.get_keys():
                values = self.__istore.get_key_values(key)
                reduce_fn.execute(key, values, self.__output_store)
                self.log(self.__comm, f"exec reducefn on key \"{key}\" with total of {len(values)} values.")

    def run_gather_phase(self):

        comm = self.__comm
        
        if not self.spec.gather_on_master:
            return

        reduce_workers = list(range(1, self.spec.num_reduce_workers + 1))

        if comm.rank == 0:
            awaiting_completion = len(reduce_workers)
            while awaiting_completion:
                msg = comm.probe()
                if msg.tag == tags.GatherPayloadDelivery:
                    key, values = comm.recv(msg.source, tags.GatherPayloadDelivery)
                    self.output_store.emit(key, values)
                    self.log(comm, "recvd GatherPayloadDelivery with key", key, "from", msg.source)

                elif msg.tag == tags.GatherPayloadDelivery:
                    comm.recv(msg.source, tags.GatherPayloadDeliveryComplete)
                    self.log(comm, "recvd GatherPayloadDeliveryComplete from", msg.source)
                    awaiting_completion -= 1

                elif msg.tag == tags.GatherPayloadDelivery:
                    comm.recv(msg.source, tags.MapPhasePing)

                else:
                    assert 0

        elif comm.rank in reduce_workers:
            for key in self.__output_store.get_keys():
                values = self.__output_store.get_key_values(key)
                comm.send(0, tags.GatherPayloadDelivery, (key, values))
                self.log(comm, "sent GatherPayloadDelivery with key", key, "to master")

            comm.send(0, tags.GatherPayloadDeliveryComplete)
            self.log(comm, "sent GatherPayloadDeliveryComplete to master")

   #eg
    # class Specifications:
    #     pass  

    # class Communicator:
    #     def __init__(self, rank):
    #         self._rank = rank

    #     def rank(self):
    #         return self._rank

    # class IStore:
    #     def get_keys(self):
    #         return ['key1', 'key2', 'key3']  # Example keys

    #     def get_key_values(self, key):
    #         return [str(key)+"_value" ] 

    # class ReduceFunction:
    #     def reduce(self, key, values, output_store):

    #         pass

    # def log(comm, *args):
    #     # Log the message to standard output
    #     print("[Rank {}]".format(comm.rank()), *args)

# spec = Specifications()  # Initialize specifications
# comm = Communicator(1)  # Initialize communicator with rank 1
# istore = IStore()  # Initialize istore
# reduce_fn = ReduceFunction()  # Initialize reduce function
# output_store = None  # Initialize output store (adjust as needed)

# Example usage of the function
# run_reduce_phase(spec, comm)
# comm = Communicator(0)
# run_reduce_phase(spec, comm)
# comm = Communicator(2)
# run_reduce_phase(spec, comm)
