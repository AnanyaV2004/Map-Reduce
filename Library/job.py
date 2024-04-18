from storage import store
from Mapper.map_input_handler import map_input_handler
from Mapper.map_handler import map_handler
from map_combine_handler import map_combine_handler
from reduce_gather_handler import reduce_gather_handler
from specs import specs
from task import task
import mpi4py as MPI

class job:

    def __init__(self, num_mappers, num_reducers):
        
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers

    def run(self, mapper_fn, combiner_fn, reducer_fn, comm, input_store, output_store):

        specs = specs(self.num_mappers, self.num_reducers)
        intermediate_store = store()

        assert(specs.num_map_workers >= 1)
        assert(specs.num_reduce_workers >= 1)
        assert(comm.size() >= specs.num_map_workers + 1)
        assert(comm.size() >= specs.num_reduce_workers + 1)
        
        map_handler(input_store, intermediate_store, comm).run(mapper_fn)
        comm.barrier()
        map_combine_handler(intermediate_store, comm, specs).run(combiner_fn)
        comm.barrier()
        reduce_gather_handler(specs, comm, output_store).run(reducer_fn)
        comm.barrier()
