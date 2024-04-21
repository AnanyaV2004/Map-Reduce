from mpi4py import MPI
import sys
import multiprocessing
import argparse

# sys.path.insert(0, '/home/shambhavi/Documents/3-2/DS/project/our-version/Map-Reduce/Library')

from storage import store
from job import job

class Map:
    def execute(self, keys, values, output_store):
        for word in values.split(' '):
            output_store.emit(word, 1)

class Reduce:
    def execute(self, key, values, output_store):
        output_store.emit(key,sum(values))
        

class Combine:
    def execute(self, key, values, output_store):
        # print("key , values recieved in combine.execute:" , key , values)

        count=0
        for value in values:
            count+=int(value)
        output_store.emit(key, count)
        # print("emitted to output store from combine:",key,count)
        


if __name__ == "__main__":
    mpi_comm = MPI.COMM_WORLD
    rank = mpi_comm.Get_rank()
    size = mpi_comm.Get_size()
    if rank == 0:
        print("MapReduce Example: Wordcount")

    default_num_workers = multiprocessing.cpu_count()
    parser = argparse.ArgumentParser(description='Options')
    parser.add_argument('--directory', '-d', type=str, help='directory containing text files for word count')
    parser.add_argument('--num-map-workers', '-m', type=int, default=default_num_workers, help='number of workers for map task')
    parser.add_argument('--num-reduce-workers', '-r', type=int, default=default_num_workers, help='number of workers for reduce task')
    args = parser.parse_args()

    if not args.directory:
        if rank == 0:
            print("no input directory provided")
        sys.exit(1)

    if rank == 0:
        print("Configuration:")
        print("source directory:", args.directory)
        print("number of map workers:", args.num_map_workers)
        print("number of reduce workers:", args.num_reduce_workers)

    input_store = store(args.directory)
    # for key in input_store.get_keys():
        # print(key,input_store.get_key_values(key))
    output_store = store()

    map_fn = Map()
    combiner_fn = Combine()
    reducer_fn = Reduce()
    job = job(args.num_map_workers, args.num_reduce_workers)
    
    job.run(map_fn, combiner_fn, reducer_fn, mpi_comm, input_store, output_store)

    if rank == 0:
        print("FINAL OUTPUT")
        keys = output_store.get_keys()
        print(len(keys))
        for key in keys:
            print(key,output_store.get_key_values(key))


        


