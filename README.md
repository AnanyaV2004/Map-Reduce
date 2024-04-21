# Map-Reduce
Implemented map reduce functionality as python libraries. Handles node failure


## Specifications for Use:

- Input file must be .txt files in a single directory.
- The first mapper will receive input as key value pairs with key as line number and value as the line (line belongs to lines from all the files.)
- The restof the mappers will recieve input key value pairs as emitted by the reducer.
- command to run:
`mpiexec -np <no_processes> python3 main.py --directory <input_dir> --num-map-workers <num_mappers> --num-reduce-workers <num_reducers>` 