class job:

    def run(self):
        pass
        # call input handler
        # call map

    # void run(const Specifications& spec, boost::mpi::communicator& comm)
    #     {
    #         // we require at least one worker
    #         assert(spec.num_map_workers >= 1);
    #         assert(spec.num_reduce_workers >= 1);
    #         assert(comm.size() >= spec.num_map_workers + 1);
    #         assert(comm.size() >= spec.num_reduce_workers + 1);

    #         comm.barrier();
    #         run_map_phase(spec, comm);
    #         comm.barrier();
    #         run_combine_phase(spec, comm);
    #         comm.barrier();
    #         run_shuffle_phase(spec, comm);
    #         comm.barrier();
    #         run_reduce_phase(spec, comm);
    #         comm.barrier();
    #         run_gather_phase(spec, comm);
    #         comm.barrier();
    #     }