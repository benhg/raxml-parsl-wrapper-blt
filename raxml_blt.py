#!/local/cluster/bin/python3
import parsl
from parsl.app.app import bash_app
from parsl.config import Config
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_route





@bash_app
def run_raxml(mode, name, input_phylip, number=100, bootstrap=False, cores=48):
    import random
    random_seed = random.getrandbits(64)
    cmd_str = f"/local/cluster/bin/raxmlHPC -m {mode} -n {name} -s {input_phylip} -p {random_seed} -# {number} -T {cores}"
    if bootstrap:
        cmd_str += "-f a"
    return cmd_str


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="Mode for RaxML to use", type=str, required=True)
    parser.add_argument("--name", help="Run name for RaxML to store", type=str, required=True)
    parser.add_argument("--input", help="Input file in PHYLIP format.", type=str, required=True)
    parser.add_argument("--cores", help="Number of cores to use. Max of 48. Optional.", type=int)
    parser.add_argument("--bootstrap",  help="Tell the computer to bootstrap. No argument. Optional. Set if bootstrapping.", action="store_true")
    parser.add_argument("--iterations", help="Number of iterations for bootstrapping. Optional. Set if bootstrapping.", type=int)
    args = parser.parse_args()
    cores = 48 if not args.cores else args.cores
    iterations = 100 if not args.iterations else args.iterations
    
    
    config = Config(
    executors=[HighThroughputExecutor(worker_debug=True,
                                      cores_per_worker=cores,
                                      address=address_by_route(),
                                      provider=GridEngineProvider(walltime='10000:00:00',
                                                                  nodes_per_block=1,
                                                                  init_blocks=1,
                                                                  max_blocks=1,
                                                                  scheduler_options=f"#$ -pe smp {cores}"
                                                                  ),
                                      label="workers")
               ],
    )

    # Enable parsl logging if you want, but it prints out a lot of (useful) info
    # parsl.set_stream_logger()
    parsl.load(config)
    
    fu = run_raxml(args.mode, args.name, args.input, cores=cores, number=iterations, bootstrap=args.bootstrap).result()
