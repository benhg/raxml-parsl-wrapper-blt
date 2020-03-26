#!/local/cluster/bin/python3
import parsl
from parsl.app.app import bash_app
from parsl.config import Config
from parsl.providers import GridEngineProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_route


config = Config(
    executors=[HighThroughputExecutor(worker_debug=True,
                                      cores_per_worker=12,
                                      address=address_by_route(),
                                      provider=GridEngineProvider(walltime='10000:00:00',
                                                                  nodes_per_block=1,
                                                                  init_blocks=1,
                                                                  max_blocks=4,
                                                                  scheduler_options="#$ -pe smp 12"
                                                                  ),
                                      label="workers")
               ],
)

# Enable parsl logging if you want, but it prints out a lot of (useful) info
parsl.set_stream_logger()
parsl.load(config)


@bash_app
def run_raxml(mode, name, input_phylip, number=1000, cores=48):
    import random
    random_seed = random.getrandbits(64)
    return f"/local/cluster/bin/raxmlHPC -m {mode} -n {name} -s {input_phylip} -p {random_seed} -# {number} -T {cores}"


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", help="Mode for RaxML to use", type=str, required=True)
    parser.add_argument("--name", help="Run name for RaxML to store", type=str, required=True)
    parser.add_argument("--input", help="Input file in PHYLIP format.", type=str, required=True)
    parser.add_argument("--cores", help="Number of cores to use. Max of 48. Optional.", type=int)
    args = parser.parse_args()
    cores = 48 if not args.cores else args.cores
    fu = run_raxml(args.mode, args.name, args.input, cores=cores).result()
