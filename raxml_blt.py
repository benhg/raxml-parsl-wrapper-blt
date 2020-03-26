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
def run_raxml(mode, name, input_phylip, cores=48):
    import random
    random_seed = random.getrandbits(64)
    return f"/local/cluster/bin/raxmlHPC -m {mode} -n {name} -s {input_phylip} -p {random_seed} -# 1000 -T {cores}"    
    
