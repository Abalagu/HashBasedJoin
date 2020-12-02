# Created by Luming on 11/15/2020 1:56 PM
from collections import Counter
from math import ceil

from experiment import init, init_relations
from virtual_database import VirtualDatabase


def hash_function_benchmark(experiment_no: int, relation_name: str, bucket_size: int, threshold: int = None,
                            num_trial: int = 100):
    """benchmark a given hash function.
    two-pass hash-based algorithm requires that content in each bucket does not exceed the number of memory blocks.
    the number of hash bucket is no larger than the number of memory blocks

    threshold as benchmark metric, certain bucket exceeding threshold is regarded as hash function not good enough
    """
    if not threshold:
        threshold = float('inf')

    disk, memory, db = init()
    benchmark_log = []
    for i in range(num_trial):
        init_relations(disk, db, experiment_no)
        table = db.get_table(relation_name)
        tuples = db.table_to_memory(relation_name)

        hash_values = [t[table.key_idx] % bucket_size for t in tuples]
        counter = Counter(hash_values)
        # report the max block used
        max_block = ceil(max(counter.values()) / 8)
        benchmark_log.append(max_block)
    else:
        if threshold:
            constraint_met = [block <= threshold for block in benchmark_log]
            print("experiment {}, bucket size: {} blocks.  max block usage: {}, met:violated = {}:{}"
                  .format(experiment_no, bucket_size, max(benchmark_log), sum(constraint_met),
                          num_trial - sum(constraint_met)))
        return benchmark_log


ret = []
for experiment_no in [1, 2]:
    for bucket_size in [12, 13, 14]:
        log = hash_function_benchmark(experiment_no, 'relation_r', bucket_size, bucket_size, 100)
        max_block_usage = max(log)
        record = (experiment_no, bucket_size, max_block_usage)
        ret.append(record)

for bucket_size in [12, 13, 14]:
    average_block_size = 5000 / 8 / bucket_size
    log = hash_function_benchmark(1, 'relation_s', bucket_size, None, 100)
    print('average block size per bucket: {:.2f}, max block size per bucket: {}'.format(average_block_size, max(log)))
