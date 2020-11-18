# Created by Luming on 11/15/2020 1:56 PM
from collections import Counter
from math import ceil

from experiment import init, init_relations
from virtual_database import VirtualDatabase


def hash_function_benchmark(db: VirtualDatabase, relation_name: str, bucket_size: int, threshold: int = None,
                            num_trial: int = 100):
    """benchmark a given hash function.
    two-pass hash-based algorithm requires that content in each bucket does not exceed the number of memory blocks.
    the number of hash bucket is no larger than the number of memory blocks

    threshold as benchmark metric, certain bucket exceeding threshold is regarded as hash function not good enough
    """
    if not threshold:
        threshold = float('inf')

    benchmark_log = []
    for i in range(num_trial):
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
            print("constraint: {} blocks.  met:violated = {}:{}"
                  .format(threshold, sum(constraint_met), num_trial - sum(constraint_met)))
        return benchmark_log
    #
    # for i in range(num_trial):
    #     bucket_disk_ranges = db.hash_relation(db.get_table(relation_name))
    #     benchmark_log.append(bucket_disk_ranges)
    #     max_block_range = max([len(bucket_range) for bucket_range in bucket_disk_ranges.values()])
    #     if max_block_range <= threshold:
    #         met += 1
    #     else:
    #         violated += 1
    # else:
    #     print("bucket size: {}, met: {}, violated: {}".format(bucket_size, met, violated))
    #     return benchmark_log


# relation_name = 'relation_r'
# for bucket_size in range(10, 16):
#     disk, memory, db = init()
#     init_relations(disk, db, 1)
#     # print(disk.describe())
#     # print(db.describe_table(relation_name))
#     benchmark_log = hash_function_benchmark(db, relation_name, bucket_size, bucket_size, 100)
#
# relation_name = 'relation_r'
# benchmark_log = dict()
# for bucket_size in range(10, 16):
#     disk, memory, db = init()
#     init_relations(disk, db, 2)
#     # print(disk.describe())
#     # print(db.describe_table(relation_name))
#     benchmark_log[bucket_size] = hash_function_benchmark(db, relation_name, bucket_size, bucket_size, 100)

disk, memory, db = init()
init_relations(disk, db, 1)
bucket_size = 14
log = hash_function_benchmark(db, 'relation_s', bucket_size, None, 100)
