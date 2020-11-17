# Created by Luming on 11/15/2020 1:56 PM
from collections import Counter
from math import ceil
from typing import Tuple

from experiment import init, init_relations
from virtual_database import VirtualDatabase, mod_hash
from virtual_storage import VirtualDisk


def hash_function_benchmark(disk: VirtualDisk, db: VirtualDatabase, relation_name: str, bucket_size: int,
                            num_trial: int):
    """benchmark a given hash function.
    two-pass hash-based algorithm requires that content in each bucket does not exceed the number of memory blocks.
    the number of hash bucket is no larger than the number of memory blocks
    """
    met, violated = 0, 0
    benchmark_log = []
    block_range = db.get_table(relation_name)['block_range']

    for i in range(num_trial):
        db.hash_relation(db.get_table(relation_name), 1)

    for i in range(num_trial):
        hash_values = []
        start, end = block_range[0], block_range[1] + 1
        for block_idx in range(start, end):
            block = disk.get_block(block_idx)
            data = block.read()
            block_hash_values = [mod_hash(t[1], bucket_size) for t in data]
            hash_values.extend(block_hash_values)
        else:
            count = Counter(hash_values)
            benchmark_log.append(count)
            # min_blocks = ceil(min(count.values()) / 8)
            max_blocks = ceil(max(count.values()) / 8)
            # print("max blocks: {}".format(max_blocks))
            if max_blocks <= bucket_size:
                met += 1
            else:
                violated += 1
    else:
        print("bucket size: {}, met: {}, violated: {}".format(bucket_size, met, violated))
        return benchmark_log


relation_name = 'relation_r'
for bucket_size in range(10, 16):
    disk, memory, db = init()
    init_relations(disk, db, 1)
    # print(disk.describe())
    print(db.describe_table(relation_name))
    benchmark_log = hash_function_benchmark(disk, db, relation_name, bucket_size, 100)

relation_name = 'relation_r'
for bucket_size in range(10, 16):
    disk, memory, db = init()
    init_relations(disk, db, 2)
    # print(disk.describe())
    print(db.describe_table(relation_name))
    benchmark_log = hash_function_benchmark(disk, db, relation_name, bucket_size, 100)
