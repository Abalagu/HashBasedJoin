# Created by Luming on 11/15/2020 1:27 AM
from typing import Dict, List, Tuple, Iterator

from virtual_storage import VirtualMemory, VirtualDisk


def mod_hash(key: int, bucket_size: int) -> int:
    """return integer mod as hash value, ranging from 0 to bucket_size-1"""
    try:
        return key % bucket_size
    except TypeError:
        raise Exception("key: {}, type: {}".format(key, type(key)))


class VirtualDatabase:

    def __init__(self, virtual_memory: VirtualMemory, virtual_disk: VirtualDisk):
        self.tables: Dict[str, Dict] = {}
        self.memory = virtual_memory
        self.disk = virtual_disk

    def add_table(self, table_name: str, block_range: Tuple[int, int], num_tuples: int):
        self.tables[table_name] = {
            'size': block_range[1] - block_range[0] + 1,
            'block_range': block_range,
            'num_tuples': num_tuples,
        }

    def get_table(self, table_name: str) -> Dict:
        return self.tables.get(table_name, None)

    def disk_to_memory(self, disk_idx, memory_idx) -> None:
        """encourage explicit clear of memory block after usage"""
        data = self.disk.read(disk_idx)
        if not self.memory.is_empty(memory_idx):
            raise Exception("writing to non-empty memory idx: {}".format(memory_idx))
        else:
            self.memory.write(data, memory_idx)

    def memory_to_disk(self, memory_idx, disk_idx) -> None:
        data = self.memory.read(memory_idx)
        self.disk.write(data, disk_idx)

    def hash_relation(self, relation: Dict, key_idx: int) -> Dict[int, range]:
        """hash relation into buckets stored in the disk, then return the block range
        assign the last memory block as temporary storage loading from disk
        all other blocks are for hash buckets

        key_idx: idx of column to be considered as hash key
        """
        block_range = relation['block_range']
        start, end = block_range[0], block_range[1] + 1
        memory_temp_idx = self.memory.get_size() - 1
        bucket_size = self.memory.get_size() - 1

        # allocate free space within disks for all buckets, each bucket with bucket size of blocks
        # construct mapping between hash value and disk block for buckets

        bucket_disk_ranges: Dict[int, range] = dict()

        for bucket_idx in range(bucket_size):
            bucket_disk_ranges[bucket_idx] = self.disk.allocate(bucket_size)
        else:
            bucket_disk_iterators = {bucket_idx: iter(bucket_idx_range) for bucket_idx, bucket_idx_range in
                                     bucket_disk_ranges.items()}

        for disk_block_idx in range(start, end):  # first pass, phase 1
            self.disk_to_memory(disk_block_idx, memory_temp_idx)
            data = self.memory.read(memory_temp_idx)
            for t in data:
                bucket_idx = mod_hash(t[key_idx], bucket_size)
                self.memory.extend([t], bucket_idx)
                if self.memory.is_full(bucket_idx):
                    self.memory_to_disk(bucket_idx, next(bucket_disk_iterators[bucket_idx]))
                    self.memory.clear(bucket_idx)
            else:
                self.memory.clear(memory_temp_idx)
        else:
            return bucket_disk_ranges

    def nature_join(self, table_name_1: str, idx_1: int, table_name_2: str, idx_2: int):
        """hash-based two-pass algorithm for nature join
        1. hash both relations into buckets
        2. load the smaller relation into memory
        3. perform one-pass nature join with large relation blocks loaded from disk iteratively
        """
        small_table = self.get_table(table_name_1)
        small_idx, large_idx = idx_1, idx_2
        large_table = self.get_table(table_name_2)
        if small_table['size'] > large_table['size']:
            small_table, large_table = large_table, small_table
            small_idx, large_idx = large_idx, small_idx

        small_table_location = self.hash_relation(small_table, small_idx)
        large_table_location = self.hash_relation(large_table, large_idx)

    def describe_table(self, table_name: str) -> str:
        table = self.get_table(table_name)
        if not table:
            return "table {} not found.".format(table_name)
        else:
            return "name: {}, size: {}, block range: {}, num tuples: {}".format(table_name, table['size'],
                                                                                table['block_range'],
                                                                                table['num_tuples'])

    def get_tables(self) -> str:
        tables = self.tables.keys()
        ret = [self.describe_table(table) for table in tables]
        return '\n'.join(ret)

    def __str__(self):
        return self.get_tables()
