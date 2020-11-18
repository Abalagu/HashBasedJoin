# Created by Luming on 11/15/2020 1:27 AM
from math import ceil
from typing import Dict, List, Tuple

from virtual_storage import VirtualMemory, VirtualDisk


def mod_hash(key: int, bucket_size: int) -> int:
    """return integer mod as hash value, ranging from 0 to bucket_size-1"""
    try:
        return key % bucket_size
    except TypeError:
        raise Exception("key: {}, type: {}".format(key, type(key)))


class Table:
    def __init__(self, name: str, key_idx: int, disk_range: range = None, size: int = None, definition: str = None):
        self.name: str = name
        self.key_idx: int = key_idx
        self.disk_range: range = disk_range
        self.size: int = size
        self.definition: str = definition


class VirtualDatabase:

    def __init__(self, virtual_memory: VirtualMemory, virtual_disk: VirtualDisk):
        # self.tables: Dict[str, Dict] = {}
        self.tables: Dict[str, Table] = {}
        self.memory = virtual_memory
        self.disk = virtual_disk

    def add_table(self, table_name: str, key_idx: int, disk_range: range, num_tuples: int):
        relation = Table(table_name, key_idx, disk_range, num_tuples)
        self.tables[table_name] = relation
        # self.tables[table_name] = {
        #     'size': block_range[1] - block_range[0] + 1,
        #     'block_range': block_range,
        #     'num_tuples': num_tuples,
        # }

    def get_table(self, table_name: str) -> Table:
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

    def hash_relation(self, table: Table) -> Dict[int, range]:
        """hash relation into buckets stored in the disk, then return the block range
        assign the last memory block as temporary storage loading from disk
        all other blocks are for hash buckets

        key_idx: idx of column to be considered as hash key
        """

        memory_temp_idx = self.memory.get_size() - 1
        bucket_size = self.memory.get_size() - 1
        block_size_per_bucket = max(bucket_size, ceil(len(table.disk_range) / bucket_size))
        # block_size_per_bucket = ceil(len(table.disk_range) / bucket_size)
        # allocate free space within disks for all buckets, each bucket with bucket size of blocks
        # construct mapping between hash value and disk block for buckets

        bucket_disk_ranges: Dict[int, range] = dict()

        for bucket_idx in range(bucket_size):
            bucket_disk_ranges[bucket_idx] = self.disk.allocate(block_size_per_bucket)
        else:
            bucket_disk_iterators = {bucket_idx: iter(bucket_idx_range) for bucket_idx, bucket_idx_range in
                                     bucket_disk_ranges.items()}

        for disk_block_idx in table.disk_range:  # first pass, phase 1
            self.disk_to_memory(disk_block_idx, memory_temp_idx)
            data = self.memory.read(memory_temp_idx)
            for t in data:
                bucket_idx = mod_hash(t[table.key_idx], bucket_size)
                self.memory.extend([t], bucket_idx)
                if self.memory.is_full(bucket_idx):
                    self.memory_to_disk(bucket_idx, next(bucket_disk_iterators[bucket_idx]))
                    self.memory.clear(bucket_idx)
            else:
                self.memory.clear(memory_temp_idx)
        else:
            # transfer remaining in memory buckets to disk
            for bucket_idx in range(bucket_size):
                if not self.memory.is_empty(bucket_idx):
                    self.memory_to_disk(bucket_idx, next(bucket_disk_iterators[bucket_idx]))
                    self.memory.clear(bucket_idx)
            else:
                return bucket_disk_ranges

    def one_pass_nature_join(self, table_1: Table, table_2: Table) -> List[Tuple]:
        """with given column index and disk block range of two relations, perform in memory nature join
        output is store outside the virtual memory
        """
        if len(table_1.disk_range) + len(table_2.disk_range) > self.memory.get_size():
            raise Exception("relation size: {}+{}>{}, too large to fit into memory for one pass algorithm"
                            .format(table_1.disk_range, table_2.disk_range, self.memory.get_size()))

        table_memory_indices_1: List[int] = []
        table_memory_indices_2: List[int] = []
        memory_idx = 0
        result: List[Tuple] = []
        for disk_idx in table_1.disk_range:
            self.disk_to_memory(disk_idx, memory_idx)
            table_memory_indices_1.append(memory_idx)
            memory_idx += 1

        for disk_idx in table_2.disk_range:
            self.disk_to_memory(disk_idx, memory_idx)
            table_memory_indices_2.append(memory_idx)
            memory_idx += 1

        for memory_idx_1 in table_memory_indices_1:
            for t_1 in self.memory.read(memory_idx_1):
                for memory_idx_2 in table_memory_indices_2:
                    for t_2 in self.memory.read(memory_idx_2):
                        if t_1[table_1.key_idx] == t_2[table_2.key_idx]:
                            # keep key in tuple 1 in nature joined result
                            result.append(t_1 + t_2[:table_2.key_idx] + t_2[table_2.key_idx + 1:])
                    else:
                        self.memory.clear(memory_idx_2)
            else:
                self.memory.clear(memory_idx_1)
        else:
            return result

    def nature_join(self, table_1: Table, table_2: Table) -> List[Tuple]:
        """hash-based two-pass algorithm for nature join
        1. hash both relations into buckets
        2. load the smaller relation into memory
        3. perform one-pass nature join with large relation blocks loaded from disk iteratively
        """
        if table_1.size < table_2.size:
            small_table, large_table = table_1, table_2
        else:
            small_table, large_table = table_2, table_1

        small_table_bucket_disk_ranges: Dict[int, range] = self.hash_relation(small_table)
        large_table_bucket_disk_ranges: Dict[int, range] = self.hash_relation(large_table)
        joined_relation: List[Tuple] = []  # store joined relation within memory, outside virtual memory

        bucket_size = self.memory.get_size() - 1
        for bucket_idx in range(bucket_size):
            table_1_slice = Table(table_1.name, table_1.key_idx, small_table_bucket_disk_ranges[bucket_idx])
            for disk_idx in large_table_bucket_disk_ranges[bucket_idx]:
                table_2_slice = Table(table_2.name, table_2.key_idx, range(disk_idx, disk_idx + 1))
                ret = self.one_pass_nature_join(table_1_slice, table_2_slice)
                joined_relation.extend(ret)
        else:
            return joined_relation

    def describe_table(self, table_name: str) -> str:
        table = self.get_table(table_name)
        if not table:
            return "table {} not found.".format(table_name)
        else:
            return "name: {}, size: {}, block range: {}, num tuples: {}" \
                .format(table_name, table.size, table.disk_range, table.size)

    def get_tables(self) -> str:
        tables = self.tables.keys()
        ret = [self.describe_table(table) for table in tables]
        return '\n'.join(ret)

    def table_to_memory(self, table_name: str) -> List[Tuple]:
        """for debug only, load all table disk blocks to memory"""
        table = self.get_table(table_name)
        ret = []
        for disk_idx in table.disk_range:
            ret.extend(self.disk.read(disk_idx))
        else:
            return ret

    def __str__(self):
        return self.get_tables()
