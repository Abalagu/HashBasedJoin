# Created by Luming on 11/14/2020 10:32 PM
from typing import List, Tuple
from math import floor, ceil


class VirtualMemory:

    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data


class Block:
    """minimal read and write unit"""
    max_tuple_per_block = 8
    num_tuples = 0

    def __init__(self, data=None):
        if data:
            self.write(data)
        else:
            self.data = []

    def verify(self, data) -> bool:
        """check if the given data is of proper size, no more than 8 tuples"""
        if len(data) <= self.max_tuple_per_block:
            return True
        else:
            return False

    def read(self):
        return self.data

    def write(self, data):
        if self.verify(data):
            self.data = data
            self.num_tuples = len(data)
        else:
            raise Exception("data size exceeded: {} > {}".format(len(data), self.max_tuple_per_block))

    def is_full(self):
        if self.num_tuples == self.max_tuple_per_block:
            return True
        else:
            return False


class VirtualDisk:
    blocks: List[Block] = []
    max_tuple_per_block = 8
    num_blocks = 0
    read_count, write_count = 0, 0

    def __init__(self, data: List[Tuple[int, str]] = None):
        if data:
            self.append(data)

    def append(self, data: List[Tuple]) -> Tuple[int, int]:
        """append the given content at the end of the current blocks, return the index range of the new blocks
        note that block range is left and right inclusive.
        """
        num_new_blocks = ceil(len(data) / self.max_tuple_per_block)
        block_range = (self.num_blocks, self.num_blocks + num_new_blocks - 1)
        new_blocks = [Block(data[i * self.max_tuple_per_block:(i + 1) * self.max_tuple_per_block])
                      for i in range(num_new_blocks)]
        self.blocks.extend(new_blocks)
        self.num_blocks += num_new_blocks
        self.write_count += num_new_blocks
        return block_range

    def read(self, idx):
        self.read_count += 1
        return self.blocks[idx]

    def get_disk_io_stat(self):
        return self.read_count, self.write_count
