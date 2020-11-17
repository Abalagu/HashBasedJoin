# Created by Luming on 11/14/2020 10:32 PM
from typing import List, Tuple
from math import floor, ceil


class Block:
    """minimal read and write unit"""
    max_tuple_per_block = 8

    def __init__(self, data=None):
        self.num_tuples = 0
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

    def is_empty(self):
        if self.num_tuples == 0:
            return True
        else:
            return False

    def clear(self):
        self.data = []
        self.num_tuples = 0


class VirtualDisk:
    # blocks = []
    max_tuple_per_block = 8

    # TODO: it seems that defining self.blocks outside the init statement results in the same block
    #  list every time a virtual disk instance is initialized, such that each time it writes to the previous disk.

    # num_blocks = 0
    # read_count, write_count = 0, 0

    def __init__(self, data: List[Tuple[int, str]] = None):
        self.blocks: List[Block] = []
        self.read_count = 0
        self.write_count = 0
        self.num_blocks = 0
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

    def get_block(self, idx: int) -> Block:
        if idx >= self.num_blocks:
            raise Exception("request memory block index out of range.")
        else:
            return self.blocks[idx]

    def read(self, idx: int) -> List[Tuple]:
        """return content within memory block of the given index"""
        block = self.get_block(idx)
        self.read_count += 1
        return block.read()

    def write(self, data: List[Tuple], idx: int):
        """write the given data to a memory block with given index"""
        block = self.get_block(idx)
        self.write_count += 1
        block.write(data)

    def clear(self, idx: int):
        self.get_block(idx).clear()

    def get_disk_io_stat(self):
        return self.read_count, self.write_count

    def describe(self):
        return "blocks used: {}, read count: {}, write count: {}".format(self.num_blocks, self.read_count,
                                                                         self.write_count)


class VirtualMemory:
    """perform read, write, and hash functions"""

    def __init__(self, size: int):
        self.blocks = [Block() for _ in range(size)]
        self.max_block_size = size

    def get_block(self, idx: int) -> Block:
        if idx >= self.max_block_size:
            raise Exception("request memory block index out of range.")
        else:
            return self.blocks[idx]

    def read(self, idx: int) -> List[Tuple]:
        """return content within memory block of the given index"""
        return self.get_block(idx).read()

    def write(self, data: List[Tuple], idx: int):
        """write the given data to a memory block with given index"""
        self.get_block(idx).write(data)

    def clear(self, idx: int):
        self.get_block(idx).clear()
