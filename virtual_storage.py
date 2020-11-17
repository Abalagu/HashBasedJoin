# Created by Luming on 11/14/2020 10:32 PM
from math import ceil
from typing import List, Tuple


class Block:
    """minimal read and write unit"""
    max_tuple_per_block = 8

    def __init__(self, data=None):
        # self.num_tuples = 0
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
        else:
            raise Exception("data size exceeded: {} > {}".format(len(data), self.max_tuple_per_block))

    def get_size(self) -> int:
        return len(self.data)

    def is_full(self):
        if self.get_size() == self.max_tuple_per_block:
            return True
        else:
            return False

    def is_empty(self):
        if self.get_size() == 0:
            return True
        else:
            return False

    def clear(self):
        self.data = []


class MemoryBlock(Block):
    """memory block can extend by any size within max size, but disk block can only read and write by block size"""

    def extend(self, data: List[Tuple]):
        """append data as list of tuples.  single record should be in [record] type"""
        if len(data) + self.get_size() > self.max_tuple_per_block:
            raise Exception("Exceeds {} per block. Existing: {}, input: {}"
                            .format(self.max_tuple_per_block, self.get_size(), len(data)))
        else:
            self.data.extend(data)


class VirtualDisk:
    # blocks = []
    max_tuple_per_block = 8

    # TODO: it seems that defining self.blocks outside the init statement results in the same block
    #  list every time a virtual disk instance is initialized, such that each time it writes to the previous disk.

    # num_blocks = 0
    # read_count, write_count = 0, 0

    def __init__(self, data: List[Tuple[int, str]] = None):
        self.__blocks: List[Block] = []
        self.__read_count = 0
        self.__write_count = 0
        # self.num_blocks = 0
        if data:
            self.append(data)

    def append(self, data: List[Tuple]) -> Tuple[int, int]:
        """append the given content at the end of the current blocks, return the index range of the new blocks
        note that block range is left and right inclusive.
        """
        num_new_blocks = ceil(len(data) / self.max_tuple_per_block)
        block_range = (self.get_disk_size(), self.get_disk_size() + num_new_blocks - 1)
        new_blocks = [Block(data[i * self.max_tuple_per_block:(i + 1) * self.max_tuple_per_block])
                      for i in range(num_new_blocks)]
        self.__blocks.extend(new_blocks)
        self.__write_count += num_new_blocks
        return block_range

    def allocate(self, size: int) -> range:
        """allocate empty blocks after the tail of the current block list, return block range"""
        start = self.get_disk_size()
        end = start + size
        self.__blocks.extend([Block() for _ in range(size)])
        block_range = range(start, end)
        return block_range

    def get_block(self, idx: int) -> Block:
        if idx >= self.get_disk_size():
            raise Exception("request disk block index out of range.")
        else:
            return self.__blocks[idx]

    def get_disk_size(self) -> int:
        return len(self.__blocks)

    def read(self, idx: int) -> List[Tuple]:
        """return content within memory block of the given index"""
        block = self.get_block(idx)
        self.__read_count += 1
        return block.read()

    def write(self, data: List[Tuple], idx: int):
        """write the given data to a memory block with given index"""
        block = self.get_block(idx)
        self.__write_count += 1
        block.write(data)

    def clear(self, idx: int):
        self.get_block(idx).clear()

    def get_disk_io_stat(self):
        return self.__read_count, self.__write_count

    def describe(self):
        return "blocks used: {}, read count: {}, write count: {}".format(self.get_disk_size(), self.__read_count,
                                                                         self.__write_count)


class VirtualMemory:
    """perform read, write, and hash functions"""

    def __init__(self, size: int):
        self.__blocks = [MemoryBlock() for _ in range(size)]

    def get_block(self, idx: int) -> MemoryBlock:
        if idx >= self.get_size():
            raise Exception("request memory block index out of range.")
        else:
            return self.__blocks[idx]

    def read(self, idx: int) -> List[Tuple]:
        """return content within memory block of the given index"""
        return self.get_block(idx).read()

    def write(self, data: List[Tuple], idx: int):
        """write the given data to a memory block with given index"""
        self.get_block(idx).write(data)

    def extend(self, data: List[Tuple], idx: int):
        self.get_block(idx).extend(data)

    def clear(self, idx: int):
        self.get_block(idx).clear()

    def clear_all(self):
        for block in self.__blocks:
            block.clear()

    def get_size(self):
        return len(self.__blocks)

    def is_full(self, idx: int) -> bool:
        return self.get_block(idx).is_full()

    def is_empty(self, idx: int) -> bool:
        return self.get_block(idx).is_empty()
