# Created by Luming on 12/1/2020 11:12 AM
from typing import List, Tuple, Union, Dict


class Block:
    def read(self) -> List[Tuple]:
        """return content within the block"""
        pass

    def write(self, data: List[Tuple]) -> None:
        """overwrite content within the block.  """
        pass

    def verify(self) -> bool:
        """check that the number of data within is under the constraint"""
        pass

    def clear(self) -> None:
        """clear the content within the block"""
        pass


class MemoryBlock(Block):
    def extend(self, data: List[Tuple]) -> None:
        """extend the content within the memory block"""
        pass


class VirtualDisk:
    def append(self, data: List[Tuple]) -> range:
        """append the given data at the end of the last used block.  for large amount of data, slide into pieces
        suitable for each unit block.  return the disk index range of the new blocks. """
        pass

    def allocate(self, size: int) -> range:
        """allocate empty blocks of given number after the last used block.  return block index range."""
        pass

    def get_block(self, idx: int) -> Block:
        """return a block at the given index."""
        pass

    def read(self, idx: int) -> List[Tuple]:
        """get block at the given index, read and return its content"""
        pass

    def write(self, data: List[Tuple], idx: int) -> None:
        """overwrites the disk data at the given index"""
        pass


class VirtualMemory:
    def get_block(self, idx: int) -> MemoryBlock:
        pass

    def is_empty(self, idx: int) -> bool:
        """check if the memory block is empty.  """
        pass

    def read(self, idx: int) -> List[Tuple]:
        pass

    def write(self, data: List[Tuple], idx: int) -> None:
        pass

    def extend(self, data: List[Tuple], idx: int) -> None:
        pass

    def clear(self, idx: int) -> None:
        """notice that writing to a memory block with existing content may override data.
        using block.clear() after the block usage is encouraged.  """
        pass


class Table:
    def __init__(self, name: str, key_idx: int, disk_range: Union[range, List], size: int, definition: str):
        """ Object to exchange information about a virtual table

        @param name: table name
        @param key_idx: key column index of a tuple
        @param disk_range: supports list and range.  list if stored in block fragments, range if stored contiguously.
        @param size: number of tuples within a relation
        @param definition: store the data definition language for reference if provided
        """
        pass


class VirtualDatabase:
    def __init__(self, virtual_memory: VirtualMemory, virtual_disk: VirtualDisk):
        """mounts memory and disk for relation operations"""
        pass

    def add_table(self, table_name: str, key_idx: int, disk_range: range, num_tuples: int) -> None:
        pass

    def disk_to_memory(self, disk_idx, memory_idx) -> None:
        """transfer content within a disk block to a memory block.  exception is raised if the memory is not empty
        to encourage explicit clear of memory block after usage.
        """
        pass

    def memory_to_disk(self, memory_idx, disk_idx) -> None:
        """transfer content within a memory block to a disk block"""
        pass

    def hash_relation(self, table: Table) -> Dict[int, List]:
        """hash relation into buckets stored in the disk, then return the block range"""
        pass

    def one_pass_nature_join(self, table_1: Table, table_2: Table) -> List[Tuple]:
        """perform one-pass hash-based algorithm in memory nature join.
        table sizes are checked before operation to ensure viability.
        output is not stored back to disk and shall be handled by the upper layer."""
        pass

    def nature_join(self, table_1: Table, table_2: Table) -> List[Tuple]:
        """hash-based two-pass algorithm for nature join
        1. hash both relations into buckets
        2. load the small relation content into memory from each bucket
        3. perform one-pass nature join with large relation blocks loaded from disk iteratively"""
        pass
