# Created by Luming on 11/15/2020 1:27 AM
from typing import Dict, List, Tuple

from virtual_storage import VirtualMemory, VirtualDisk


class VirtualDatabase:

    def __init__(self, virtual_memory: VirtualMemory, virtual_disk: VirtualDisk):
        self.tables: Dict[str, Dict] = {}
        self.virtual_memory = virtual_memory
        self.virtual_disk = virtual_disk

    def add_table(self, table_name: str, block_range: Tuple[int, int], num_tuples: int):
        self.tables[table_name] = {
            'size': block_range[1] - block_range[0] + 1,
            'block_range': block_range,
            'num_tuples': num_tuples,
        }

    def get_table(self, table_name: str):
        return self.tables.get(table_name, None)

    def nature_join(self, table_name_1, table_name_2):
        pass

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
