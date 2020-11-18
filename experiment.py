# Created by Luming on 11/15/2020 12:19 AM
from typing import Tuple

from numpy.random import randint, choice

from virtual_database import VirtualDatabase
from virtual_storage import VirtualDisk, VirtualMemory


def init() -> Tuple[VirtualDisk, VirtualMemory, VirtualDatabase]:
    """return virtual disk, virtual memory, virtual database"""
    disk = VirtualDisk()
    memory = VirtualMemory(size=15)
    db = VirtualDatabase(memory, disk)
    return disk, memory, db


def init_relations(disk: VirtualDisk, db: VirtualDatabase, experiment: int):
    s_b_range = range(10000, 50000)
    relation_s_size = 5000
    s_b_values = choice(s_b_range, relation_s_size, replace=False)
    s_c_values = ['C' + str(value) for value in s_b_values]
    relation_s = [t for t in zip(s_b_values, s_c_values)]
    relation_s_block_range = disk.append(relation_s)
    db.add_table('relation_s', 0, relation_s_block_range, relation_s_size)

    if experiment == 1:
        relation_r_size = 1000
        r_b_range = s_b_values
        r_b_values = choice(r_b_range, relation_r_size, replace=True)
        r_a_values = ['A' + str(b + randint(1, 100)) for b in r_b_values]
        relation_r = [t for t in zip(r_a_values, r_b_values)]
        relation_r_block_range = disk.append(relation_r)
        db.add_table('relation_r', 1, relation_r_block_range, relation_r_size)
    elif experiment == 2:
        relation_r_size = 1200
        r_b_range = range(20000, 30000)
        r_b_values = choice(r_b_range, relation_r_size, replace=True)
        r_a_values = ['A' + str(b + randint(1, 100)) for b in r_b_values]
        relation_r = [t for t in zip(r_a_values, r_b_values)]
        relation_r_block_range = disk.append(relation_r)
        db.add_table('relation_r', 1, relation_r_block_range, relation_r_size)
    else:
        raise Exception("unknown experiment param.")


def first_experiment():
    """generation relation S, and relation R with possible duplicate B values"""
    print("FIRST EXPERIMENT: ")
    disk, memory, db = init()
    init_relations(disk, db, 1)
    print(db.get_tables())


def second_experiment():
    """generate relation S, and relation R with B values randomly picked within range 20000, 30000"""
    print("SECOND EXPERIMENT: ")
    disk, memory, db = init()
    init_relations(disk, db, 2)
    print(db.get_tables())


if __name__ == '__main__':
    first_experiment()
    second_experiment()
