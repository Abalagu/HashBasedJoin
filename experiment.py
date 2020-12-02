# Created by Luming on 11/15/2020 12:19 AM
from typing import Tuple

from numpy.random import randint, choice

from virtual_database import VirtualDatabase
from virtual_storage import VirtualDisk, VirtualMemory

from database_verify import init_pg_tables, relation_to_pg


def init() -> Tuple[VirtualDisk, VirtualMemory, VirtualDatabase]:
    """return virtual disk, virtual memory, virtual database"""
    disk = VirtualDisk()
    memory = VirtualMemory(size=15)
    db = VirtualDatabase(memory, disk)
    return disk, memory, db


def init_relations(disk: VirtualDisk, db: VirtualDatabase, experiment: int):
    s_b_range = range(10000, 50000)
    relation_s_size = 5000
    s_b_values = choice(s_b_range, relation_s_size, replace=False).tolist()
    s_c_values = ['C' + str(value) for value in s_b_values]
    relation_s = [t for t in zip(s_b_values, s_c_values)]
    relation_s_block_range = disk.append(relation_s)
    db.add_table('relation_s', 0, relation_s_block_range, relation_s_size)

    if experiment == 1:
        relation_r_size = 1000
        r_b_range = s_b_values
        r_b_values = choice(r_b_range, relation_r_size, replace=True).tolist()
        r_a_values = ['A' + str(b + randint(1, 100)) for b in r_b_values]
        relation_r = [t for t in zip(r_a_values, r_b_values)]
        relation_r_block_range = disk.append(relation_r)
        db.add_table('relation_r', 1, relation_r_block_range, relation_r_size)

    elif experiment == 2:
        relation_r_size = 1200
        r_b_range = range(20000, 30000)
        r_b_values = choice(r_b_range, relation_r_size, replace=True).tolist()
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
    print('DISK USAGE AT INIT:')
    print(disk.describe())

    init_relations(disk, db, 1)
    print('DISK USAGE AFTER CREATION:')
    print(disk.describe())

    table_name_1 = 'relation_r'
    table_name_2 = 'relation_s'
    table_1 = db.get_table(table_name_1)
    table_2 = db.get_table(table_name_2)
    ret = db.nature_join(table_1, table_2)
    print('DISK USAGE AFTER NATURE JOIN:')
    print(disk.describe())

    t2 = db.table_to_memory(table_name_2)
    t2_keys = [t[table_2.key_idx] for t in t2]
    selected_keys = choice(t2_keys, 20, False)
    selected_print = {key: [] for key in selected_keys}

    for key in selected_keys:
        selected_print[key] = [t for t in ret if t[1] == key]
    else:
        print(selected_print)

    init_pg_tables()
    relation_to_pg(table_name_1, db.table_to_memory(table_name_1))
    relation_to_pg(table_name_2, db.table_to_memory(table_name_2))
    print('R(A,B), S(B,C), AND JOIN RESULTS WRITTEN TO DATABASE')
    return selected_print


def second_experiment():
    """generate relation S, and relation R with B values randomly picked within range 20000, 30000"""
    print("SECOND EXPERIMENT: ")
    disk, memory, db = init()
    init_relations(disk, db, 2)
    table_name_1 = 'relation_r'
    table_name_2 = 'relation_s'
    table_1 = db.get_table(table_name_1)
    table_2 = db.get_table(table_name_2)
    ret = db.nature_join(table_1, table_2)
    ret.sort(key=lambda x: x[1])

    print('AFTER TWO PASS NATURE JOIN')
    print(disk.describe())
    print("JOINED RESULTS: ")
    print(ret)

    init_pg_tables()

    relation_to_pg(table_name_1, db.table_to_memory(table_name_1))
    relation_to_pg(table_name_2, db.table_to_memory(table_name_2))
    print('R(A,B), S(B,C), AND JOIN RESULTS WRITTEN TO DATABASE')
    return ret

# note that one experiment overrides the other for the table content within the database
