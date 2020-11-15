# Created by Luming on 11/15/2020 12:19 AM
from typing import Dict, Tuple

from numpy.random import randint, choice

from virtual_storage import VirtualDisk


def get_relation_s():
    b_range = range(10000, 50000)
    relation_s_size = 5000
    b_values = choice(b_range, relation_s_size, replace=False)
    c_values = ['C' + str(value) for value in b_values]
    relation_s = [t for t in zip(b_values, c_values)]
    return relation_s


def first_experiment():
    """generation relation S, and relation R with possible duplicate B values"""
    relation_s = get_relation_s()

    relation_r_size = 1000
    b_range = [t[0] for t in relation_s]
    b_values = choice(b_range, relation_r_size, replace=True)
    a_values = ['A' + str(b + randint(1, 100)) for b in b_values]
    relation_r = [t for t in zip(a_values, b_values)]

    block_ranges: Dict[str, Tuple] = dict()
    virtual_disk = VirtualDisk()
    block_range = virtual_disk.append(relation_s)
    block_ranges['relation_s'] = block_range


def second_experiment():
    """generate relation S, and relation R with B values randomly picked within range 20000, 30000"""
    relation_s = get_relation_s()
    relation_r_size = 1200
    b_range = range(20000, 30000)
    b_values = choice(b_range, relation_r_size, replace=True)
    a_values = ['A' + str(b + randint(1, 100)) for b in b_values]
    relation_r = [t for t in zip(a_values, b_values)]


if __name__ == '__main__':
    first_experiment()
