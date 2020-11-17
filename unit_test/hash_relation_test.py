# Created by Luming on 11/17/2020 3:20 AM
from experiment import init, init_relations

disk, memory, db = init()
init_relations(disk, db, 1)

relation_name = 'relation_r'
key_idx = 1

relation = db.get_table(relation_name)
bucket_disk_ranges = db.hash_relation(relation, key_idx)
