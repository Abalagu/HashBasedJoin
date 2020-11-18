# Created by Luming on 11/17/2020 3:20 AM
from experiment import init, init_relations

disk, memory, db = init()
init_relations(disk, db, 1)

table_name_1 = 'relation_r'
table_name_2 = 'relation_s'

table_1 = db.get_table(table_name_1)
table_2 = db.get_table(table_name_2)

# bucket_disk_ranges = db.hash_relation(table_1)

ret = db.nature_join(table_1, table_2)
