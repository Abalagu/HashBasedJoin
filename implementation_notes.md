<!-- Created by Luming on 11/14/2020 10:24 PM -->

# System Design
## project constraint
* virtual main memory: 15 blocks
* virtual disk: unlimited
* relation S(B, C): 5000 tuples
* range of B: [10000, 50000]
* maximum number of tuples per block: 8
* join based operation can only be performed when the tuples are in the virtual main memory.


## Interface
### Virtual Memory:
supports read and write in block
internally counts the number of calls for block read and write

There should be a minimal unit of storage as "block". 

disk.write(data)

# On disk block storage capacity
The block size should be pre-determined, in this case it matters whether it is a count based or size based.
* a count based size stores for example at most 10 records, regardless of the size of each record, which would be a simplification.
* a size based block counts the size of each record, and the number of records stored within dynamically depends on the record size, which would be a more realistic simulation.

In a size based situation, a block that is able to store 10 records for relation R does not imply its ability to store 10 records for relation S, since these two relations may have very much different size for each tuple.   

One method to get object size in python is to use `sys.getsizeof()`, which returns the size in bytes.


## determine the size of python objects
ref: https://stackoverflow.com/questions/449560/how-do-i-determine-the-size-of-an-object-in-python

## block size
The block size should be a fixed and identical value for both virtual disk and virtual memory.
In windows system, the block size is often 512 bytes.

## tuple size
If a relation is strictly defined, then the size of each tuple is bounded in the data declaration statement.

```sql
create table s
(
	b integer not null
		constraint s_pk
			primary key,
	c char(6)
);
```

For a relation as defined above, the size of each tuple is fixed:
```python
from sys import getsizeof
sample_tuple = [10000, 'C10000']
tuple_size = getsizeof(sample_tuple) + sum(getsizeof(elm) for elm in sample_tuple)
```

The above tuple size is 163 bytes. 
Then it can be easily computed how many tuples a block can store for relation S.
```python
from math import floor
block_size = 1000
tuple_size = 163
s_tuple_per_block = floor(block_size / tuple_size) 
```

## conclusion
At this time, the writer noticed that the project assumes "each block can hold up to 8 tuples of the relations", therefore it is a count based storage.

# Part 1: Data Generation 
## generate key values
https://stackoverflow.com/questions/40689152/what-does-replacement-mean-in-numpy-random-choice

with replace: the sample return to the pool. the result may have duplicates
without replace: the sample does not return to the pool, the result does not have duplicates, which is suitable for key values.




# Part 2: Virtual Disk I/O
# Part 3: Hash Function
# Part 4: Join Algorithm
# Part 5: Experiment
## property of R(A, B)
note that it is not mentioned whether B is a key of the relation R(A, B).  
It is mentioned that A can be of any type.  For the ease of debugging, set A = 'A'+str(B+randint(1, 100)).
For example, ['A123457', 123456], ['A123458', 123456], ['A123459', 123456] are all valid tuples for relation R.

