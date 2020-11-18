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


Do I generate using virtual memory and virtual disk?
1. Populate virtual memory with blocks of tuples to write to disk
2. Call disk write procedure to transfer content from memory to disk
3. return written block range 

# Part 2: Virtual Disk I/O
# Part 3: Hash Function
A hash function is "good" for a relation if each resulting bucket size does not violate the block constraint, i.e., that it can fit into the memory.  In this sense, a hash function needs to be verified, and one cannot tell whether a given hash function is "good" in a general sense.   

## general issues
When hashing a large relation, one can only estimate the total tuple size for each bucket.  When performing hash in memory, if a memory bucket/block is full, then it needs to write back to disk.  If all the bucket blocks are to be stored continuously, then one needs to allocate a block range for each bucket.   
Pre-allocation of bucket range on disk makes it ea

Otherwise, when a memory bucket/block is full, it writes to the end of the disk, creating a new block.  In this case, the surrounding blocks may not belong to the same bucket, since each bucket is randomly filled.  Then there needs to be a memo on the index list of each bucket within the disk, such as:
```python
bucket_disk_idx = {
    0:[0,2,4,8],
    1:[1,3,5,7],
    # ...
}
```   

If a bucket index list exceeds memory block size, then it indicates that the chosen hash function is not good enough.  


After hashing both relations, when performing join operation, read blocks from each index list.  Since block retains a minimal read/write unit, the performance is not hindered.
For aesthetics reasons, when relation is fully hashed, one may swap blocks to cluster those belonging to the same bucket.  


 
   

## Mod function
For the convenience of experiment, first use integer modular function as the hash function.  After benchmarking, the integer modular function is sufficiently good for the given relation, and using either 13 buckets or 14 buckets will satisfy the constraint of the two-pass hash-based algorithm. 

when testing, it occurs that for relation with 1000 tuples, i.e., 125 blocks, using 14 buckets and assigning ceil(125/14)=ceil(8.9)=9 block on disk for each bucket may easily run out of block, as slight unevenness of key value distribution will cause trouble.  Therefore, certain redundancy needs to be granted, by allocating `max(num_buckets, ceil(table_block_size/num_buckets))` blocks for each bucket.  

## relation s hash function
relation s size: 5000, 5000/8 = 625 blocks, using 14 buckets, average bucket block usage=ceil(625/14)=ceil(44.6)=45,
but the benchmark result reports that the max usage out of 100 trials is 47 blocks.  

Instead of hashing to pre-allocated blocks, allocate a new block when necessary.  This results in discontinuous on disk hash bucket blocks.   

# Part 4: Join Algorithm
When performing the two-pass hash-based algorithm on nature join, 14 blocks of memory is loaded with content of one hash bucket from the smaller relation, and the remaining one memory block is iteratively loaded with on-disk content of the same bucket index from the larger relation.   

## on output of join algorithm
ref: note 22, page 65
The output is not written back to disk.  The result may not need to be stored, and it is hard to estimate the output size, since it depends on the specific operation, not the algorithm used.   


# Part 5: Experiment
## property of R(A, B)
note that it is not mentioned whether B is a key of the relation R(A, B).  
It is mentioned that A can be of any type.  For the ease of debugging, set A = 'A'+str(B+randint(1, 100)).
For example, ['A123457', 123456], ['A123458', 123456], ['A123459', 123456] are all valid tuples for relation R.

## two pass hash based algorithm constraint check
### experiment 1
R(A, B) has 1000 tuples, and is to be stored in 125 blocks.  
S(B, C) has 5000 tuples, and is to be stored in 625 blocks.

R is a smaller relation than S, and requires ceil(sqrt(125)) = 12 blocks of main memory to perform the two pass hash-based algorithm.  

## memory block allocation
total of 15 blocks
1 for temporary storage of read from disk
all the other 14 blocks may be used as hash buckets, which grants certain redundancy to unevenness of hash results.
when a memory block is full, write it back to disk and clear the content within.


# Python Relevant
defining `self.blocks` for VirtualDisk class outside the init statement results in behavior that each disk instance writes to the same disk list.

ref: https://stackoverflow.com/questions/1537202/difference-between-variables-inside-and-outside-of-init
Variable set outside __init__ belong to the class. They're shared by all instances.