<!-- Created by Luming on 11/17/2020 11:23 PM -->
# CSCE 608 Database Systems Project 2 Report Part 1: Two-pass Hash-based Join Algorithm 
* Name: Luming Xu 
* UIN: 930001415 
* Mail: xuluming@tamu.edu
* GitHub Repo: https://github.com/Abalagu/HashBasedJoin.git

## Overview
This project implements a two-pass hash-based algorithm to perform a binary operation of nature join.  Two-pass hash-based algorithm works when the two relations are too large to completely fit into the main memory.  The basic idea of the algorithm is to handle the memory constraint due to large relation size.  

The algorithm includes two phases.  In phase one, each relation loads into the main memory by disk block, then hash them to buckets according to their key column value.  Since many operations only apply to those tuples with the same key value, which is equivalent to those tuples with the same hash value, performing hash operation serves as a grouping of the relation tuples, which greatly reduces the search space for binary operations such as nature join.  

In phase two, two-pass hash-based algorithm utilizes sub-routine of one-pass algorithm.  For one-pass algorithm performing binary operations, the smaller relation fully loads into the memory, and the larger relation loads block by block from disk, achieving a pair-wise operation while minimizing the total disk I/O.  Two-pass hash-based algorithm simply converts the original problem to multiple one-pass hash problem, which reduces the constraint to that the smaller relation size(in unit of block) hashed into each bucket should not exceed the main memory size.      

### On implementation of the algorithm
Although the professor suggests using Java or C++ for this project, I select Python as the main language considering the complexity of this project and my proficiency in programming with it. Note that this implementation uses none other than primitive data structures and math library for random number generation.    

### Derivation of constraint
Suppose the selected hash function has n unique values, which corresponds to n buckets, and suppose memory has at most M blocks, and the relation size hashed to each bucket is s_1, s_2, ..., s_n.  
1. Memory should hold one block for each bucket, n <= M.  The block assigned to each bucket within the memory serves as an intermediate collection that will be transferred to the disk when it is full.    
2. For the total size of the relation hashed into each bucket, s_i, if it is the smaller relation, it should not exceed the main memory size, so that it does not violate the constraint of one-pass algorithm.  For the larger relation, the total size within each bucket does no matter.  
 
The inequalities are:
* `s_i <= M, for i = 1, 2, ..., n`
* `n <= M`
* `sum(s_i) = B(R_small)` the block size of the smaller relation.  

The memory constraint M >=sqrt(B(R_small)) in the slide page 98 is derived from an implicit assumption that each hash bucket has approximately the same total size.  With this assumption, the extreme case is that each bucket holds M blocks of relation hashed into it, and there are M buckets, in total it would be M*M>=B(R_small), M >= sqrt(B(R_small))

However, such even distribution of hashed result can only be achieved with a good hash function, along with an evenly distributed relation key value.  The input relation is often not under control of the database system, and if the distribution of hash values is a bit too uneven for a certain buckets, it may violate the one-pass algorithm constraint, while it appears doable with two-pass based algorithm.  Also, one cannot tell whether a hash function is good enough prior to the inspection of the relation key value distribution.  Therefore, the hash function needs to be benchmarked to ensure that the selected function is suitable for the relation.  During the implementation of this algorithm, the above discussed problem arises, that one cannot pre-allocate blocks on disk with size `ceil(B(R)/n)` for each bucket, otherwise certain bucket will run out faster than the others.  This problem will be discussed later.       
    
  


For more reference on two-pass hash-based algorithm, see course note 22 page 66.   
The project poses an extra constraint that join operation can only be done when the tuples are in the virtual main memory.  


## System Design
To imitate actual disk operation, 
 
## Part 1. Data Generation
A routine generates tuples for the two relations R(A, B) and S(B, C).  In the project requirement document, it mentions that attribute A and C can be of any type.  For the ease of debugging, A` = str(B + randint(1, 100)), C = str(B)`.  For example, valid tuples for R(A, B) are `['A12345', 12345], ['A23903', 23827]`; valid tuples for S(B, C) are `[18272, 'C18272'], [40504, 'C40504']`.  

## Part 2. Virtual Disk I/O
## Part 3. Hash Function
The professor mentioned that a hash function is to be requested from the system.  In this scenario where the relation key is an integer, the integer modulo function is picked to the be hash function, and shows decent performance with benchmark on high number of trials.  The benchmark script is included in the project.    

## Part 4. Join Algorithm



## Part 5. Experiment