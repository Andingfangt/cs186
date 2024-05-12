# DIS 11

## REVIEW

### Some professional terms

* Intra-: making one run as quickly as possible.
* Inter-: making the super run as fast as possible by running many this in parallel.

### Three partitioning scheme

1. Range Partitioning:  
   * Each machine gets a certain range of values that it will store.
   * Good for queries that lookup on a specific key.
   * Use for parallel sorting and parallel sort merge join.
2. Hash Partitioning:
   * Each record is hashed and is sent to a machine matches that hash value.
   * Still perform well for key lookup, but not for range queries.
   * Is the other scheme of choice for parallel hashing and parallel hash join.
3. Round Robin Partitioning:
   * Go record by record and assign each record to the next machine.
   * Every machine is guaranteed to get the same amount of data which will actually achieve maximum parallelization.
   * Every machine will need to be activated for every query.

### Parallel algorithm

1. Parallel Sorting:
   * Range partition the table.
   * Perform local sort on each machine.
2. Parallel Hashing:
   * Hash partition the table.
   * Perform local hashing on each machine..
3. Parallel Sort Merge Join
   * Range partition each table using the same ranges on the join column.
   * Perform local sort merge join on each machine.
4. Parallel Grace Hash Join:
   * Hash partition each table using the same hash function on the join column.
   * Perform local grace hash join on each machine.
5. Broadcast Join:
   * When we want to join one very big table that is currently round robin partitioned with a very small table that is currently all stored on one machine.
   * We will send the entire small relation to every machine, and then each machine will perform a local join.
6. Symmetric Hash Join(pipeline-friendly):
   * Build two hash tables, one for each table in the join.
   * When a record from R arrives, probe the hash table for S for all of the matches. When a record from S arrives, probe the hash table for R for all of the matches.
   * Whenever a record arrives add it to its corresponding hash table after probing the other hash table for matches.
7. Hierarchical Aggregation:
   * To parallelize COUNT, each machine individually counts their records. The machines all send their counts to the coordinator machine who will then sum them together to figure out the overall count.
   * To parallelize AVG each machine must calculate the sum of all the values and the count. Then send these values to the coordinator machine. The coordinator machine then divides the sum by the count to calculate the final average.

## 1. Parallel Query Processing

1. What is the difference between inter- and intra- query parallelism?
inter-query is running many queries; intra-query is to make one query running as fast as possible.

2. What are the advantages and disadvantages of organizing data by keys?
Advantages: because data is organized by keys, search and update operations (which require
searching on the key) can be done more efficiently, since we have some sense of where the
data must be (if it exists).
Disadvantages: we must maintain the organization, which adds overhead to insertions and
updates.

3. Assume for parts (a) and (b) that we have m=3 machines with B=5 buffer pages each, along
with N=63 pages of data that don’t contain duplicates.
**(a) In the best case, what is the number of passes needed to sort the data?**

   * The first pass through the data will be for range partitioning the data among the 3 machines. In the best case, each machine will have 21 pages.
   * Then, we execute the external sorting algorithm on each machine.
     * Pass0: each time load $B=5$ pages and sort them all at once $\rightarrow$ 4 sorted runs with 5 pages and 1 sorted runs with 1 pages.
     * Pass1: use $B-1=4$ input buffer pages and 1 output buffer to merge 4 sorted runs at once $\rightarrow$ 2 sorted runs.
     * Pass2: merge those 2 sorted runs to final sorted runs.
   * $\therefore $ the sorting process takes 4 passes overall (1 pass for partitioning the data, and 3 passes for executing external sorting on each machine).
\
**(b) What is the number of passes needed to hash the data (once)? Find the best case, assum-ing that somehow the data will be uniformly distributed under the given hash function.**

   * The first pass through the data will be for range partitioning the data among the 3 machines. In the best case, each machine will have 21 pages.
   * Then, we execute the external hash algorithm on each machine.
     * Pass0: using B-1=4 output buffers and 1 input buffer to hash records to B − 1 partitions $\rightarrow$ 4 partitions with 6 pages each.
     * Pass1: since 6>B=5(not fit in memory), we simply redo to make 6 pages $\rightarrow$ 4 partitions with 2 pages each.
     * Pass2: for each partition, we read in and construct an in-memory hash table, and write that back to disk.

   * $\therefore $ the sorting process takes 4 passes overall (1 pass for partitioning the data, and 3 passes for executing hashing on each machine).
\
**(c) Assume that relation R has R pages of data, and relation S has S pages of data. If we have m machines with B buffer pages each, what is the number of passes in order to perform sort merge join (in terms of R, S, m, and B)?
Consider reading over either relation to be a pass.**

   * The first 2 pass will be partition S and R across machines using the same ranges on the join column.
   * On each machine, run sort S, sort R and merge them:
     * Sort R cost $(1+\log_{B-1}\frac{R}{mB})$ Passes
     * Sort S cost $(1+\log_{B-1}\frac{S}{mB})$ Passes
     * merge them simply cost 2 by going through both tables.

   * $\therefore 2 + (1+\log_{B-1}\frac{R}{mB}) + (1+\log_{B-1}\frac{S}{mB}) + 2$
\
**(d) Can you use pipeline parallelism to implement this join?**
No, the sorting pass must complete before the merge pass can begin.

4. All of the data for a relation with N pages starts on one machine, and we would like to
partition the data onto M machines. Assume that the size of each page is S (in KB).
(a)
Regardless of the method used, ultimately we need send $\frac{N\cdot(M-1)}{M} $ pages out to other machines, $\therefore  \frac{S\cdot N\cdot(M-1)}{M}$ KB
(b)
   * For round-robin, it still cost $ \frac{S\cdot N\cdot(M-1)}{M}$ since the data will still be divided evenly.
   * For range and hash, the best case will be 0 KB, if all the data stays on the current machine; the worst case is $S\cdot N$ KB with sending all data to the other machines.

5. Relation R has 10,000 pages, round-robin partitioned across 4 machines (M1, M2, M3, M4).
Relation S has 10 pages, all of which are only stored on M1. We want to join R and S on the
condition R.col = C.col.
Assume the size of each page is 1 KB.
(a)
use Broadcast join by sending this small S table to all the other matches.
(b)
need to send S to M2, M3, M4 $\therefore 10*1*3=30$ KB.
(c)
if R was hash partitioned across the 4 machines, we might be able to get a
lower network cost by using parallel Grace Hash Join.