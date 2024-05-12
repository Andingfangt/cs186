# DIS 5

## 1. General External Merge Sort

(a)
$B =4, N =108$

* Pass0: use B buffer pages. Produce $[N/B] = 27$ sorted runs of B pages each.
* Pass1: each time, we take B-1 those sorted runs, check each's current page to find the smallest one, push to the last buffer page, if buffer page is full, write it to disk, if ever sorted runs' current page is empty, update to its next page. In the end, we will convert those $27$ sorted runs in to $27/3 = 9$ new sorted runs of $B*3 = 12$ pages each.
* Pass2: recursively do Pass1, until new sorted runs number is $1$, this time, we convert those $9$ sorted runs in to $9/3 = 3$ new sorted runs of $B*3*3 = 36$ pages each.
* Pass3: since now we only have 3 sorted runs, we can sorted them at once. create the final sorted run of 108 pages, also the final sorted file.
$\therefore $ total is 4 passes.

(b)
27, 9, 3, 1

(c)
for each original page, for each pass, we need read and write it, $\therefore$ the total $I/O = 2\cdot N\cdot (4Pass) = 864$ I/Os

(d)
since it is sorted individually, we don't know the relations between tow pages, so it won't do any help, still $864$ I/Os

(e)
$\because p = 1+\log_{B-1}[\frac{N}{B}]$
$\Rightarrow B(p-1)^{B-1} \geq N$
if we want p = 1, $\Rightarrow B >= N$, which means we need the buffer memory to fit all the pages at once.

## 2. Hashing

$N$ pages, $B$ buffer pages memory.

* Step1, Divide: 
  every time read one pages, use hash function $h_p$ to dived record in pages into left $B-1$ pages as $B-1$ partitions, if one is full, flush it to disk(same partitions pages flush to disk nearby).
* Step1+, recursively Divide:
  for every partition, if it is $\geq B$ pages, we recursively do step1 for this partition use another hash function $h_{p1}$ to rehashing this partition into another $B-1$ partitions.
* Step2, Conquer:
  read an partition that fit in buffer memory and build a build-in RAM based hash table for it using hash function $h_r$, then read out the RAM hash table and write it to disk.


(a)
When no need for sorting, only require data rendezvous, such as GROUP BY without ORDER BY.

(b)
1 input buffer
B-1 partitions after Step1
B pages per partition

(c)
In this case, only the absolutely perfect hash function can evenly distributes the records into $B-1$ partitions, which is impossible. So it is very likely we will have to perform recursive external hashing?

(d)
$B=10,N=100$

1. initial partitions = $\{10,20,20,50\}$, cost = $2*100=200$
2. for $\{20,20,50\}$, which are bigger than B, recursive divide into $B-1$ partitions use uniform hash function:
   $20 \Rightarrow\{\frac{20}{9} \approx 3\}*9$
   $20 \Rightarrow\{\frac{20}{9} \approx 3\}*9$
   $50 \Rightarrow\{\frac{50}{9} \approx 6\}*9$
   cost $=(20+20+50 \text{ for read}) + (27+27+54 \text{ for write})  = 198$
3. Conquer:
   $10 \Rightarrow (10 \text{ for read}) + (0 \text{ build hash table}) + (10 \text{ for write}) = 20$
   $20 \Rightarrow ((3 \text{ for read}) + (0 \text{ build hash table}) + (3 \text{ for write}) = 6) * 9 = 54$
   $20 \Rightarrow ((3 \text{ for read}) + (0 \text{ build hash table}) + (3 \text{ for write}) = 6) * 9 = 54$
   $50 \Rightarrow ((6 \text{ for read}) + (0 \text{ build hash table}) + (6 \text{ for write}) = 12) * 9 = 108$

$\therefore $ total I/O = $200+198+20+54+54+108 = 634$ I/Os