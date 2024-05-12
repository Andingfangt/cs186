# DIS 4

## 1. Buffer Management

(a)
**LRU**
```
A   * *      F
 B   F     E
  C     G *
   D   * *  *
```
$\frac{6}{14}$

**MRU**
```
A   *FA
 B
  C
   D   *GDGEDF
```
$\frac{2}{14}$

**CLOCK("second chance LRU")**
The policy works as follows when trying to evict:

* iterate through frames within the table, skipping pinned pages and wrapping around to frame 0 upon reaching the end, until the first unpinned frame with ref bit = 0 is found.
* during each iteration, if the current frame’s ref bit = 1, set the ref bit to 0 and move the clock hand to the next frame.
* upon reaching a frame with ref bit = 0, evict the existing page (and write it to disk if the dirty bit is set; then set the dirty bit to 0), read in the new page, set the frame’s ref bit to 1, and move the clock hand to the next frame.


If accessing a page currently in the buffer pool, the clock policy sets the page’s ref bit to 1 without moving the clock hand.
```
A   *F      D                             1
 B    A      F                            1
  C     G *                               0
   D   * * E                              1

Clock Hand
   3
```
$\frac{4}{14}$

(b)
**LRU**
```
A   *     E
 B   F      *
  C    G *
   D  * *  *
```
$\frac{6}{13}$

**MRU**
```
A   *F   GE
 B   
  C    
   D  *GD  *F
```
$\frac{3}{13}$

(c)
In some case, MRU could be better like when facing sequential flooding during sequential scans.

(d)
The Clock policy provides an alternative implementation that efficiently approximates LRU using a ref bit (recently referenced) column in the metadata table and a clock hand variable to track the current frame in consideration.
Don’t need to maintain entire ordering.

(e)
They have a better understanding of the access patterns, queries, and transactions performed on the data. Custom buffer replacement policies can be tailored to optimize for these patterns, improving overall performance.

## 2. Relational Algebra

Relational algebra are plans to execute queries; the many ways of writing the plans give the system room to design for optimizations.

(a)
```sql
SELECT a1.artist_name
FROM Artists as a1
JOIN Albums as a2 ON a1.artist_id = a2.artist_id
WHERE a2.genre IN {'pop', 'rock'}
```

$\pi_{artist\_name}(\sigma_{genre \in \{'pop', 'rock' \}} (Artists \bowtie Albums))$

(b)
```sql
SELECT a.artist_name
FROM Artists as a1
JOIN Albums as a2 ON a1.artist_id = a2.artist_id
WHERE a2.genre = 'pop'

INTERSECT

SELECT a1.artist_name
FROM Artists a1
JOIN Albums a2 ON a1.artist_id = a2.artist_id
WHERE a2.genre = 'rock';
```

$\pi_{artist\_name}(\sigma_{genre = 'pop'}(Albums) \bowtie Artists) \cap \pi_{artist\_name}(\sigma_{genre = 'rock'}(Albums) \bowtie Artists)$

(c)
```sql
SELECT artist_id
FROM Albums 
WHERE genre = 'pop'

UNION

SELECT a2.artist_id
FROM Albums as a2 
JOIN Songs as s on a2.album_id = s.album_id
WHERE s.weeks_in_top_40 > 10
```

$\pi_{artist\_id}(\sigma_{genre = 'pop'}(Albums)) \cup \pi_{artist\_id}(\sigma_{weeks\_in\_top\_40 > 10}(Songs) \bowtie Albums)$

(d)
```sql
SELECT artist_name
FROM Artists as a1
LEFT JOIN Albums as a2 ON a1.artist_id = a2.artist_id
WHERE a2.album_id IS NULL
```

or

```sql
SELECT artist_name
FROM Artists as a1
JOIN (
   SELECT artist_id
   FROM Artists
   EXCEPT
   SELECT artist_id
   FROM Albums
) as a2 ON a1.artist_id = a2.artist_id
```

$\pi_{artist\_name}(Artists \bowtie (\pi_{artist\_id} (Artists) - \pi_{artist\_id} (Albums)))$
