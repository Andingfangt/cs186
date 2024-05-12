# DIS 7

## 1. Selectivity Estimation

**Useful selectivity estimation table:**
|predicate|selectivity|requirement|
|---------|-----------|-----------|
|$A=c$|$\frac{1}{\|D(A)\|} \\ \frac{1}{10}$|$\text{if index on A} \\ \text{ otherwise}$ |
|$A>c$|$\frac{max(A)-c}{max(A)-min(A)+1} \\ \frac{max(A)-c}{max(A)-min(A)} \\ \frac{1}{10} $|$\text{if index on A, and A is interger} \\ \text{if index on A, and A is float} \\ \text{otherwise}$|
|$A\leq c$|$1-(A>c) \\ \frac{1}{10}$|same above|
|$A_1=A_2$|$\frac{1}{max(\|D(A_1)\|,\|D(A_2)\|)} \\ \frac{1}{\|D(A_1)\|} \\ \frac{1}{\|D(A_2)\|} \\ \frac{1}{10}$|$\text{if index on } A_1 \text{ and } A_2 \\ \text{if index only on } A_1 \\ \text{if index only on } A_2 \\ \text{otherwise} $|
|$cond1$ AND $cond2$|$Sel(cond1)$ * $Sel(cond2)$|-|
|$cond1$ OR $cond2$|$$Sel(cond1) + Sel(cond2) - Sel(cond1) * Sel(cond2)$$|-|


1. $sel = 1, 1000*sel = 1000$
2. $sel = \frac{1}{50}, 1000*sel = 20$
3. $sel = \frac{1}{10}, 1000*sel = 100$ Since we don't have index on C
4. $sel = \frac{25+1-1}{50} = \frac{1}{2}, 1000*sel = 500$
5. $sel = \frac{25-1}{99} = \frac{24}{99}, 1000*sel = 242$
6. $sel = \frac{1}{10}, 1000*sel = 100$ Since we don't have index on C
7. $sel = sel_4 * sel_5 = \frac{24}{198}, 1000*sel = 121$
8. $sel = sel_4 * sel_6 = \frac{1}{20}, 1000*sel = 50$
9. $sel = sel_4 + sel_5 - sel_7 = \frac{41}{66}, 1000*sel = 621$
10. $sel = \frac{1}{50}, 1000*sel = 20$, Since we only have index on a
11. $sel = \frac{1}{max(50,25)} = \frac{1}{50}$, but this time, it is join, so the total tuples will be $|R|*|S| = 1000*500$, $\therefore 1000*500*sel = 10000$

## 2. Single Table Access Plans

1. Full scan on $R$ require us to read all the page in $R$, which is $1000 \text{ I/Os}$
2. An index scan on R.a:
   * 2 I/Os to reach the level above the leaf
   * 25 I/Os to read the wanted leaf pages, since all indexes have keys in the range [1, 100] with 100 distinct values and we want $R.a \leq 50$, so we need read half of the leaf pages, which is $\dfrac{1}{2} * 50 = 25 \text{ I/Os}$
   * 5000 I/Os to read the record reference pages, since this index is un-clustered, and this index is alt2 type, so for every record, we need to follow the reference to read its page, which is total $\frac{1}{2} * 10000 = 5000 \text{ I/Os}$
   * $\therefore 2+25+5000 = 5027 \text{ I/Os}$
3. An index scan on R.b:
   * 2 I/Os to reach the level above the leaf
   * 100 I/Os to read 100 leaf pages.
   * 1000 I/Os to read all the pages, since this index is clustered.
   * $\therefore 2+10+1000 = 1012 \text{ I/Os}$
4. 500 pages, since $R.a \leq 50$ reduce half of the pages
5. * Full scan on R ,T, S, which are the optimal pattern for their respective table
   * Index scan on S.b, R.b, T.c, they have interesting order cause they are used in downstream join.

## 3. Multi-table Plans

(1)
a. $R$ $BNLJ_{R.b ==S.b}$ $S$

1. The block nested loop join's I/Os cost is not relative to the order of tuples. So we just choose the cheapest scan plans, which is full scan on R and S.
2. B = 52, B-2 for R, 1 for S, 1 for output
$([R] \text{ for read every page of R}) + ([\frac{\text{passed in }[R]}{B-2}* [S]]) \\
= 1000 + \frac{500}{50} * 2000 = 21000$, since we push down the selection on R.a, only 500 pages of R will be passed along to the BNLJ operator.

b. $R$ $SMJ_{R.b ==S.b}$ $S$

1. In sorted merge join, the pre steps is to sort two merged tables. so if they are already sorted, it will reduce the cost of this join. So we prefer index scan on $R.b$ and $S.b$
2. The cost of SMJ is: cost of sorted $S$ + cost of sorted $R$ + $([R] + [S])$ = $0+0+(1102+2500) = 3602$

(2)
In pass2, we won't consider join that causes cross join, in this problem, $R$ and $T$ will cause cross join since they don't have join condition. So delete all the join between $R$ and $T$

(3)
From pass2 and so on, we consider advance the optimal minimum join cost instead of cross join, and some interesting join.

* S SMJ R and S SMJ T, they are the optimal.
* no more interesting left.

(4)
Since this query don't has ORDER BY or GROUP BY clause, no join will be consider interesting.

(5)
S SMJ R will be sorted on column b. So We could add ORDER BY b, GROUP BY b, or another join condition involving R.b or S.b to the query.

(6)
No, we only consider append join to the current optimal joins, which is known as left deep join, so this query plan will not appear.