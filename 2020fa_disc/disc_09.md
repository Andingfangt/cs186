# DIS 9

**Review:**

## ARIES Recovery Algorithm: 

**The recovery algorithm consists of 3 phases that execute in the following order:**


1. *Analysis Phase:* The entire purpose of the analysis phase is to rebuild what the Xact Table and the DPT looked like at the time of the crash.
Use the Xact Table and the DPT found in the < END CHECK POINT > record.  Start at the record at the < BEGIN CHECK POINT > record and go though the blow process.
   * 1. On any record that is not an END record: add the transaction to the the Xact Table (if necessary). Set the lastLSN of the transaction to the LSN of the record you are on
   * 2. • If the record is a COMMIT or an ABORT record, change the status of the transaction in the Xact Table accordingly
   * 3. If the record is an UPDATE record, if the page is not in the DPT add the page to the DPT and set recLSN equal to the LSN.
   * 4. If the record is an END record, remove the transaction from the Xact Table.
   * 5. At the end of the analysis phase, for any transactions that were committing we will also write the END record to the log and remove the transaction from the Xact Table. Additionally, any transactions that were running at the time of the crash need to be aborted and the abort record should be logged.
  
2. *Redo Phase:* Repeat history in order to reconstruct the state at the crash.
   Start at the smallest recLSN in the DPT cause that is the first operation that may not have made it to disk. Redo all UPDATE and CLR operations unless one of the following conditions is met:
    * 1. The page is not in the DPT. If the page is not in the DPT it implies that all changes (and thus this one!) have already been flushed to disk.
    * 2. recLSN > LSN. This is because the first update that dirtied the page occurred after this operation. This implies that the operation we are currently at has already made it to disk, otherwise it would be the recLSN.
    * 3. pageLSN (disk) ≥ LSN. If the most recent update to the page that made it to disk occurred after the current operation, then we know the current operation must have made it to disk.

3. *Undo Phase:* Start at the end of the log and works its way towards the start of the log.
   * 1. It undoes every UPDATE (only UPDATEs!) for each transaction that was active (either running or aborting) at the time of the crash(so that we do not leave the database in an intermediate state).It will not undo an UPDATE if it has already been undone (and thus a CLR record is already in the log for that UPDATE).
   * 2. For every UPDATE the undo phase undoes, it will write a corresponding CLR record to the log. CLR records have one additional field that we have not yet introduced called the undoNextLSN. The undoNextLSN stores the LSN of the next operation to be undone for that transaction (it comes from the prevLSN of the operation that you are undoing).
   * 3. Once you have undone all the operations for a transaction, write the END record for that transaction to the log.

## LSN list:

There are a lot of different LSNs, so here is a list of what each one is:

* LSN: stored in each log record. Unique, increasing, ordered identifier for each log record.
* flushedLSN: stored in memory, keeps track of the most recent log record written to disk.
* pageLSN: LSN of the last operation to update the page (in memory page may have a different pageLSN than the on disk page)
* prevLSN: stored in each log record, the LSN of the previous record written by the current record’s transaction.
* lastLSN: stored in the Xact Table, the LSN of the most recent log record written by the transaction.
* recLSN: stored in the DPT, the log record that first dirtied the page since the last checkpoint.
* undoNextLSN: stored in CLR records, the LSN of the next operation we need to undo for the current record’s transaction.




## 1. Undo Logging Q1

| Operation | Men A | Mem B | Disk A | Disk B | UNDO Log |
| --------- | ----- | ----- | ------ | ------ | -------- |
|           |       |       |    7   |  3     |   \<START T\>       |
|  READ(A)  |   7   |       |    7   |  3     |          |
|  READ(B)  |       |   3   |    7   |  3     |          |
|  WRITE(A,A+B)  |  10     |  3    |   7    |   3    | \<T, A, 7\>         |
|  WRITE(B,A-B)  |  10     |  7    |   7    |   3    | \<T, B, 3 \>         |
|  FLUSH(A)  |   10   |   7   |   10     |   3    |          |
|  FLUSH(B)  |    10   |  7    |    10    |    7   |          |
|  COMMIT  |   10   |   7   |   10    |    7   |  \<COMMIT\>        |

(b) Undo by setting A = 7, B = 3.
(c) Do all the Undos again.

## 2. Redo Logging Q1

| Operation | Men A | Mem B | Disk A | Disk B | REDO Log |
| --------- | ----- | ----- | :----: | :----: | -------- |
|           |       |       |   5    | 4     |   \<START, T\>       |
|  READ(A)  |   5   |       |   5    | 4      |          |
|  READ(B)  |   5   |   4   |        |        |          |
|  WRITE(A,A+B)  |   9   |   4   |   5    |   4    |  \<T, A, 9\>        |
|  WRITE(B,A-B)  |   9   |   5   |   5    |   4    |  \<T, B, 5\>        |
|  COMMIT  |   9   |   5   |    5   |    4   |   \<COMMIT\>       |
|  FLUSH(A)  |  9    |   5   |    9   |   4    |          |
|  FLUSH(B)  |  9    |   5   |    9   |   5    |          |

(b) Recover by REDO-ing and setting A = 9, B = 5.


## 3. Recovery Q1

(a) Analysis Phase:
|Transaction Table| | |
| :-------------: | :----: | :----: |
| Transaction | Status | lastLSN |
| T2 | ABORT | 80 |
| T3 |RUNNING  | 70 |


|Dirty Page Table | |
| :-------------: | :----: |
| Page ID | recLSN |
| P1 | 10 |
| P3 | 20 |
| P4 | 40 |
| P2 | 70 |


(b) Redo Phase:
Start at LSN = 10(smallest recLSN in the DPT).:

* 10 - UPDATE that does not meet any of the conditions.
* 20 - UPDATE that does not meet any of the conditions.
* not 30 -  only redo UPDATEs and CLRs.
* 40 - UPDATE that does not meet any of the conditions.
* 50 - UPDATE that does not meet any of the conditions.
* not 60 -  only redo UPDATEs and CLRs.
* 70 - UPDATE that does not meet any of the conditions.
* not 80 -  only redo UPDATEs and CLRs.


(c) Undo Phase:
only look for T2 and T3 in Transaction Table and undo their UPDATES.

At the end, we should abort the transactions tha were running and add the abort record <LSN = 90, Record =ABORT T2, prevLSN = 80 >


|LSN |Record | prevLSN | undoNextLSN |
| :-------------: | :----: | :----: | :----: |
| 90 | ABORT T2 | 90 | |
| 100 | CLR Undo T3: LSN 70 | 70 | 40 |
| 110 | CLR Undo T2: LSN 50| 80 | 20 |
| 120 | CLR Undo T3: LSN 40| 100 | null |
| 130 | T3 end | 120 |  |
| 140 | CLR Undo T2: LSN 20| 110 | null |
| 150 | T2 end | 140 |  |


## 4. Recovery Q2

(a) The update at LSN 60 may have been written to disk since P5 still in the DPT at this checkpoint; The update at LSN 70 was flushed to disk cause it’s not in the dirty page table at the time of the checkpoint.

(b) Analysis Phase:
start at LSN = 80.

|Transaction Table| | |
| :-------------: | :----: | :----: |
| Transaction | lastLSN | Status |
| T1 | 190 | ABORT |
| T3 | 200 | ABORT |
| T4 | 180 | ABORT |
| T5 | 210 | ABORT |

|Dirty Page Table | |
| :-------------: | :----: |
| Page ID | recLSN |
| P5 | 50 | 
| P1 | 40 | 
| P3 | 90 | 
| P2 | 160 | 

At the end,  we should abort the transactions tha were running and add the abort record.

|LSN |Record | prevLSN | undoNextLSN |
| :-------------: | :----: | :----: | :----: |
| 190 | ABORT T1 | 90 | |
| 200 | ABORT T3 | 30 | |
| 210 | ABORT T5 | 160 | |

(c) Redo Phase:

Start at LSN = 40(smallest recLSN in the DPT).:

* 40 - UPDATE that does not meet any of the conditions.
* 50 - UPDATE that does not meet any of the conditions.
* 60 - UPDATE that does not meet any of the conditions.
* not 70 - P2 recLSN = 160 > 70
* not 80 - only redo UPDATEs and CLRs.
* 90 - UPDATE that does not meet any of the conditions.
* not 100 - only redo UPDATEs and CLRs.
* 110 - UPDATE that does not meet any of the conditions.
* not 120 - only redo UPDATEs and CLRs.
* 130 - UPDATE that does not meet any of the conditions.
* not 140 - only redo UPDATEs and CLRs.
* not 150 - only redo UPDATEs and CLRs.
* 160 - UPDATE that does not meet any of the conditions.
* 180 - CLR that does not meet any of the conditions.
* not 190 - only redo UPDATEs and CLRs.
* not 200 - only redo UPDATEs and CLRs.
* not 210 - only redo UPDATEs and CLRs.

(d) Undo Phase:
only look for T1, T3, T4, T5 in Transaction Table and undo their UPDATES.

|LSN |Record | prevLSN | undoNextLSN |
| :-------------: | :----: | :----: | :----: |
| 190 | ABORT T1 | 90 | |
| 200 | ABORT T3 | 30 | |
| 210 | ABORT T5 | 160 | |
| 220 | CLR: undo T5 LSN 160 | 210 | null |
| 230 | T5 end | 220 |  |
| 240 | CLR: undo T1 LSN 90 | 190 | 70 |
| 250 | CLR: undo T1 LSN 70 | 240 | null |
| 260 | T1 end | 250 |  |
| 270 | CLR: undo T4 LSN 50 | 180 | 40 |
| 280 | CLR: undo T4 LSN 40 | 270 | null |
| 290 | T4 end | 280 |  |
| 300 | CLR: undo T3 LSN 30 | 200 | null |
| 310 | T3 end | 300 |  |
