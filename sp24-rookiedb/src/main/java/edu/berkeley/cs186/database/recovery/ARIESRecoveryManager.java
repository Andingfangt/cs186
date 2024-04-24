package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }


    // Some help function for commit abort and end.

    /**
     * Use for create new record for commit, abort and end.
     *
     * @param transNum transaction being running.
     * @param type need record type
     * @return the new needed record
     */
    public LogRecord createRecord(long transNum, String type) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // use Transaction Table to get the lastLSN which is also the prevLSN that stores the last operation from the same transaction.
        long prevLSN = transactionTableEntry.lastLSN;

        // create the new needed record
        switch (type) {
            case "commit":
                return new CommitTransactionLogRecord(transNum, prevLSN);
            case "abort":
                return new AbortTransactionLogRecord(transNum, prevLSN);
            case "end":
                return new EndTransactionLogRecord(transNum, prevLSN);
            default:
                return null;
        }
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // append commit record
        // create the new commit record. (this record's LSN is handled in LongManager#appendToLog)
        LogRecord commitTransactionLogRecord = createRecord(transNum, "commit");

        // append the record to the log and get its LSN
        long recordLSN = logManager.appendToLog(commitTransactionLogRecord);

        // In commit the commit record needs to be flushed to disk before the commit call returns to ensure durability.
        // flush the log into disk
        flushToLSN(recordLSN);

        // update the transaction status
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMMITTING);
        // update its LastLSN.
        transactionTableEntry.lastLSN = recordLSN;

        // return the LSN of the commit record
        return recordLSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // append abort record
        // create the new abort record. (this record's LSN is handled in LongManager#appendToLog)
        LogRecord abortTransactionLogRecord = createRecord(transNum, "abort");

        // append the record to the log and get its LSN
        long recordLSN = logManager.appendToLog(abortTransactionLogRecord);

        // update the transaction status
        transactionTableEntry.transaction.setStatus(Transaction.Status.ABORTING);
        // update its LastLSN.
        transactionTableEntry.lastLSN = recordLSN;

        // return the LSN of the abort record
        return recordLSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // if the transaction is aborting, should roll back changes using rollbackToLSN helper function.
        if (transactionTableEntry.transaction.getStatus().equals(Transaction.Status.ABORTING)) {
            rollbackToLSN(transNum, 0);
        }

        // append end record
        // create the new end record. (this record's LSN is handled in LongManager#appendToLog)
        LogRecord abortTransactionLogRecord = createRecord(transNum, "end");
        // append the record to the log and get its LSN
        long recordLSN = logManager.appendToLog(abortTransactionLogRecord);

        // remove this transaction from Transaction Table
        transactionTable.remove(transNum);
        // update the transaction status
        transactionTableEntry.transaction.setStatus(Transaction.Status.COMPLETE);

        // return the LSN of the end record
        return recordLSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);

        //The undo phase will start at the end of the log and works its way towards the start of the log.
        while (currentLSN > LSN) {
            LogRecord currRecord = logManager.fetchLogRecord(currentLSN);
            // undoes every UPDATE Logs (only UPDATEs!), use isUndoable to check.
            if (currRecord.isUndoable()) {
                // Get a compensation log record (CLR) by calling undo on the record
                LogRecord CLR = currRecord.undo(transactionEntry.lastLSN);
                // Append the CLR and update transaction LastLSN.
                transactionEntry.lastLSN = logManager.appendToLog(CLR);
                // Call redo on the CLR to perform the undo
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            // update the current LSN to that of the next record to undo(Which is the prevLSN)
            currentLSN = currRecord.getPrevLSN().orElse(-1L);
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);

        TransactionTableEntry transactionTableEntry = transactionTable.get(transNum);

        // append update record
        // use Transaction Table to get the lastLSN which is also the prevLSN that stores the last operation from the same transaction.
        long prevLSN = transactionTableEntry.lastLSN;
        // create the new UPDATE record. (this record's LSN is handled in LongManager#appendToLog)
        LogRecord updatePageLogRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        // append the record to the log and get its LSN
        long recordLSN = logManager.appendToLog(updatePageLogRecord);

        // Update Transaction Table by updating the lastLSN
        transactionTableEntry.lastLSN = recordLSN;

        // Update DPT
        // if this pageNum is not in DPT, add it.
        if (!dirtyPageTable.containsKey(pageNum)) {
            dirtyPageTable.put(pageNum, recordLSN);
        }

        // return the LSN of the update record
        return recordLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // By just simply using #rollbackToLSN....
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // First, create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // DONE(proj5): generate end checkpoint record(s) for DPT and transaction table
        // iterate through the dirtyPageTable and copy the entries.
        int numDPTRecords = 0, numTxnTableRecords = 0;
        for (Long pageNum : dirtyPageTable.keySet()) {
            //  If at any point, copying the current record would cause the end checkpoint record to be too large,
            //  an end checkpoint record with the copied DPT entries should be appended to the log.
            if (!EndCheckpointLogRecord.fitsInOneRecord(numDPTRecords + 1, numTxnTableRecords)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                // initial all.
                chkptDPT = new HashMap<>();
                numDPTRecords = 0;
            }
            // else copy this entries to chkptDPT
            chkptDPT.put(pageNum, dirtyPageTable.get(pageNum));
            numDPTRecords++;
        }

        // iterate through the transaction table, and copy the status/lastLSN, outputting end checkpoint records only as needed.
        for (Long transNum : transactionTable.keySet()) {
            //  If at any point, copying the current record would cause the end checkpoint record to be too large,
            //  an end checkpoint record with the copied DPT entries should be appended to the log.
            if (!EndCheckpointLogRecord.fitsInOneRecord(numDPTRecords, numTxnTableRecords + 1)) {
                LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(endRecord);
                // initial all.
                chkptDPT = new HashMap<>();
                chkptTxnTable = new HashMap<>();
                numDPTRecords = 0;
                numTxnTableRecords = 0;
            }
            // else copy the status/lastLSN to chkptTxnTable
            TransactionTableEntry transactionEntry = transactionTable.get(transNum);
            Pair<Transaction.Status, Long> value = new Pair<>(transactionEntry.transaction.getStatus(), transactionEntry.lastLSN);
            chkptTxnTable.put(transNum, value);
            numTxnTableRecords++;
        }

        // Last end checkpoint record when nothing is left to be written
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed into disk before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    // some help function for #restartAnanlysis /////////////////////////////

    /**
     * The following applies to record
     * with a non-empty result for LogRecord#getTransNum().
     * These are the records that involve a transaction,
     * and therefore, we need to update the transaction table
     * whenever we encounter one of these records.
     */
    public void logRecordsForTransactionOperations(LogRecord record) {
        long transNum = record.getTransNum().get();
        // If the transaction is not in the transaction table
        if (!transactionTable.containsKey(transNum)) {
            // use newTransaction function object to create a Transaction object
            Transaction transaction = newTransaction.apply(transNum);
            // use startTransaction to add it to txnTable.
            startTransaction(transaction);
        }
        // update this transaction lastLSN
        transactionTable.get(transNum).lastLSN = record.LSN;
    }

    /**
     * The following applies to record
     * with a non-empty result for LogRecord#getPageNum().
     * These are the records that involve to Page,
     * and therefore, we need to update the DPT.
     */
    public void logRecordsForPageOperations(LogRecord record) {
        long pageID = record.getPageNum().get();
        LogType type = record.getType();
        // case this record is UpdatePage/UndoUpdatePage
        if (type.equals(LogType.UPDATE_PAGE) || type.equals(LogType.UNDO_UPDATE_PAGE)) {
            // if this page is already in DPT, do nothing,
            // else add it with recLSN
            if (!dirtyPageTable.containsKey(pageID)) {
                dirtyPageTable.put(pageID, record.LSN);
            }
        }
        // case this record is FreePage/UndoAllocPage.
        else if (type.equals(LogType.FREE_PAGE) || type.equals(LogType.UNDO_ALLOC_PAGE)){
            // remove from DPT
            dirtyPageTable.remove(pageID);
        }
        // do anything for AllocPage/UndoFreePage>
    }

    /**
     * The following applies to CommitTransaction/AbortTransaction/EndTransaction record
     */
    public void logRecordsForTransactionStatusChanges(LogRecord record, Set<Long> endedTransactions) {
        long transNum = record.getTransNum().get();
        LogType type = record.getType();
        if (type.equals(LogType.COMMIT_TRANSACTION)) {
            // if is CommitTransaction, set status to COMMITTING and update lastLSN
            transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
            transactionTable.get(transNum).lastLSN = record.LSN;
        } else if (type.equals(LogType.ABORT_TRANSACTION)) {
            // if is AbortTransaction, set status to RECOVERY_ABORTING and update lastLSN
            transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            transactionTable.get(transNum).lastLSN = record.LSN;
        } else {
            // if is EndTransaction, the transaction should be cleaned up before setting the status
            // and the entry should be removed from the transaction table.
            // also should add the ended transaction's transaction number into the endedTransactions set
            transactionTable.get(transNum).transaction.cleanup();
            transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
            transactionTable.remove(transNum);
            endedTransactions.add(transNum);
        }
    }

    /**
     * Return True if the status in the checkpoint is more "advanced" than the status in memory.
     * Transactions will always advance through states in one of two ways:
     * running -> committing -> complete
     * running -> aborting -> complete
     */
    public boolean isAdvancedStatus(Transaction.Status chkptStatus, Transaction.Status memoryStatus) {
        switch (memoryStatus) {
            case RUNNING:
                return chkptStatus.equals(Transaction.Status.COMMITTING)
                        || chkptStatus.equals(Transaction.Status.COMPLETE)
                        || chkptStatus.equals(Transaction.Status.ABORTING);
            case COMMITTING:
            case ABORTING:
                return chkptStatus.equals(Transaction.Status.COMPLETE);
            default:
                return false;
        }
    }

    /**
     * The following applies to end checkpoint records.
     * The tables stored in the record should be combined with the tables currently in memory
     */
    public void logRecordsForEndCheckpointRecords(LogRecord record, Set<Long> endedTransactions) {
        // get chkptDPT and chkptTxnTable
        Map<Long, Long> chkptDPT = record.getDirtyPageTable();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = record.getTransactionTable();

        // copy all entries of checkpoint DPT (replace existing entries if any)
        for (Long pageID : chkptDPT.keySet()) {
            long recLSN = chkptDPT.get(pageID);
            dirtyPageTable.put(pageID, recLSN);
        }

        // loop all entries of checkpoint txnTable
        for (Long transNum : chkptTxnTable.keySet()) {
            Transaction.Status chkptStatus = chkptTxnTable.get(transNum).getFirst();
            long lastLSN = chkptTxnTable.get(transNum).getSecond();
            // if this transaction is already in endedTransactions, skip
            if (endedTransactions.contains(transNum)) continue;

            // if this transaction not in txnTable, add it
            if (!transactionTable.containsKey(transNum)) {
                // use newTransaction function object to create a Transaction object
                Transaction transaction = newTransaction.apply(transNum);
                // use startTransaction to add it to txnTable.
                startTransaction(transaction);
            }

            // The lastLSN of a transaction is the max one between two table
            transactionTable.get(transNum).lastLSN = Math.max(lastLSN, transactionTable.get(transNum).lastLSN);

            // The status's in the transaction table should be updated if it is possible
            // to the status in the checkpoint.
            Transaction.Status memoryStatus = transactionTable.get(transNum).transaction.getStatus();
            if (isAdvancedStatus(chkptStatus, memoryStatus)) {
                // Make sure that set to recovery aborting instead of aborting if the checkpoint says aborting
                if (chkptStatus.equals(Transaction.Status.ABORTING)) {
                    transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else{
                    transactionTable.get(transNum).transaction.setStatus(chkptStatus);
                }
            }
        }
    }


    /**
     * After all records in the log are processed, for each ttable entry:
     *      - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *        remove from the ttable, and append an end record
     *      - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *        record
     *      - if RECOVERY_ABORTING: no action needed
     */
    public void finalEndingTransactionsForRestartAnalysis() {
        for (Long transNum : transactionTable.keySet()) {
            Transaction transaction = transactionTable.get(transNum).transaction;
            Transaction.Status status = transaction.getStatus();

            // All transactions in the COMMITTING state should be ended (cleanup(),
            // state set to COMPLETE, end transaction record written,
            // and removed from the transaction table).
            if (status.equals(Transaction.Status.COMMITTING)) {
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                LogRecord endTransactionLogRecord = createRecord(transNum, "end");
                logManager.appendToLog(endTransactionLogRecord);
                transactionTable.remove(transNum);
            }
            // All transactions in the RUNNING state should be moved into the RECOVERY_ABORTING state,
            // and an abort transaction record should be written.
            else if (status.equals(Transaction.Status.RUNNING)) {
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                LogRecord abortTransactionLogRecord = createRecord(transNum, "abort");
                transactionTable.get(transNum).lastLSN = logManager.appendToLog(abortTransactionLogRecord);
            }
            // Nothing needs to be done for transactions in the RECOVERY_ABORTING state.
        }
    }


    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();

        // DONE(proj5): implement
        // begin scanning log records,
        // starting at the beginning of the last successful checkpoint.
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(LSN);
        while (logRecordIterator.hasNext()) {
            LogRecord currRecord = logRecordIterator.next();
            LogType type = currRecord.getType();

            // If the log record is for a transaction operation (getTransNum is present)
            if (currRecord.getTransNum().isPresent()) {
                logRecordsForTransactionOperations(currRecord);
            }

            // If the log record is page-related (getPageNum is present)
            if (currRecord.getPageNum().isPresent()) {
                logRecordsForPageOperations(currRecord);
            }

            // If the log record is for a change in transaction status
            // CommitTransaction/AbortTransaction/EndTransaction
            if (type.equals(LogType.COMMIT_TRANSACTION)
                    || type.equals(LogType.ABORT_TRANSACTION)
                    || type.equals(LogType.END_TRANSACTION)) {
                logRecordsForTransactionStatusChanges(currRecord, endedTransactions);
            }

            // skip BeginCheckpoint record

            // If the log record is an end_checkpoint record:
            if (type.equals(LogType.END_CHECKPOINT)) {
                logRecordsForEndCheckpointRecords(currRecord, endedTransactions);
            }
        }

        // After all records in the log are processed, we need ending the Transactions
        finalEndingTransactionsForRestartAnalysis();
    }


    // some help function for restartRedo /////////////////////////////////////

    /**
     * Return ture if this record is redoable and is either:
     *  - a partition-related record (AllocPart, UndoAllocPart, FreePart, UndoFreePart)
     *  - a record that allocates a page (AllocPage, UndoFreePage)
     *  - a record that modifies a page (UpdatePage, UndoUpdatePage, UndoAllocPage, FreePage)
     *    where all of the following hold:
     *      - the page is in the DPT
     *      - the record's LSN is greater than or equal to the DPT's recLSN for that page.
     *      - the pageLSN on the page itself is strictly less than the LSN of the record.
     */
    public boolean isRedoRecord(LogRecord record) {
        LogType type = record.getType();
        if (record.isRedoable()) {
            // a partition-related record
            if (type.equals(LogType.ALLOC_PART)
                    || type.equals(LogType.UNDO_ALLOC_PART)
                    || type.equals(LogType.FREE_PART)
                    || type.equals(LogType.UNDO_FREE_PART)) {
                return true;
            }
            // a record that allocates a page
            if (type.equals(LogType.ALLOC_PAGE)
                    || type.equals(LogType.UNDO_FREE_PAGE)) {
                return true;
            }
            // a record that modifies a page
            if (type.equals(LogType.UPDATE_PAGE)
                    || type.equals(LogType.UNDO_UPDATE_PAGE)
                    || type.equals(LogType.UNDO_ALLOC_PAGE)
                    || type.equals(LogType.FREE_PAGE)) {
                long pageID = record.getPageNum().get();
                // the page should in DPT
                if (!dirtyPageTable.containsKey(pageID)) return false;
                // the record's LSN is greater than or equal to the DPT's recLSN for that page.
                long recLSN = dirtyPageTable.get(pageID);
                if (record.getLSN() < recLSN) return false;
                // the pageLSN on the page itself is strictly less than the LSN of the record.
                Page page = bufferManager.fetchPage(new DummyLockContext(), pageID);
                long pageLSN;
                try {
                    pageLSN = page.getPageLSN();
                } finally {
                    page.unpin();
                }
                return pageLSN < record.getLSN();
            }
        }
        return false;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // DONE(proj5): implement
        // Start at the smallest recLSN in the DPT
        long startLSN = dirtyPageTable.values().stream().min(Long::compareTo).orElse(0L);
        // Scanning from the starting point
        Iterator<LogRecord> logRecordIterator = logManager.scanFrom(startLSN);
        while (logRecordIterator.hasNext()) {
            LogRecord currRecord = logRecordIterator.next();
            // redo a record if it needs to
            if (isRedoRecord(currRecord)) {
                currRecord.redo(this, diskSpaceManager, bufferManager);
            }
        }
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   or the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // DONE(proj5): implement
        // Create a maxHeap to store the lastLSN for all the transactions in the RECOVERY_ABORTING state
        PriorityQueue<Long> maxLSNHeap = new PriorityQueue<>((a,b) -> b.compareTo(a));
        for (Long transNum : transactionTable.keySet()) {
            TransactionTableEntry te =transactionTable.get(transNum);
            if (te.transaction.getStatus().equals(Transaction.Status.RECOVERY_ABORTING)) {
                maxLSNHeap.add(te.lastLSN);
            }
        }

        while (!maxLSNHeap.isEmpty()) {
            long LSN = maxLSNHeap.poll();
            LogRecord currRecord = logManager.fetchLogRecord(LSN);
            long transNum = currRecord.getTransNum().get();
            Transaction transaction = transactionTable.get(transNum).transaction;
            // if the record is undoable
            if (currRecord.isUndoable()) {
                // we write the CLR, update the lastLSN and undo it
                long lastLSN = transactionTable.get(transNum).lastLSN;
                LogRecord CLR = currRecord.undo(lastLSN);
                transactionTable.get(transNum).lastLSN = logManager.appendToLog(CLR);
                // Call redo on the CLR to perform the undo
                CLR.redo(this, diskSpaceManager, bufferManager);
            }

            // new LSN to the maxHeap with the undoNextLSN of the record if it has one, or the prevLSN otherwise
            long newLSN = currRecord.getUndoNextLSN().orElse(currRecord.getPrevLSN().orElse(0L));

            // if the new LSN is 0, clean up the transaction, set the status to complete,
            // write the END record for that transaction.
            // and remove from transaction table.
            if (newLSN == 0L) {
                transaction.cleanup();
                transaction.setStatus(Transaction.Status.COMPLETE);
                LogRecord endTransactionLogRecord = createRecord(transNum, "end");
                logManager.appendToLog(endTransactionLogRecord);
                transactionTable.remove(transNum);
            }
            // else just add this newLSN
            else {
                maxLSNHeap.offer(newLSN);
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
