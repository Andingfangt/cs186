package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // DONE(proj4_part1): implement
            for (Lock lock : locks) {
                // Allows conflicts for locks held by transaction with id `except`, which is
                // useful when a transaction tries to replace a lock it already has on
                // the resource.
                if (lock.transactionNum.equals(except)) continue;
                if (!LockType.compatible(lock.lockType, lockType)) return false;
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // DONE(proj4_part1): implement
            long transactionID = lock.transactionNum;
            // case 1: Updates lock on resource if the transaction already has a lock.
            for (Lock currLock : locks) {
                if (currLock.transactionNum.equals(transactionID)) {
                    // update lock in ResourceLocksList
                    // this also update lock in TransactionLocksList
                    // since lock is reference.
                    currLock.lockType = lock.lockType;
                    return;
                    /*
                    ResourceName name = currLock.name;
                    //
                    for (Lock tLock : transactionLocks.get(transactionID)) {
                        if (tLock.name.equals(name)) {
                            tLock.lockType = lock.lockType;
                        }
                        return;
                     }
                     */
                }
            }

            // case 2: Gives the transaction the lock `lock`.
            // add to ResourcesLockList
            locks.add(lock);
            // add to TransactionLocksList
            List<Lock> preTLocksList = transactionLocks.getOrDefault(transactionID, new ArrayList<>());
            preTLocksList.add(lock);
            transactionLocks.put(transactionID, preTLocksList);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // DONE(proj4_part1): implement
            long transactionID = lock.transactionNum;
            // remove this lock in ResourceLockList
            locks.remove(lock);
            // remove this lock in transactionLocksList
            transactionLocks.get(transactionID).remove(lock);
            // processes the queue.
            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // DONE(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // DONE(proj4_part1): implement
            while (requests.hasNext()) {
                LockRequest currRequest = requests.next();
                TransactionContext transaction = currRequest.transaction;
                long transactionID = transaction.getTransNum();
                Lock lock = currRequest.lock;
                List<Lock> releaseLocks = currRequest.releasedLocks;

                // stopping when the next lock cannot be granted,
                // which means its 'lockType' is not compatible.
                if (!checkCompatible(lock.lockType, transactionID)) break;

                // remove this request in queue
                waitingQueue.remove(currRequest);

                // grant this new lock
                grantOrUpdateLock(lock);

                // release all the locks in releaseList
                for (Lock releaseLock : releaseLocks) {
                    releaseLock(releaseLock);
                }

                // Once a request is completely granted,
                // the transaction that made the request can be unblocked.
                transaction.unblock();
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // DONE(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum.equals(transaction)) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        /**
         * Return ture if the waitingQueue is empty.
         */
        boolean isWaitingQueueEmpty() {
            return waitingQueue.isEmpty();
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }


    // Some help method for the blow ////////////////////////////////////////////

    /**
     * if a lock on `name` is already held by `transaction` and isn't being released
     * throws DuplicateLockRequestException
     */
    void checkDuplicateLockRequestForAcquireAndRelease(LockType currLockType, ResourceName name,
                                                       List<ResourceName> releaseNames, TransactionContext transaction) {
        if (!currLockType.equals(LockType.NL) && !releaseNames.contains(name)) {
            throw new DuplicateLockRequestException(String.format(
                    "Transaction %s already hold a lock %s, and will not be released this time.",
                    transaction.getTransNum(), currLockType
            ));
        }
    }

    /**
     * if a lock on `name` is already held by `transaction`
     * throws DuplicateLockRequestException
     */
    void checkDuplicateLockRequestForAcquire(LockType currLockType, TransactionContext transaction) {
        if (!currLockType.equals(LockType.NL)) {
            throw new DuplicateLockRequestException(String.format(
                    "Transaction %s already hold a lock %s",
                    transaction.getTransNum(), currLockType
            ));
        }
    }

    /**
     * if `transaction` already has a newLockType` lock on `name`
     * throws DuplicateLockRequestException
     */
    void checkDuplicateLockRequestForPromote(LockType currLockType, LockType newLockType,
                                             TransactionContext transaction, ResourceName name) {
        if (currLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException(String.format(
                        "Transaction %s already has a %s lock on %s",
                        transaction.getTransNum(), newLockType, name
                ));
            }
    }

    /**
     * if `transaction` doesn't hold a lock on one or more of the names in `releaseNames',
     * throws NoLockHeldException
     */
    void checkNoLockHeldExceptionForAcquireAndRelease(TransactionContext transaction, List<ResourceName> releaseNames) {
        for (ResourceName releaseName : releaseNames) {
            if (getLockType(transaction, releaseName).equals(LockType.NL)) {
                throw new NoLockHeldException(String.format(
                        "Transaction %s don't holds a lock on %s, so we can't release",
                        transaction.getTransNum(), releaseName
                ));
            }
        }
    }

    /**
     * if `transaction` doesn't hold a lock on `releaseName',
     * throws NoLockHeldException
     */
    void checkNoLockHeldExceptionForRelease(TransactionContext transaction, ResourceName releaseName) {
        checkNoLockHeldExceptionForAcquireAndRelease(transaction, Collections.singletonList(releaseName));
    }

    /**
     * if `transaction` has no lock on `name`
     * throws NoLockHeldException
     */
    void checkNoLockHeldExceptionForPromote(TransactionContext transaction, LockType currLockType, ResourceName name) {
        if (currLockType.equals(LockType.NL)) {
                throw new NoLockHeldException(String.format(
                        "Transaction %s has no lock on %s",
                        transaction.getTransNum(), name
                ));
            }
    }

    void checkInvalidLockExceptionForPromote(LockType currLockType, LockType newLockType) {
        if (!LockType.substitutable(newLockType, currLockType)) {
            throw new InvalidLockException(String.format(
                    "we can't promote %s to %s, since they are not substitutable.",
                    currLockType, newLockType
            ));
        }
    }


    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // DONE(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        boolean acquired = false;
        synchronized (this) {
            // **Error checking must be done before any locks are acquired.
            // case 1: if a lock on `name` is already held by `transaction` and isn't being released
            // throws DuplicateLockRequestException
            ResourceEntry resourceEntry = getResourceEntry(name);
            long transactionID = transaction.getTransNum();
            LockType currLockType = getLockType(transaction, name);
            checkDuplicateLockRequestForAcquireAndRelease(currLockType, name, releaseNames, transaction);

            // case 2: if `transaction` doesn't hold a lock on one
            // or more of the names in `releaseNames,
            // throws NoLockHeldException
            checkNoLockHeldExceptionForAcquireAndRelease(transaction, releaseNames);

            // **Acquire a `lockType` lock on `name`, for transaction `transaction`.
            // case 1: If the new lock is not compatible with another transaction's lock on the
            // resource, the transaction is blocked and the request is placed at the
            // FRONT of the resource's queue.
            Lock lock = new Lock(name, lockType, transactionID);
            List<Lock> releasedLocks = new ArrayList<>();
            // get releasedLocks: for all the lock current transaction holds,
            // if this lock is on the resource that in releaseList, add it.
            for (Lock holdLock : getLocks(transaction)) {
                if (releaseNames.contains(holdLock.name)) {
                    // skip this Resource, since we use update for this case.
                    if (holdLock.name.equals(name)) continue;
                    releasedLocks.add(holdLock);
                }
            }
            LockRequest lockRequest = new LockRequest(transaction, lock, releasedLocks);
            if (!resourceEntry.checkCompatible(lockType, transactionID)) {
                transaction.prepareBlock();
                shouldBlock = true;
                resourceEntry.addToQueue(lockRequest, true);
            } else {
                // case 2: if it is compatible, we simply grant this lock.
                // the grantOrUpdateLock method also handled the case
                // add one and release the old one to this Resource by update.
                resourceEntry.grantOrUpdateLock(lock);
                acquired = true;
            }

            // **Locks on `releaseNames` should be released only after the requested lock
            // has been acquired. The corresponding queues should be processed.
            if (acquired) {
                for (Lock releasedLock : releasedLocks) {
                    ResourceName rName = releasedLock.name;
                    ResourceEntry rEntry = getResourceEntry(rName);
                    rEntry.releaseLock(releasedLock);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // DONE(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            // **Error checking must be done before any locks are acquired.
            // if a lock on `name` is already held by `transaction`
            // throws DuplicateLockRequestException
            ResourceEntry resourceEntry = getResourceEntry(name);
            long transactionID = transaction.getTransNum();
            LockType currLockType = getLockType(transaction, name);
            checkDuplicateLockRequestForAcquire(currLockType, transaction);

            // **Acquire a `lockType` lock on `name`, for transaction `transaction`.
            // case 1: If the new lock is not compatible with another transaction's lock
            // on the resource, or if there are other transaction in queue for the resource,
            // the transaction is blocked and the request is placed at the
            // BACK of the resource's queue.
            Lock lock = new Lock(name, lockType, transactionID);
            LockRequest lockRequest = new LockRequest(transaction, lock);
            if (!resourceEntry.checkCompatible(lockType, transactionID) || !resourceEntry.isWaitingQueueEmpty()) {
                transaction.prepareBlock();
                shouldBlock = true;
                resourceEntry.addToQueue(lockRequest, false);
            } else {
                // case 2: if it is compatible, we simply grant this lock.
                resourceEntry.grantOrUpdateLock(lock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // DONE(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            // **Error checking must be done before the lock is released.
            // if `transaction` doesn't hold a lock on this resource,
            // throws NoLockHeldException
            checkNoLockHeldExceptionForRelease(transaction, name);

            // **Lock on `name` should be released.
            // The corresponding queues should be processed.
            ResourceEntry rEntry = getResourceEntry(name);
            for (Lock lock : getLocks(transaction)) {
                if (lock.name.equals(name)) {
                    rEntry.releaseLock(lock);
                }
            }
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // DONE(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            // **Error checking must be done before any locks are acquired.
            // case 1: if `transaction` already has a newLockType` lock on `name`
            // throws DuplicateLockRequestException
            ResourceEntry resourceEntry = getResourceEntry(name);
            long transactionID = transaction.getTransNum();
            LockType currLockType = getLockType(transaction, name);
            checkDuplicateLockRequestForPromote(currLockType, newLockType, transaction, name);

            // case 2: if `transaction` has no lock on `name`
            // throws NoLockHeldException
            checkNoLockHeldExceptionForPromote(transaction, currLockType, name);

            // case 3: if the requested lock type is not a promotion.
            // A promotion from lock type A to lock type B is valid if and
            // only if B is substitutable for A, and B is not equal to A.
            checkInvalidLockExceptionForPromote(currLockType, newLockType);


            // **Promote a transaction's lock on `name` to `newLockType`
            // case 1:If the new lock is not compatible with another transaction's lock on the
            // resource, the transaction is blocked and the request is placed at the
            // FRONT of the resource's queue.
            Lock lock = new Lock(name, newLockType, transactionID);
            LockRequest lockRequest = new LockRequest(transaction, lock);
            if (!resourceEntry.checkCompatible(newLockType, transactionID)) {
                transaction.prepareBlock();
                shouldBlock = true;
                resourceEntry.addToQueue(lockRequest, true);
            } else {
                // case 2: if it is compatible, we simply change
                // the transaction's lock on `name` from the current lock type to
                // `newLockType`.
                resourceEntry.grantOrUpdateLock(lock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // DONE(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        long transactionID = transaction.getTransNum();
        return resourceEntry.getTransactionLockType(transactionID);
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
