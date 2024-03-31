package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * Every time we need to promote or acquire a lock, we need to ensure appropriate locks
     * on all ancestors
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // DONE(proj4_part2): implement
        // case 1: The current lock type can effectively substitute the requested type
        // do nothing, since if request == NL, it will return this time, after no need to consider NL
        if (LockType.substitutable(explicitLockType, requestType)) return;
        // case 2: The current lock type is IX and the requested lock is S
        // promote to SIX and update Ancestors if needed
        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
            ensureAppropriateLocksOnAllAncestors(parentContext, transaction, LockType.SIX);
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // case 3: The current lock type is an intent lock
        // escalate to S or X
        if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);
            explicitLockType = lockContext.getExplicitLockType(transaction);
            // if The current lock type can effectively substitute the requested type, return
            if (LockType.substitutable(explicitLockType, requestType)) return;
            // else promote to requestType and update Ancestors if needed
            ensureAppropriateLocksOnAllAncestors(parentContext, transaction, requestType);
            lockContext.promote(transaction, requestType);
            return;
        }
        // case 4: this time explicitType can only be S, X, NL
        // since X will always be substitutable, we only need consider S and NL
        // case 4.a: explicitType = S
        if (explicitLockType.equals(LockType.S)) {
            // only need to promote if request is X
            if (requestType.equals(LockType.X)) {
                ensureAppropriateLocksOnAllAncestors(parentContext, transaction, LockType.X);
                lockContext.promote(transaction, LockType.X);
            }
            return;
        }
        // case 4.b: explicitType = NL, check effectiveType
        // if substitutable, return
        if (LockType.substitutable(effectiveLockType, requestType)) return;
        // else acquire and ensure all Ancestors
        ensureAppropriateLocksOnAllAncestors(parentContext, transaction, requestType);
        lockContext.acquire(transaction, requestType);
        return;
    }

    // DONE(proj4_part2) add any helper methods you want

    public static void ensureAppropriateLocksOnAllAncestors(LockContext lockContext, TransactionContext transaction,LockType newLockTypeOnChild) {
        // base case
        if (lockContext == null) return;

        LockContext parentContext = lockContext.parentContext();
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // if lockType on this context canBeParent to its child, return
        if (LockType.canBeParentLock(explicitLockType, newLockTypeOnChild)) return;
        // else, we need acquire or promote
        LockType neededCurrLockType = null;

        // loop all lockType to find the lowest authority suitable lockType
        List<LockType> allLocks = LockType.allLockTypes();
        for (LockType locktype : allLocks) {
            if (LockType.canBeParentLock(locktype, newLockTypeOnChild) && LockType.substitutable(locktype, explicitLockType)) {
                neededCurrLockType = locktype;
                break;
            }
        }
        ensureAppropriateLocksOnAllAncestors(parentContext, transaction, neededCurrLockType);
        if (explicitLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, neededCurrLockType);
        } else {
            lockContext.promote(transaction, neededCurrLockType);
        }
    }
}
