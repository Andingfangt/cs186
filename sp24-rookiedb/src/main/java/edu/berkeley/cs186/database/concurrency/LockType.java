package edu.berkeley.cs186.database.concurrency;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement
        // based on the Multi-granularity Locking Table
        if (a == NL || b == NL) return true;

        switch (a) {
            case IS: return b != X;
            case IX: return b == IS || b == IX;
            case S: return b == IS || b == S;
            case SIX: return b == IS;
            case X: return false;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement
        // based on note11 5.1 Multiple Granularity Locking Protocol
        // and in this project SIX(A) can do anything that having S(A) or IX(A) lets it do,
        // except requesting S, IS, or SIX locks on children of A, which would be redundant(R).
        switch (childLockType) {
            case NL: return true;
            case IS:
            case S:
                return parentLockType == IS || parentLockType == IX;
            case IX:
            case X:
                return parentLockType == IX || parentLockType == SIX;
            case SIX:
                return parentLockType == IX;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement
        if (required == NL) return true;
        switch (substitute) {
            case NL: return false;
            case IS: return required == IS;
            case IX: return required == IS || required == IX;
            case S: return required == IS || required == S;
            case SIX: return required != X;
            case X: return true;
            default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }


    /**
     * Return all LockTypes from low to high
     */
    public static List<LockType> allLockTypes() {
        List<LockType> lst = new ArrayList<>();
        lst.add(LockType.NL);
        lst.add(LockType.S);
        lst.add(LockType.IS);
        lst.add(LockType.X);
        lst.add(LockType.IX);
        lst.add(LockType.SIX);
        return lst;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

