/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.txn;

import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.utilint.StatGroup;

/**
 * SyncedLockManager uses the synchronized keyword to implement its critical
 * sections.
 */
public class SyncedLockManager extends LockManager {

    public SyncedLockManager(EnvironmentImpl envImpl) {
        super(envImpl);
    }

    public Set<LockInfo> getOwners(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return getOwnersInternal(lsn, lockTableIndex);
        }
    }

    public LockType getOwnedLockType(Long lsn, Locker locker) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return getOwnedLockTypeInternal(lsn, locker, lockTableIndex);
        }
    }
    
    public boolean isLockUncontended(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return isLockUncontendedInternal(lsn, lockTableIndex);
        }
    }

    /**
     * @see LockManager#attemptLock
     */
    @Override
    Lock lookupLock(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return lookupLockInternal(lsn, lockTableIndex);
        }
    }

    /**
     * @see LockManager#attemptLock
     */
    @Override
    LockAttemptResult attemptLock(Long lsn,
                                  Locker locker,
                                  LockType type,
                                  boolean nonBlockingRequest,
                                  boolean jumpAheadOfWaiters)
        throws DatabaseException {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return attemptLockInternal
                (lsn, locker, type, nonBlockingRequest, jumpAheadOfWaiters,
                 lockTableIndex);
        }
    }

    /**
     * @see LockManager#makeTimeoutMsg
     */
    @Override
    LockConflictException makeTimeoutMsg(boolean isLockNotTxnTimeout,
                                         Locker locker,
                                         long lsn,
                                         LockType type,
                                         LockGrantType grantType,
                                         Lock useLock,
                                         long timeout,
                                         long start,
                                         long now,
                                         DatabaseImpl database) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return makeTimeoutMsgInternal
                (isLockNotTxnTimeout, locker, lsn, type, grantType, useLock,
                 timeout, start, now, database);
        }
    }

    /**
     * @see LockManager#releaseAndNotifyTargets
     */
    @Override
    Set<Locker> releaseAndFindNotifyTargets(long lsn, Locker locker) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return releaseAndFindNotifyTargetsInternal
                (lsn, locker, lockTableIndex);
        }
    }

    /**
     * @see LockManager#demote
     */
    @Override
    void demote(long lsn, Locker locker) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            demoteInternal(lsn, locker, lockTableIndex);
        }
    }

    /**
     * @see LockManager#isLocked
     */
    @Override
    boolean isLocked(Long lsn) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return isLockedInternal(lsn, lockTableIndex);
        }
    }

    /**
     * @see LockManager#isOwner
     */
    @Override
    boolean isOwner(Long lsn, Locker locker, LockType type) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return isOwnerInternal(lsn, locker, type, lockTableIndex);
        }
    }

    /**
     * @see LockManager#isWaiter
     */
    @Override
    boolean isWaiter(Long lsn, Locker locker) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return isWaiterInternal(lsn, locker, lockTableIndex);
        }
    }

    /**
     * @see LockManager#nWaiters
     */
    @Override
    int nWaiters(Long lsn) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return nWaitersInternal(lsn, lockTableIndex);
        }
    }

    /**
     * @see LockManager#nOwners
     */
    @Override
    int nOwners(Long lsn) {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return nOwnersInternal(lsn, lockTableIndex);
        }
    }

    /**
     * @see LockManager#getWriterOwnerLocker
     */
    @Override
    Locker getWriteOwnerLocker(Long lsn) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return getWriteOwnerLockerInternal(lsn, lockTableIndex);
        }
    }

    /**
     * @see LockManager#validateOwnership
     */
    @Override
    boolean validateOwnership(Long lsn,
                              Locker locker,
                              LockType type,
                              boolean flushFromWaiters,
                              MemoryBudget mb) {
        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return validateOwnershipInternal
                (lsn, locker, type, flushFromWaiters, mb, lockTableIndex);
        }
    }

    /**
     * @see LockManager#stealLock
     */
    @Override
    protected LockAttemptResult stealLock(Long lsn,
                                          Locker locker,
                                          LockType lockType,
                                          MemoryBudget mb)
        throws DatabaseException {

        int lockTableIndex = getLockTableIndex(lsn);
        synchronized(lockTableLatches[lockTableIndex]) {
            return stealLockInternal(lsn, locker, lockType,
                                     mb, lockTableIndex);
        }
    }

    /**
     * @see LockManager#dumpLockTable
     */
    @Override
    void dumpLockTable(StatGroup stats, boolean clear) {
        for (int i = 0; i < nLockTables; i++) {
            synchronized(lockTableLatches[i]) {
                dumpLockTableInternal(stats, i, clear);
            }
        }
    }
}
