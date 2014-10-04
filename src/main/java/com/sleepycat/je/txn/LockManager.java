/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.txn;

import static com.sleepycat.je.txn.LockStatDefinition.GROUP_DESC;
import static com.sleepycat.je.txn.LockStatDefinition.GROUP_NAME;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_OWNERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_READ_LOCKS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_REQUESTS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_TOTAL;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITERS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WAITS;
import static com.sleepycat.je.txn.LockStatDefinition.LOCK_WRITE_LOCKS;

import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DeadlockException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.LockNotGrantedException;
import com.sleepycat.je.LockStats;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.ThreadInterruptedException;
import com.sleepycat.je.TransactionTimeoutException;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvConfigObserver;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.dbi.RangeRestartException;
import com.sleepycat.je.latch.Latch;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TinyHashSet;

/**
 * LockManager manages locks.
 *
 * Note that locks are counted as taking up part of the JE cache;
 */
public abstract class LockManager implements EnvConfigObserver {

    /*
     * The total memory cost for a lock is the Lock object, plus its entry and
     * key in the lock hash table.
     *
     * The addition and removal of Lock objects, and the corresponding cost of
     * their hashmap entry and key are tracked through the LockManager.
     */
    static final long TOTAL_LOCKIMPL_OVERHEAD =
        MemoryBudget.LOCKIMPL_OVERHEAD +
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.LONG_OVERHEAD;

    static final long TOTAL_THINLOCKIMPL_OVERHEAD =
        MemoryBudget.THINLOCKIMPL_OVERHEAD +
        MemoryBudget.HASHMAP_ENTRY_OVERHEAD +
        MemoryBudget.LONG_OVERHEAD;

    private static final long REMOVE_TOTAL_LOCKIMPL_OVERHEAD =
        0 - TOTAL_LOCKIMPL_OVERHEAD;

    private static final long REMOVE_TOTAL_THINLOCKIMPL_OVERHEAD =
        0 - TOTAL_THINLOCKIMPL_OVERHEAD;

    private static final long THINLOCK_MUTATE_OVERHEAD =
        MemoryBudget.LOCKIMPL_OVERHEAD -
        MemoryBudget.THINLOCKIMPL_OVERHEAD +
        MemoryBudget.LOCKINFO_OVERHEAD;
    
    private static final List<ThreadLocker> EMPTY_THREAD_LOCKERS =
        Collections.emptyList();

    int nLockTables = 1;
    Latch[] lockTableLatches;
    private final Map<Long,Lock>[] lockTables;          // keyed by LSN
    private final boolean oldLockExceptions;
    private final EnvironmentImpl envImpl;
    private final MemoryBudget memoryBudget;

    private final StatGroup stats;
    private final LongStat nRequests; /* number of time a request was made. */
    private final LongStat nWaits;    /* number of time a request blocked. */

    private static RangeRestartException rangeRestartException =
        new RangeRestartException();
    private static boolean lockTableDump = false;

    /**
     * Maps a thread to a set of ThreadLockers.  Currently this map is only
     * maintained (non-null) in a replicated environment because it is only
     * needed for determining when to throw LockPreemptedException.
     *
     * Access to the map need not be synchronized because it is a
     * ConcurrentHashMap.  Access to the TinyHashSet stored for each thread
     * need not be synchronized, since it is only accessed by a single thread.
     *
     * A TinyHashSet is used because typically only a single ThreadLocker per
     * thread will be open at one time.
     *
     * @see ThreadLocker#checkPreempted
     * [#16513]
     */
    private final Map<Thread, TinyHashSet<ThreadLocker>> threadLockers;

    /*
     * @SuppressWarnings is used to stifle a type safety complaint about the
     * assignment of lockTables = new Map[nLockTables]. There's no way to
     * specify the type of the array.
     */
    @SuppressWarnings("unchecked")
    public LockManager(EnvironmentImpl envImpl) {

        DbConfigManager configMgr = envImpl.getConfigManager();
        nLockTables = configMgr.getInt(EnvironmentParams.N_LOCK_TABLES);
        oldLockExceptions =
            configMgr.getBoolean(EnvironmentParams.LOCK_OLD_LOCK_EXCEPTIONS);
        lockTables = new Map[nLockTables];
        lockTableLatches = new Latch[nLockTables];
        for (int i = 0; i < nLockTables; i++) {
            lockTables[i] = new HashMap<Long,Lock>();
            lockTableLatches[i] = new Latch("Lock Table " + i);
        }
        this.envImpl = envImpl;
        memoryBudget = envImpl.getMemoryBudget();

        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        nRequests = new LongStat(stats, LOCK_REQUESTS);
        nWaits = new LongStat(stats, LOCK_WAITS);

        /* Initialize mutable properties and register for notifications. */
        envConfigUpdate(configMgr, null);
        envImpl.addConfigObserver(this);

        if (envImpl.isReplicated()) {
            threadLockers =
                new ConcurrentHashMap<Thread, TinyHashSet<ThreadLocker>>();
        } else {
            threadLockers = null;
        }
    }

    /**
     * Process notifications of mutable property changes.
     */
    public void envConfigUpdate(DbConfigManager configMgr,
                                EnvironmentMutableConfig ignore) {
        LockInfo.setDeadlockStackTrace(configMgr.getBoolean
            (EnvironmentParams.TXN_DEADLOCK_STACK_TRACE));
        setLockTableDump(configMgr.getBoolean
            (EnvironmentParams.TXN_DUMPLOCKS));
    }

    /**
     * Called when the je.txn.dumpLocks property is changed.
     */
    static void setLockTableDump(boolean enable) {
        lockTableDump = enable;
    }

    int getLockTableIndex(Long lsn) {
        return (((int) lsn.longValue()) & 0x7fffffff) %
            nLockTables;
    }

    int getLockTableIndex(long lsn) {
        return (((int) lsn) & 0x7fffffff) % nLockTables;
    }

    /**
     * Attempt to acquire a lock of <i>type</i> on <i>lsn</i>.  If the lock
     * acquisition would result in a deadlock, throw an exception.<br> If the
     * requested lock is not currently available, block until it is or until
     * timeout milliseconds have elapsed.<br> If a lock of <i>type</i> is
     * already held, return EXISTING.<br> If a WRITE lock is held and a READ
     * lock is requested, return PROMOTION.<br>
     *
     * If a lock request is for a lock that is not currently held, return
     * either NEW or DENIED depending on whether the lock is granted or
     * not.<br>
     *
     * @param lsn The LSN to lock.
     *
     * @param locker The Locker to lock this on behalf of.
     *
     * @param type The lock type requested.
     *
     * @param timeout milliseconds to time out after if lock couldn't be
     * obtained.  0 means block indefinitely.  Not used if nonBlockingRequest
     * is true.
     *
     * @param nonBlockingRequest if true, means don't block if lock can't be
     * acquired, and ignore the timeout parameter.
     *
     * @param jumpAheadOfWaiters grant the lock before other waiters, if any.
     *
     * @return a LockGrantType indicating whether the request was fulfilled or
     * not.  LockGrantType.NEW means the lock grant was fulfilled and the
     * caller did not previously hold the lock.  PROMOTION means the lock was
     * granted and it was a promotion from READ to WRITE.  EXISTING means the
     * lock was already granted (not a promotion).  DENIED means the lock was
     * not granted because the timeout passed without acquiring the lock or
     * timeout was 0 and the lock was not immediately available.
     *
     * @throws LockConflictException if lock could not be acquired.
     *
     * @throws IllegalArgumentException via db/cursor read/write methods, if
     * non-transactional access to a replicated environment is attempted, and
     * read-uncommitted is not specified.
     */
    public LockGrantType lock(long lsn,
                              Locker locker,
                              LockType type,
                              long timeout,
                              boolean nonBlockingRequest,
                              boolean jumpAheadOfWaiters,
                              DatabaseImpl database)
        throws LockConflictException, DatabaseException {

        assert timeout >= 0;

        /* No lock needed for dirty-read, return as soon as possible. */
        if (type == LockType.NONE) {
            return LockGrantType.NONE_NEEDED;
        }

        /*
         * Assert that a replication-defined locker is used for locks on
         * replicated databases.  Two cases are exempt from this rule:
         * - Only NameLNs that identify replicated DBs are replicated, not
         *   all NameLNs in the naming DB, so the naming DB is exempt.
         * - Non-preemption is permissible for selected internal operations
         *   because we can ensure that they are not long running and will not
         *   hold locks interminably.  A BasicLocker is often used internally
         *   in such cases.
         */
        if (envImpl.isReplicated() &&
            database != null &&
            database.isReplicated() &&
            !database.getId().equals(DbTree.NAME_DB_ID) &&
            (locker.getPreemptable() || type.isWriteLock()) &&
            !locker.isReplicationDefined()) {
            throw EnvironmentFailureException.unexpectedState
                ("Locker: " + locker.getClass().getName());
        }

        /*
         * Lock on locker before latching the lockTable to avoid having another
         * notifier perform the notify before the waiter is actually waiting.
         */
        synchronized (locker) {
            LockGrantType ret = null;
            ret = lockInternal(lsn, locker, type, timeout, nonBlockingRequest,
                               jumpAheadOfWaiters, database);
            return ret;
        }
    }

    private LockGrantType lockInternal(long lsn,
                                       Locker locker,
                                       LockType type,
                                       long timeout,
                                       boolean nonBlockingRequest,
                                       boolean jumpAheadOfWaiters,
                                       DatabaseImpl database)
        throws DeadlockException, DatabaseException {

        Long nid = Long.valueOf(lsn);
        LockAttemptResult result = attemptLock
            (nid, locker, type, nonBlockingRequest, jumpAheadOfWaiters);
        /* If we got the lock or a non-blocking lock was denied, return. */
        if (result.success ||
            result.lockGrant == LockGrantType.DENIED) {
            assert nonBlockingRequest || result.success;
            return result.lockGrant;
        }

        assert checkNoLatchesHeld(nonBlockingRequest):
            LatchSupport.countLatchesHeld() +
            " latches held while trying to lock, lock table =" +
            LatchSupport.latchesHeldToString();

        /*
         * We must have gotten WAIT_* from the lock request. We know that
         * this is a blocking request, because if it wasn't, Lock.lock
         * would have returned DENIED. Go wait!
         */
        assert !nonBlockingRequest;
        try {
            boolean doWait = true;
            boolean isImportunate = locker.getImportunate();

            /*
             * Before blocking, check locker/txn timeout. We need to check here
             * or lock timeouts will always take precedence and we'll never
             * actually get any txn timeouts.
             */
            if (locker.isTimedOut()) {
                if (validateOwnership(nid, locker, type,
                                      !isImportunate,
                                      memoryBudget)) {
                    doWait = false;
                } else if (isImportunate) {
                    result = stealLock(nid, locker, type, memoryBudget);
                    if (result.success) {
                        doWait = false;
                    } else {
                        /* Lock holder is non-preemptable, wait below. */
                    }
                } else {
                    throw makeTimeoutMsg(false /*isLockNotTxnTimeout*/,
                                         locker, lsn, type,
                                         result.lockGrant,
                                         result.useLock,
                                         locker.getTxnTimeout(),
                                         locker.getTxnStartMillis(),
                                         System.currentTimeMillis(),
                                         database);
                }
            }

            boolean keepTime = (timeout > 0);
            long startTime = (keepTime ? System.currentTimeMillis() : 0);
            while (doWait) {
                locker.setWaitingFor(result.useLock);

                try {
                    locker.wait(timeout);
                } catch (InterruptedException IE) {
                    throw new ThreadInterruptedException(envImpl, IE);
                }

                boolean lockerTimedOut = locker.isTimedOut();
                long now = System.currentTimeMillis();
                boolean thisLockTimedOut =
                    (keepTime && (now - startTime >= timeout));
                boolean isRestart =
                    (result.lockGrant == LockGrantType.WAIT_RESTART);

                /*
                 * Re-check for ownership of the lock following wait.  If
                 * we timed out and we don't have ownership then flush this
                 * lock from both the waiters and owners while under the
                 * lock table latch.  See SR 10103.
                 */
                if (validateOwnership(nid, locker, type,
                                      (lockerTimedOut ||
                                      thisLockTimedOut ||
                                      isRestart) &&
                                      !isImportunate,
                                      memoryBudget)) {
                    break;
                } else if (isImportunate) {
                    result = stealLock(nid, locker, type, memoryBudget);
                    if (result.success) {
                        break;
                    } else {
                        /* Lock holder is non-preemptable, wait again. */
                    }
                } else {
                    /* After a restart conflict the lock will not be held. */
                    if (isRestart) {
                        throw rangeRestartException;
                    }

                    if (thisLockTimedOut) {
                        throw makeTimeoutMsg
                            (true /*isLockNotTxnTimeout*/, locker, lsn,
                             type, result.lockGrant, result.useLock,
                             timeout, startTime, now, database);
                    }

                    if (lockerTimedOut) {
                        throw makeTimeoutMsg
                            (false /*isLockNotTxnTimeout*/, locker, lsn,
                             type, result.lockGrant, result.useLock,
                             locker.getTxnTimeout(),
                             locker.getTxnStartMillis(), now, database);
                    }
                }
            }
        } finally {
            locker.setWaitingFor(null);
            assert EnvironmentImpl.maybeForceYield();
        }

        /*
         * After waiting for the lock, we must break out of the wait loop and
         * add the lock to the locker.  This is true even for importunate
         * lockers, since an existing lock (acquired via a release) will not be
         * added to the locker by attemptLock. [#16879]
         */
        locker.addLock(nid, type, result.lockGrant);

        return result.lockGrant;
    }

    /**
     * Returns the Lockers that own a lock on the given LSN.  Note that when
     * this method returns, there is nothing to prevent these lockers from
     * releasing the lock or being closed.
     */
    public abstract Set<LockInfo> getOwners(Long lsn);

    Set<LockInfo> getOwnersInternal(Long lsn, int lockTableIndex) {
        /* Get the target lock. */
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return null;
        }
        return useLock.getOwnersClone();
    }

    /**
     * Returns the LockType if the given locker owns a lock on the given node,
     * or null if the lock is not owned.
     */
    public abstract LockType getOwnedLockType(Long lsn, Locker locker);

    LockType getOwnedLockTypeInternal(Long lsn,
                                      Locker locker,
                                      int lockTableIndex) {
        /* Get the target lock. */
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return null;
        }
        return useLock.getOwnedLockType(locker);
    }

    public abstract boolean isLockUncontended(Long lsn);

    boolean isLockUncontendedInternal(Long lsn, int lockTableIndex) {
        /* Get the target lock. */
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            return true;
        }
        return useLock.nWaiters() == 0 &&
               useLock.nOwners() == 0;
    }

    abstract Lock lookupLock(Long lsn)
        throws DatabaseException;

    Lock lookupLockInternal(Long lsn, int lockTableIndex) {
        /* Get the target lock. */
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        return useLock;
    }

    abstract LockAttemptResult attemptLock(Long lsn,
                                           Locker locker,
                                           LockType type,
                                           boolean nonBlockingRequest,
                                           boolean jumpAheadOfWaiters)
        throws DatabaseException;

    LockAttemptResult attemptLockInternal(Long lsn,
                                          Locker locker,
                                          LockType type,
                                          boolean nonBlockingRequest,
                                          boolean jumpAheadOfWaiters,
                                          int lockTableIndex)
        throws DatabaseException {

        nRequests.increment();

        /* Get the target lock. */
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            useLock = new ThinLockImpl();
            lockTable.put(lsn, useLock);
            memoryBudget.updateLockMemoryUsage
                (TOTAL_THINLOCKIMPL_OVERHEAD, lockTableIndex);
        }

        /*
         * Attempt to lock.  Possible return values are NEW, PROMOTION, DENIED,
         * EXISTING, WAIT_NEW, WAIT_PROMOTION, WAIT_RESTART.
         */
        LockAttemptResult lar = useLock.lock
            (type, locker, nonBlockingRequest, jumpAheadOfWaiters,
             memoryBudget, lockTableIndex);
        if (lar.useLock != useLock) {
            /* The lock mutated from ThinLockImpl to LockImpl. */
            useLock = lar.useLock;
            lockTable.put(lsn, useLock);
            /* We still have the overhead of the hashtable (locktable). */
            memoryBudget.updateLockMemoryUsage
                (THINLOCK_MUTATE_OVERHEAD, lockTableIndex);
        }
        LockGrantType lockGrant = lar.lockGrant;
        boolean success = false;

        /* Was the attempt successful? */
        if ((lockGrant == LockGrantType.NEW) ||
            (lockGrant == LockGrantType.PROMOTION)) {
            locker.addLock(lsn, type, lockGrant);
            success = true;
        } else if (lockGrant == LockGrantType.EXISTING) {
            success = true;
        } else if (lockGrant == LockGrantType.DENIED) {
            /* Locker.lock will throw LockNotAvailableException. */
        } else {
            nWaits.increment();
        }
        return new LockAttemptResult(useLock, lockGrant, success);
    }

    /**
     * Create a informative lock or txn timeout message.
     */
    abstract LockConflictException makeTimeoutMsg(boolean isLockNotTxnTimeout,
                                                  Locker locker,
                                                  long lsn,
                                                  LockType type,
                                                  LockGrantType grantType,
                                                  Lock useLock,
                                                  long timeout,
                                                  long start,
                                                  long now,
                                                  DatabaseImpl database)
        throws DatabaseException;

    /**
     * Do the real work of creating an lock or txn timeout message.
     */
    LockConflictException makeTimeoutMsgInternal(boolean isLockNotTxnTimeout,
                                                 Locker locker,
                                                 long lsn,
                                                 LockType type,
                                                 LockGrantType grantType,
                                                 Lock useLock,
                                                 long timeout,
                                                 long start,
                                                 long now,
                                                 DatabaseImpl database) {

        /*
         * Because we're accessing parts of the lock, need to have protected
         * access to the lock table because things can be changing out from
         * underneath us.  This is a big hammer to grab for so long while we
         * traverse the graph, but it's only when we have a deadlock and we're
         * creating a debugging message.
         *
         * The alternative would be to handle ConcurrentModificationExceptions
         * and retry until none of them happen.
         */
        if (lockTableDump) {
            System.out.println("++++++++++ begin lock table dump ++++++++++");
            for (int i = 0; i < nLockTables; i++) {
                boolean success = false;
                for (int j = 0; j < 3 && !success; j++) {
                    try {
                        StringBuilder sb = new StringBuilder();
                        dumpToStringNoLatch(sb, i);
                        System.out.println(sb.toString());
                        success = true;
                        break; // for j...
                    } catch (ConcurrentModificationException CME) {
                        continue;
                    }
                }
                if (!success) {
                    System.out.println("Couldn't dump locktable " + i);
                }
            }
            System.out.println("++++++++++ end lock table dump ++++++++++");
        }

        StringBuilder sb = new StringBuilder();
        sb.append(isLockNotTxnTimeout ? "Lock" : "Transaction");
        sb.append(" expired. Locker ").append(locker);
        sb.append(": waited for lock");

        if (database != null) {
            sb.append(" on database=").append(database.getDebugName());
        }
        sb.append(" LockAddr:").append(System.identityHashCode(useLock));
        sb.append(" LSN=").append(DbLsn.getNoFormatString(lsn));
        sb.append(" type=").append(type);
        sb.append(" grant=").append(grantType);
        sb.append(" timeoutMillis=").append(timeout);
        sb.append(" startTime=").append(start);
        sb.append(" endTime=").append(now);
        Set<LockInfo> owners = useLock.getOwnersClone();
        List<LockInfo> waiters = useLock.getWaitersListClone();
        sb.append("\nOwners: ").append(owners);
        sb.append("\nWaiters: ").append(waiters).append("\n");
        StringBuilder deadlockInfo = findDeadlock(useLock, locker);
        if (deadlockInfo != null) {
            sb.append(deadlockInfo);
        }
        LockConflictException ret = isLockNotTxnTimeout ?
            newLockTimeoutException(locker, sb.toString()) :
            newTxnTimeoutException(locker, sb.toString());

        ret.setOwnerTxnIds(getTxnIds(owners));
        ret.setWaiterTxnIds(getTxnIds(waiters));
        ret.setTimeoutMillis(timeout);
        return ret;
    }

    private long[] getTxnIds(Collection<LockInfo> c) {
        long[] ret = new long[c.size()];
        Iterator<LockInfo> iter = c.iterator();
        int i = 0;
        while (iter.hasNext()) {
            LockInfo info = iter.next();
            ret[i++] = info.getLocker().getId();
        }

        return ret;
    }

    /**
     * This method should always be called instead of explicitly creating
     * TransactionTimeoutException, to ensure that je.lock.oldLockExceptions is
     * enforced.
     */
    private LockConflictException newTxnTimeoutException(Locker locker,
                                                         String msg) {
        return oldLockExceptions ?
            new DeadlockException(locker, msg) :
            new TransactionTimeoutException(locker, msg);
    }

    /**
     * This method should always be called instead of explicitly creating
     * LockTimeoutException, to ensure that je.lock.oldLockExceptions is
     * enforced.
     */
    private LockConflictException newLockTimeoutException(Locker locker,
                                                          String msg) {
        return oldLockExceptions ?
            new DeadlockException(locker, msg) :
            new LockTimeoutException(locker, msg);
    }

    /**
     * This method should always be called instead of explicitly creating
     * LockNotAvailableException, to ensure that je.lock.oldLockExceptions is
     * enforced.
     */
    LockConflictException newLockNotAvailableException(Locker locker,
                                                       String msg) {
        return oldLockExceptions ?
            new LockNotGrantedException(locker, msg) :
            new LockNotAvailableException(locker, msg);
    }

    /**
     * Release a lock and possibly notify any waiters that they have been
     * granted the lock.
     *
     * @param lsn The LSN of the lock to release.
     *
     * @return true if the lock is released successfully, false if
     * the lock is not currently being held.
     */
    public boolean release(long lsn, Locker locker)
        throws DatabaseException {

        synchronized (locker) {
            Set<Locker> newOwners =
                releaseAndFindNotifyTargets(lsn, locker);

            if (newOwners == null) {
                return false;
            }

            if (newOwners.size() > 0) {

                /*
                 * There is a new set of owners and/or there are restart
                 * waiters that should be notified.
                 */
                Iterator<Locker> iter = newOwners.iterator();

                while (iter.hasNext()) {
                    Locker lockerToNotify = iter.next();

                    /* Use notifyAll to support multiple threads per txn. */
                    synchronized (lockerToNotify) {
                        lockerToNotify.notifyAll();
                    }

                    assert EnvironmentImpl.maybeForceYield();
                }
            }

            return true;
        }
    }

    /**
     * Release the lock, and return the set of new owners to notify, if any.
     *
     * @return
     * null if the lock does not exist or the given locker was not the owner,
     * a non-empty set if owners should be notified after releasing,
     * an empty set if no notification is required.
     */
    abstract Set<Locker> releaseAndFindNotifyTargets(long lsn,
                                                     Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of releaseAndFindNotifyTargets
     */
    Set<Locker> releaseAndFindNotifyTargetsInternal(long lsn,
                                                    Locker locker,
                                                    int lockTableIndex) {
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(lsn);
        if (useLock == null) {
            useLock = lockTable.get(Long.valueOf(lsn));
        }

        if (useLock == null) {
            /* Lock doesn't exist. */
            return null;
        }

        Set<Locker> lockersToNotify =
            useLock.release(locker, memoryBudget, lockTableIndex);
        if (lockersToNotify == null) {
            /* Not owner. */
            return null;
        }

        /* If it's not in use at all, remove it from the lock table. */
        if ((useLock.nWaiters() == 0) &&
            (useLock.nOwners() == 0)) {
            lockTables[lockTableIndex].remove(lsn);
            if (useLock.isThin()) {
                memoryBudget.updateLockMemoryUsage
                    (REMOVE_TOTAL_THINLOCKIMPL_OVERHEAD, lockTableIndex);
            } else {
                memoryBudget.updateLockMemoryUsage
                    (REMOVE_TOTAL_LOCKIMPL_OVERHEAD, lockTableIndex);
            }
        }

        return lockersToNotify;
    }

    /**
     * Demote a lock from write to read. Call back to the owning locker to
     * move this to its read collection.
     * @param lock The lock to release. If null, use LSN to find lock
     * @param locker
     */
    abstract void demote(long lsn, Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of demote.
     */
    void demoteInternal(long lsn, Locker locker, int lockTableIndex) {
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock useLock = lockTable.get(Long.valueOf(lsn));
        /* Lock may or may not be currently held. */
        if (useLock != null) {
            useLock.demote(locker);
            locker.moveWriteToReadLock(lsn, useLock);
        }
    }

    /**
     * Test the status of the lock on LSN.  If any transaction holds any
     * lock on it, true is returned.  If no transaction holds a lock on it,
     * false is returned.
     *
     * This method is only used by unit tests.
     *
     * @param lsn The LSN to check.
     * @return true if any transaction holds any lock on the LSN. false
     * if no lock is held by any transaction.
     */
    abstract boolean isLocked(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of isLocked.
     */
    boolean isLockedInternal(Long lsn, int lockTableIndex) {

        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock entry = lockTable.get(lsn);
        if (entry == null) {
            return false;
        }

        return entry.nOwners() != 0;
    }

    /**
     * Return true if this locker owns this a lock of this type on given node.
     *
     * This method is only used by unit tests.
     */
    abstract boolean isOwner(Long lsn, Locker locker, LockType type)
        throws DatabaseException;

    /**
     * Do the real work of isOwner.
     */
    boolean isOwnerInternal(Long lsn,
                            Locker locker,
                            LockType type,
                            int lockTableIndex) {

        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock entry = lockTable.get(lsn);
        if (entry == null) {
            return false;
        }

        return entry.isOwner(locker, type);
    }

    /**
     * Return true if this locker is waiting on this lock.
     *
     * This method is only used by unit tests.
     */
    abstract boolean isWaiter(Long lsn, Locker locker)
        throws DatabaseException;

    /**
     * Do the real work of isWaiter.
     */
    boolean isWaiterInternal(Long lsn,
                             Locker locker,
                             int lockTableIndex) {

        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock entry = lockTable.get(lsn);
        if (entry == null) {
            return false;
        }

        return entry.isWaiter(locker);
    }

    /**
     * Return the number of waiters for this lock.
     */
    abstract int nWaiters(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of nWaiters.
     */
    int nWaitersInternal(Long lsn, int lockTableIndex) {

        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock entry = lockTable.get(lsn);
        if (entry == null) {
            return -1;
        }

        return entry.nWaiters();
    }

    /**
     * Return the number of owners of this lock.
     */
    abstract int nOwners(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of nWaiters.
     */
    int nOwnersInternal(Long lsn, int lockTableIndex) {

        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock entry = lockTable.get(lsn);
        if (entry == null) {
            return -1;
        }

        return entry.nOwners();
    }

    /**
     * @return the transaction that owns the write lock for this
     */
    abstract Locker getWriteOwnerLocker(Long lsn)
        throws DatabaseException;

    /**
     * Do the real work of getWriteOwnerLocker.
     */
    Locker getWriteOwnerLockerInternal(Long lsn, int lockTableIndex) {
        Map<Long,Lock> lockTable = lockTables[lockTableIndex];
        Lock lock = lockTable.get(lsn);
        if (lock == null) {
            return null;
        } else if (lock.nOwners() > 1) {
            /* not a write lock */
            return null;
        } else {
            return lock.getWriteOwnerLocker();
        }
    }

    /*
     * Check if we got ownership while we were waiting.  If we didn't get
     * ownership, and we timed out, remove this locker from the set of
     * waiters. Do this in a critical section to prevent any orphaning of the
     * lock -- we must be in a critical section between the time that we check
     * ownership and when we flush any waiters (SR #10103)
     * @return true if you are the owner.
     */
    abstract boolean validateOwnership(Long lsn,
                                       Locker locker,
                                       LockType type,
                                       boolean flushFromWaiters,
                                       MemoryBudget mb)
        throws DatabaseException;

    /*
     * Do the real work of validateOwnershipInternal.
     */
    boolean validateOwnershipInternal(Long lsn,
                                      Locker locker,
                                      LockType type,
                                      boolean flushFromWaiters,
                                      MemoryBudget mb,
                                      int lockTableIndex) {
        if (isOwnerInternal(lsn, locker, type, lockTableIndex)) {
            return true;
        }

        if (flushFromWaiters) {
            Lock entry = lockTables[lockTableIndex].get(lsn);
            if (entry != null) {
                entry.flushWaiter(locker, mb, lockTableIndex);
            }
        }
        return false;
    }

    abstract protected LockAttemptResult stealLock(Long lsn,
                                                   Locker locker,
                                                   LockType lockType,
                                                   MemoryBudget mb)
        throws DatabaseException;

    protected LockAttemptResult stealLockInternal(Long lsn,
                                                  Locker locker,
                                                  LockType lockType,
                                                  MemoryBudget mb,
                                                  int lockTableIndex)
        throws DatabaseException {

        Lock entry = lockTables[lockTableIndex].get(lsn);
        assert entry != null;

        /*
         * Note that flushWaiter may do nothing, because the lock may have been
         * granted to our locker after the prior call to attemptLock and before
         * the call to this method.
         */
        entry.flushWaiter(locker, mb, lockTableIndex);

        /* Remove all owners except for our owner. */
        entry.stealLock(locker, mb, lockTableIndex);

        /*
         * The lock attempt normally succeeds, but can fail if the lock holder
         * is non-preemptable.
         */
        return attemptLockInternal
            (lsn, locker, lockType, false /*nonBlockingRequest*/,
             false /*jumpAheadOfWaiters*/, lockTableIndex);
    }

    /**
     * Called when a ThreadLocker is created.
     */
    public void registerThreadLocker(final ThreadLocker locker) {
        if (threadLockers == null) {
            return;
        }
        final Thread thread = Thread.currentThread();
        final TinyHashSet<ThreadLocker> set = threadLockers.get(thread);
        if (set != null) {
            final boolean added = set.add(locker);
            assert added;
        } else {
            threadLockers.put(thread, new TinyHashSet(locker));
        }
    }

    /**
     * Called when a ThreadLocker is closed.
     */
    public void unregisterThreadLocker(final ThreadLocker locker) {
        if (threadLockers == null) {
            return;
        }
        final Thread thread = Thread.currentThread();
        final TinyHashSet<ThreadLocker> set = threadLockers.get(thread);
        assert set != null;
        final boolean removed = set.remove(locker);
        assert removed;
        if (threadLockers.size() == 0) {
            threadLockers.remove(thread);
        }
    }

    /**
     * Returns an iterator over all thread lockers for the given thread, or
     * an empty iterator if none.
     */
    public Iterator<ThreadLocker> getThreadLockers(final Thread thread) {
        if (threadLockers == null) {
            return EMPTY_THREAD_LOCKERS.iterator();
        }
        final TinyHashSet<ThreadLocker> set = threadLockers.get(thread);
        if (set == null) {
            return EMPTY_THREAD_LOCKERS.iterator();
        }
        return set.iterator();
    }

    /**
     * Statistics
     */
    public LockStats lockStat(StatsConfig config)
        throws DatabaseException {

        StatGroup latchStats = new StatGroup("Locktable latches", 
                                             "Shows lock table contention");
        for (int i = 0; i < nLockTables; i++) {
            latchStats.addAll(lockTableLatches[i].getLatchStats());
        }

        /* Dump info about the lock table. */
        StatGroup tableStats = 
            new StatGroup("Locktable",
                          "The types of locks held in the lock table");
        if (!config.getFast()) {
            dumpLockTable(tableStats, false /*clear*/);
        }
        
        return new LockStats(stats.cloneGroup(config.getClear()),
                             latchStats.cloneGroup(config.getClear()),
                             tableStats.cloneGroup(config.getClear()));
    }

    public StatGroup loadStats(StatsConfig config) {
        StatGroup copyStats = stats.cloneGroup(config.getClear());

        StatGroup latchStats = new StatGroup("Locktable latches", 
                                             "Shows lock table contention");
        for (int i = 0; i < nLockTables; i++) {
            latchStats.addAll(lockTableLatches[i].getLatchStats());
            if (config.getClear()) {
                lockTableLatches[i].clear();
            }
        }
        /* Add all the latch stats to the whole stats group. */
        copyStats.addAll(latchStats);

        StatGroup tableStats = 
            new StatGroup("Locktable",
                          "The types of locks held in the lock table");
        if (!config.getFast()) {
            dumpLockTable(tableStats, config.getClear());
        }
        /* Add all the lock table stats to the whole stats group. */
        copyStats.addAll(tableStats);
        
        return copyStats;
    }

    /**
     * Dump the lock table to the lock stats.
     */
    abstract void dumpLockTable(StatGroup tableStats, boolean clear)
        throws DatabaseException;

    /**
     * Do the real work of dumpLockTableInternal.
     */
    void dumpLockTableInternal(StatGroup tableStats, int i, boolean clear) {
        StatGroup oneTable = new StatGroup("Single lock table",
                                           "Temporary stat group");

        IntStat totalLocks = new IntStat(oneTable, LOCK_TOTAL);
        IntStat waiters = new IntStat(oneTable, LOCK_WAITERS);
        IntStat owners = new IntStat(oneTable, LOCK_OWNERS);
        IntStat readLocks = new IntStat(oneTable, LOCK_READ_LOCKS);
        IntStat writeLocks = new IntStat(oneTable, LOCK_WRITE_LOCKS);

        Map<Long, Lock> lockTable = lockTables[i];
        totalLocks.add(lockTable.size());

        for (Lock lock : lockTable.values()) {
            waiters.add(lock.nWaiters());
            owners.add(lock.nOwners());

            /* Go through all the owners for a lock. */
            for (LockInfo info : lock.getOwnersClone()) {
                if (info.getLockType().isWriteLock()) {
                    writeLocks.increment();
                } else {
                    readLocks.increment();
                }
            }
        }
        tableStats.addAll(oneTable);
    }

    /**
     * Debugging
     */
    public void dump()
        throws DatabaseException {

        System.out.println(dumpToString());
    }

    public String dumpToString()
        throws DatabaseException {

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < nLockTables; i++) {
            lockTableLatches[i].acquire();
            try {
                dumpToStringNoLatch(sb, i);
            } finally {
                lockTableLatches[i].release();
            }
        }
        return sb.toString();
    }

    private void dumpToStringNoLatch(StringBuilder sb, int whichTable) {
        Map<Long,Lock> lockTable = lockTables[whichTable];
        Iterator<Map.Entry<Long,Lock>> entries =
            lockTable.entrySet().iterator();

        while (entries.hasNext()) {
            Map.Entry<Long,Lock> entry = entries.next();
            Long lsn = entry.getKey();
            Lock lock = entry.getValue();
            sb.append("---- LSN: ").
               append(DbLsn.getNoFormatString(lsn)).
               append("----\n");
            sb.append(lock);
            sb.append('\n');
        }
    }

    private boolean checkNoLatchesHeld(boolean nonBlockingRequest) {
        if (nonBlockingRequest) {
            return true; // don't check if it's a non blocking request.
        } else {
            return (LatchSupport.countLatchesHeld() == 0);
        }
    }

    private StringBuilder findDeadlock(Lock lock, Locker rootLocker) {

        Set<Locker> ownerSet = new HashSet<Locker>();
        ownerSet.add(rootLocker);
        StringBuilder ret = findDeadlock1(ownerSet, lock, rootLocker);
        if (ret != null) {
            return ret;
        } else {
            return null;
        }
    }

    private StringBuilder findDeadlock1(Set<Locker> ownerSet,
                                       Lock lock,
                                       Locker rootLocker) {

        Iterator<LockInfo> ownerIter = lock.getOwnersClone().iterator();
        while (ownerIter.hasNext()) {
            LockInfo info = ownerIter.next();
            Locker locker = info.getLocker();
            Lock waitsFor = locker.getWaitingFor();
            if (ownerSet.contains(locker) ||
                locker == rootLocker) {
                /* Found a cycle. */
                StringBuilder ret = new StringBuilder();
                ret.append("Transaction ").append(locker.toString());
                ret.append(" owns LockAddr:").
                    append(System.identityHashCode(lock));
                ret.append(" ").append(info).append("\n");
                ret.append("Transaction ").append(locker.toString());
                ret.append(" waits for");
                if (waitsFor == null) {
                    ret.append(" nothing");
                } else {
                    ret.append(" LockAddr:");
                    ret.append(System.identityHashCode(waitsFor));
                }
                ret.append("\n");
                return ret;
            }
            if (waitsFor != null) {
                ownerSet.add(locker);
                StringBuilder sb = findDeadlock1(ownerSet, waitsFor,
                                                rootLocker);
                if (sb != null) {
                    String waitInfo =
                        "Transaction " + locker + " waits for " +
                        waitsFor + "\n";
                    sb.insert(0, waitInfo);
                    return sb;
                }
                ownerSet.remove(locker); // is this necessary?
            }
        }

        return null;
    }
}
