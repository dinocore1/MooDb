/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.txn;

import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.StatGroup;

/**
 * Locker instances are JE's route to locking and transactional support.  This
 * class is the abstract base class for BasicLocker, ThreadLocker, Txn,
 * MasterTxn and ReadonlyTxn.  Locker instances are in fact only a transaction
 * shell to get to the lock manager, and don't guarantee transactional
 * semantics.
 *
 * Txn (includes Txns marked autoTxn) MasterTxn and ReadonlyTxn instances are
 * truly transactional. They have potentially different transaction begin and
 * end behaviors.
 */
public abstract class Locker {

    protected EnvironmentImpl envImpl;
    protected LockManager lockManager;

    protected long id;                        // transaction id
    protected boolean readUncommittedDefault; // read-uncommitted is default

    /* Timeouts */
    protected boolean defaultNoWait;      // true for non-blocking
    private long lockTimeoutMillis;       // timeout period for lock, in ms
    private long txnTimeoutMillis;        // timeout period for txns, in ms
    private long txnStartMillis;          // for txn timeout determination

    private Lock waitingFor;              // The lock that this txn is
                                          // waiting for.

    /*
     * DeleteInfo refers to BINReferences that should be sent to the
     * INCompressor for asynchronous compressing after the transaction ends.
     */
    protected Map<Long,BINReference> deleteInfo;

    /**
     * The thread that created this locker.  Used for debugging, and by the
     * ThreadLocker subclass. Note that thread may be null if the Locker is
     * instantiated by reading the log.
     */
    protected Thread thread;

    /**
     * Set to false when close() is called.  After that point no other locker
     * operations should occur.
     */
    private boolean isOpen = true;

    /**
     * True if my locks can be preempted/stolen.
     */
    private boolean preemptable = true;

    /**
     * Non-null if a lock has been stolen from this locker by the HA replayer.
     */
    private RuntimeException preemptedCause;

    /**
     * Non-null if this locker is replacing another locker that is in the
     * process of closing because a cursor is being moved.
     */
    private Locker closingLocker;

    /**
     * Create a locker id. This constructor is called very often, so it should
     * be as streamlined as possible. It should never be called directly,
     * because the mandatedId mechanism only works if the generateId() method
     * is overridden to use the mandatedId value.
     *
     * @param lockManager lock manager for this environment
     * @param readUncommittedDefault if true, this transaction does
     * read-uncommitted by default
     * @param noWait if true, non-blocking lock requests are used.
     */
    protected Locker(EnvironmentImpl envImpl,
                     boolean readUncommittedDefault,
                     boolean noWait,
                     long mandatedId) {
        initLocker(envImpl, readUncommittedDefault, noWait, mandatedId);
    }

    private void initLocker(EnvironmentImpl envImpl,
                            boolean readUncommittedDefault,
                            boolean noWait,
                            long mandatedId) {
        TxnManager txnManager = envImpl.getTxnManager();
        this.lockManager = txnManager.getLockManager();
        this.id = generateId(txnManager, mandatedId);
        this.envImpl = envImpl;
        this.readUncommittedDefault = readUncommittedDefault;
        this.waitingFor = null;

        /* get the default lock timeout. */
        defaultNoWait = noWait;
        lockTimeoutMillis = getInitialLockTimeout();

        /*
         * Check the default txn timeout. If non-zero, remember the txn start
         * time.
         */
        txnTimeoutMillis = envImpl.getTxnTimeout();

        if (txnTimeoutMillis != 0) {
            txnStartMillis = System.currentTimeMillis();
        } else {
            txnStartMillis = 0;
        }

        /* Save the thread used to create the locker. */
        thread = Thread.currentThread();

        /* Do lazy initialization of deleteInfo, to conserve memory. */
    }
    
    /**
     * For reading from the log.
     */
    Locker() {
    }

    protected long getInitialLockTimeout() {
        return envImpl.getLockTimeout();
    }

    public EnvironmentImpl getEnvironment() {
        return envImpl;
    }

    /**
     * A Locker has to generate its next id. Some subtypes, like BasicLocker,
     * have a single id for all instances because they are never used for
     * recovery. Other subtypes ask the txn manager for an id or use a
     * specific, mandated id.
     */
    protected abstract long generateId(TxnManager txnManager, long mandatedId);

    /**
     * @return the transaction's id.
     */
    public long getId() {
        return id;
    }

    /**
     * @return the default no-wait (non-blocking) setting.
     */
    public boolean getDefaultNoWait() {
        return defaultNoWait;
    }

    /**
     * Get the lock timeout period for this locker, in milliseconds
     *
     * WARNING: Be sure to always access the timeout with this accessor, since
     * it is overridden in BuddyLocker.
     */
    public synchronized long getLockTimeout() {
        return lockTimeoutMillis;
    }

    /**
     * Set the lock timeout period for any locks in this transaction,
     * in milliseconds.
     *
     * @param timeout The timeout value for the transaction lifetime, in
     * milliseconds. A value of 0 disables timeouts for the transaction.
     *
     * @throws IllegalArgumentException via Transaction.setLockTimeout
     */
    public synchronized void setLockTimeout(long timeout) {

        if (timeout < 0) {
            throw new IllegalArgumentException
                ("the timeout value cannot be negative");
        } else if (timeout > Math.pow(2, 32)) {
            throw new IllegalArgumentException
                ("the timeout value cannot be greater than 2^32");
        }

        lockTimeoutMillis = timeout;
    }

    /**
     * Set the timeout period for this transaction, in milliseconds.
     *
     * @param timeout The timeout value for the transaction lifetime, in
     * microseconds. A value of 0 disables timeouts for the transaction.
     *
     * @throws IllegalArgumentException via Transaction.setLockTimeout
     */
    public synchronized void setTxnTimeout(long timeout) {

        if (timeout < 0) {
            throw new IllegalArgumentException
                ("the timeout value cannot be negative");
        } else if (timeout > Math.pow(2, 32)) {
            throw new IllegalArgumentException
                ("the timeout value cannot be greater than 2^32");
        }

        txnTimeoutMillis = timeout;
        if (txnTimeoutMillis != 0) {
            txnStartMillis = System.currentTimeMillis();
        } else {
            txnStartMillis = 0;
        }
    }

    /**
     * @return true if transaction was created with read-uncommitted as a
     * default.
     */
    public boolean isReadUncommittedDefault() {
        return readUncommittedDefault;
    }

    Lock getWaitingFor() {
        return waitingFor;
    }

    void setWaitingFor(Lock lock) {
        waitingFor = lock;
    }

    /**
     * Set the state of a transaction to abort-only.  Should ONLY be called
     * by OperationFailureException.
     */
    public void setOnlyAbortable(OperationFailureException cause) {
        /* no-op unless Txn. */
    }

    /**
     * Set the state of a transaction's IMPORTUNATE bit.
     */
    public void setImportunate(boolean importunate) {
        /* no-op unless Txn. */
    }

    /**
     * Get the state of a transaction's IMPORTUNATE bit.
     */
    public boolean getImportunate() {
        return false;
    }

    /**
     * Allows/disallows my locks from being stolen/preemted.
     */
    public void setPreemptable(boolean preemptable) {
        this.preemptable = preemptable;
    }

    /**
     * Returns whether my locks can be stolen/preemted.
     */
    public boolean getPreemptable() {
        return preemptable;
    }

    /**
     * Called when a lock is stolen from this locker by the HA replayer.
     */
    public void setPreempted() {

        /*
         * Record the stack trace when a lock is stolen.  This will provide
         * more "cause" information when it is wrapped in a
         * LockPreemptedException that is thrown later -- see checkPreempted.
         */
        preemptedCause = new RuntimeException
            ("Lock was preempted by the replication replayer");
    }

    /**
     * Called when obtaining a lock to cause a LockPreemptedException to be
     * thrown if a lock was preempted earlier.
     *
     * This operation is split into two methods, checkPreempted and
     * throwIfPreempted, so that Txn.checkPreempted can call throwIfPreempted
     * for all its BuddyLockers without causing an infinite recursion.  This
     * method is overridden by BuddyLocker to forward the call to its parent
     * buddy (the Txn), and by Txn to check all its child buddies.
     *
     * @param allowPreemptedLocker is a locker that is being closed as the
     * result of a cursor move operation.  If the operation is successful then
     * allowPreemptedLocker will be closed, and the fact that a lock has been
     * stolen from allowPreemptedLocker can be ignored.
     */
    public void checkPreempted(final Locker allowPreemptedLocker) 
        throws OperationFailureException {

        throwIfPreempted(allowPreemptedLocker);
    }

    /**
     * Called by checkPreempted to cause a LockPreemptedException to be thrown
     * if a lock was preempted earlier.  Creating the LockPreemptedException
     * sets the txn to abort-only.
     *
     * @see #checkPreempted
     */
    final void throwIfPreempted(final Locker allowPreemptedLocker) 
        throws OperationFailureException {

        if (this != allowPreemptedLocker &&
            preemptedCause != null) {
            throw envImpl.createLockPreemptedException(this, preemptedCause);
        }
    }

    /** For unit testing. */
    final boolean isPreempted() {
        return (preemptedCause != null);
    }

    /**
     * This method is called to set the closingLocker when a cursor has been
     * duplicated prior to being moved.  The new locker is informed of the old
     * locker, so that a preempted lock taken by the old locker can be ignored.
     * When the operation is complete, this method is called to clear the
     * closingLocker so that a reference to the old closed locker is not held
     * by this object.  [#16513]
     *
     * @param closingLocker the old locker that will be closed if the new
     * cursor (using this locker) is moved successfully.
     */
    public void setClosingLocker(final Locker closingLocker) {
        this.closingLocker = closingLocker;
    }

    /**
     * See ThreadLocker.allowMultithreadedAccess.
     */
    boolean setAllowMultithreadedAccess(boolean allow) {
        /* Do nothing by default. Is overridden by ThreadLocker. */
        return false;
    }

    protected abstract void checkState(boolean ignoreCalledByAbort)
        throws DatabaseException;

    /**
     * Overridden to perform actions in a non-transactional cursor when it is
     * opened, for example, ReplicaThreadLocker performs consistency checks.
     */
    public void openCursorHook(DatabaseImpl dbImpl) {
        /* Do nothing. */
    }

    /**
     * Used for debugging checks to ensure that replication-defined lockers are
     * used for accessing replicated databases.  Overridden by
     * replicated-defined lockers to return true.
     */
    public boolean isReplicationDefined() {
        return false;
    }

    /*
     * Obtain and release locks.
     */

    /**
     * Abstract method to a blocking or non-blocking lock of the given type on
     * the given LSN.  Unlike the lock() method, this method does not throw
     * LockNotAvailableException and can therefore be used by nonBlockingLock
     * to probe for a lock without the overhead of an exception stack trace.
     *
     * @param lsn is the node to lock.
     *
     * @param lockType is the type of lock to request.
     *
     * @param noWait is true to override the defaultNoWait setting.  If true,
     * or if defaultNoWait is true, throws LockNotAvailableException if the
     * lock cannot be granted without waiting.
     *
     * @param jumpAheadOfWaiters grant the lock before other waiters, if any.
     *
     * @param database is the database containing lsn.
     *
     * @throws LockConflictException if a blocking lock could not be acquired.
     */
    abstract LockResult lockInternal(long lsn,
                                     LockType lockType,
                                     boolean noWait,
                                     boolean jumpAheadOfWaiters,
                                     DatabaseImpl database)
        throws LockConflictException, DatabaseException;

    /**
     * Request a blocking or non-blocking lock of the given type on the given
     * LSN.
     *
     * @param lsn is the node to lock.
     *
     * @param lockType is the type of lock to request.
     *
     * @param noWait is true to override the defaultNoWait setting.  If true,
     * or if defaultNoWait is true, throws LockNotAvailableException if the
     * lock cannot be granted without waiting.
     *
     * @param database is the database containing lsn.
     *
     * @throws LockNotAvailableException if a non-blocking lock was denied.
     *
     * @throws LockConflictException if a blocking lock could not be acquired.
     */
    public LockResult lock(long lsn,
                           LockType lockType,
                           boolean noWait,
                           DatabaseImpl database)
        throws LockNotAvailableException, LockConflictException {

        final LockResult result = lockInternal
            (lsn, lockType, noWait, false /*jumpAheadOfWaiters*/, database);

        if (result.getLockGrant() == LockGrantType.DENIED) {
            /* DENIED can only be returned for a non-blocking lock. */
            throw lockManager.newLockNotAvailableException
                (this, "Non-blocking lock was denied.");
        } else {
            checkPreempted(closingLocker);
            return result;
        }
    }

    /**
     * Request a non-blocking lock of the given type on the given LSN.
     *
     * <p>Unlike lock(), this method returns LockGrantType.DENIED if the lock
     * is denied rather than throwing LockNotAvailableException.  This method
     * should therefore not be used as the final lock for a user operation,
     * since in that case LockNotAvailableException should be thrown for a
     * denied lock.  It is normally used only to probe for a lock internally,
     * and other recourse is taken if the lock is denied.</p>
     *
     * @param lsn is the node to lock.
     *
     * @param lockType is the type of lock to request.
     *
     * @param jumpAheadOfWaiters grant the lock before other waiters, if any.
     *
     * @param database is the database containing LSN.
     */
    public LockResult nonBlockingLock(long lsn,
                                      LockType lockType,
                                      boolean jumpAheadOfWaiters,
                                      DatabaseImpl database) {
        final LockResult result = lockInternal
            (lsn, lockType, true /*noWait*/, jumpAheadOfWaiters, database);
        if (result.getLockGrant() != LockGrantType.DENIED) {
            checkPreempted(closingLocker);
        }
        return result;
    }

    /**
     * Release the lock on this LN and remove from the transaction's owning
     * set.
     */
    public synchronized boolean releaseLock(long lsn)
        throws DatabaseException {

        boolean ret = lockManager.release(lsn, this);
        removeLock(lsn);
        return ret;
    }

    /**
     * Revert this lock from a write lock to a read lock.
     */
    public void demoteLock(long lsn)
        throws DatabaseException {

        /*
         * If successful, the lock manager will call back to the transaction
         * and adjust the location of the lock in the lock collection.
         */
        lockManager.demote(lsn, this);
    }

    /**
     * Called when an LN is logged by an operation that will not hold the lock
     * such as eviction/checkpoint deferred-write logging or cleaner LN
     * migration.  We must acquire a lock on the new LSN on behalf of every
     * locker that currently holds a lock on the old LSN.
     *
     * Lock is non-blocking because no contention is possible on the new LSN.
     *
     * Because this locker is being used by multiple threads, this method may
     * be called for a locker that has been closed or for which the lock on the
     * old LSN has been released.  Unlike other locking methods, in this case
     * we simply return rather than report an error.
     */
    public synchronized void lockAfterLsnChange(long oldLsn,
                                                long newLsn,
                                                DatabaseImpl dbImpl) {
        if (!isValid()) {
            /* Locker was recently closed, made abort-only, etc. */
            return;
        }

        final LockType lockType = lockManager.getOwnedLockType(oldLsn, this);
        if (lockType == null) {
            /* Lock was recently released. */
            return;
        }

        final LockResult lockResult = nonBlockingLock
            (newLsn, lockType, false /*jumpAheadOfWaiters*/, dbImpl);

        if (lockResult.getLockGrant() == LockGrantType.DENIED) {
            throw EnvironmentFailureException.unexpectedState
                ("No contention is possible on new LSN: " +
                 DbLsn.getNoFormatString(newLsn) +
                 " old LSN: " + DbLsn.getNoFormatString(oldLsn) +
                 " LockType: " + lockType);
        }
    }

    /**
     * In the case where logging occurs before locking, we must ensure that
     * we're prepared to undo if logging succeeds but locking fails.
     */
    public abstract void preLogWithoutLock(DatabaseImpl database);

    /**
     * Throws ReplicaWriteException if called for a locker on a Replica.  This
     * implementation does nothing but is overridden by replication lockers.
     * [#20543]
     */
    public void disallowReplicaWrite() {
    }

    /**
     * Returns whether this locker is transactional.
     */
    public abstract boolean isTransactional();

    /**
     * Returns whether the isolation level of this locker is serializable.
     */
    public abstract boolean isSerializableIsolation();

    /**
     * Returns whether the isolation level of this locker is read-committed.
     */
    public abstract boolean isReadCommittedIsolation();

    /**
     * Returns the underlying Txn if the locker is transactional, or null if
     * the locker is non-transactional.  For a Txn-based locker, this method
     * returns 'this'.  For a BuddyLocker, this method may return the buddy.
     */
    public abstract Txn getTxnLocker();
  
    /**
     * Returns a Transaction is the locker is transctional, or null otherwise.
     */
    public Transaction getTransaction() {
        return null;
    }

    /**
     * Creates a fresh non-transactional locker, while retaining any
     * transactional locks held by this locker.  This method is called when the
     * cursor for this locker is cloned.
     *
     * <p>This method must return a locker that shares locks with this
     * locker, e.g., a ThreadLocker.</p>
     *
     * <p>In general, transactional lockers return 'this' when this method is
     * called, while non-transactional lockers return a new instance.</p>
     */
    public abstract Locker newNonTxnLocker()
        throws DatabaseException;

    /**
     * Releases any non-transactional locks held by this locker.  This method
     * is called when the cursor moves to a new position or is closed.
     *
     * <p>In general, transactional lockers do nothing when this method is
     * called, while non-transactional lockers release all locks as if
     * operationEnd were called.</p>
     */
    public abstract void releaseNonTxnLocks()
        throws DatabaseException;

    /**
     * Releases locks and closes the locker at the end of a non-transactional
     * cursor operation.  For a transctional cursor this method should do
     * nothing, since locks must be held until transaction end.
     */
    public abstract void nonTxnOperationEnd()
        throws DatabaseException;

    /**
     * By default the set of buddy lockers is not maintained.  This is
     * overridden by Txn.
     */
    void addBuddy(BuddyLocker buddy) {
    }

    /**
     * By default the set of buddy lockers is not maintained.  This is
     * overridden by Txn.
     */
    void removeBuddy(BuddyLocker buddy) {
    }

    /**
     * Returns whether this locker can share locks with the given locker.
     */
    public boolean sharesLocksWith(Locker other) {
        return false;
    }

    /**
     * The equivalent of calling operationEnd(true).
     */
    public final void operationEnd()
        throws DatabaseException {

        operationEnd(true);
    }

    /**
     * A SUCCESS status equals operationOk.
     */
    public final void operationEnd(OperationStatus status)
        throws DatabaseException {

        operationEnd(status == OperationStatus.SUCCESS);
    }

    /**
     * Different types of transactions do different things when the operation
     * ends. Txn does nothing, auto Txn commits or aborts, and BasicLocker (and
     * its subclasses) just releases locks.
     *
     * @param operationOK is whether the operation succeeded, since
     * that may impact ending behavior. (i.e for an auto Txn)
     */
    public abstract void operationEnd(boolean operationOK)
        throws DatabaseException;

    /**
     * Should be called by all subclasses when the locker is no longer used.
     * For Txns and auto Txns this is at commit or abort.  For
     * non-transactional lockers it is at operationEnd.
     */
    void close()
        throws DatabaseException {

        isOpen = false;
    }

    /**
     * Used to determine whether the locker is usable.
     *
     * FUTURE: Note that this method is overridden by Txn, and Txn.abort sets
     * the state to closed when it begins rather than when it ends, but calls
     * close() (the method above) when it ends.  This is not ideal and deserves
     * attention in the future.
     */
    public boolean isValid() {
        return isOpen;
    }

    /**
     * @see Txn#addOpenedDatabase
     */
    public void addOpenedDatabase(Database dbHandle) {
    }

    /**
     * @see HandleLocker#allowReleaseLockAfterLsnChange
     */
    public boolean allowReleaseLockAfterLsnChange() {
        return false;
    }

    /**
     * Tell this transaction about a cursor.
     */
    public abstract void registerCursor(CursorImpl cursor);

    /**
     * Remove a cursor from this txn.
     */
    public abstract void unRegisterCursor(CursorImpl cursor);

    /**
     * Returns true if locking is required for this Locker.  All Txnal lockers
     * require it; most BasicLockers do not, but BasicLockers on internal dbs
     * do.
     */
    public abstract boolean lockingRequired();

    /*
     * Transactional support
     */

    /**
     * @return the WriteLockInfo for this node.
     */
    public abstract WriteLockInfo getWriteLockInfo(long lsn);

    /**
     * Database operations like remove and truncate leave behind
     * residual DatabaseImpls that must be purged at transaction
     * commit or abort.
     */
    public abstract void markDeleteAtTxnEnd(DatabaseImpl db,
                                            boolean deleteAtCommit)
        throws DatabaseException;

    /**
     * Add delete information, to be added to the inCompressor queue when the
     * transaction ends.
     */
    public void addDeleteInfo(BIN bin) {

        /*
         * Skip queue addition if a delta will be logged.  In this case the
         * slot compression will occur in BIN.beforeLog, when a full version is
         * logged.
         */
        if (bin.shouldLogDelta()) {
            return;
        }

        synchronized (this) {
            /* Maintain only one binRef per node. */
            if (deleteInfo == null) {
                deleteInfo = new HashMap<Long,BINReference>();
            }
            Long nodeId = Long.valueOf(bin.getNodeId());
            if (deleteInfo.containsKey(nodeId)) {
                return;
            }
            deleteInfo.put(nodeId, bin.createReference());
        }
    }

    /*
     * Manage locks owned by this transaction. Note that transactions that will
     * be multithreaded must override these methods and provide synchronized
     * implementations.
     */

    /**
     * Add a lock to set owned by this transaction.
     */
    protected abstract void addLock(Long lsn,
                                    LockType type,
                                    LockGrantType grantStatus)
        throws DatabaseException;

    /**
     * @return true if this transaction created this node,
     * for a operation with transactional semantics.
     */
    public abstract boolean createdNode(long lsn);

    /**
     * Remove the lock from the set owned by this transaction. If specified to
     * LockManager.release, the lock manager will call this when its releasing
     * a lock.
     */
    abstract void removeLock(long lsn)
        throws DatabaseException;

    /**
     * A lock is being demoted. Move it from the write collection into the read
     * collection.
     */
    abstract void moveWriteToReadLock(long lsn, Lock lock);

    /**
     * Get lock count, for per transaction lock stats, for internal debugging.
     */
    public abstract StatGroup collectStats()
        throws DatabaseException;

    /*
     * Check txn timeout, if set. Called by the lock manager when blocking on a
     * lock.
     */
    public boolean isTimedOut() {
        long timeout = getTxnTimeout();
        if (timeout != 0) {
            long diff = System.currentTimeMillis() - txnStartMillis;
            if (diff > timeout) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the transaction timeout period for this locker, in milliseconds
     *
     * public for jca/ra/JELocalTransaction.
     *
     * WARNING: Be sure to always access the timeout with this accessor, since
     * it is overridden in BuddyLocker.
     */
    public synchronized long getTxnTimeout() {
        return txnTimeoutMillis;
    }

    long getTxnStartMillis() {
        return txnStartMillis;
    }

    /**
     * @return if this locker has ever been rolled back.
     */
    public boolean isRolledBack() {
        return false;  // most Locker types will never roll back.
    }

    /*
     * Helpers
     */
    @Override
    public String toString() {
        String className = getClass().getName();
        className = className.substring(className.lastIndexOf('.') + 1);

        return System.identityHashCode(this) + " " + Long.toString(id) + "_" +
               ((thread == null) ? "" : thread.getName()) + "_" +
               className;
    }

    /**
     * Dump lock table, for debugging
     */
    public void dumpLockTable()
        throws DatabaseException {

        lockManager.dump();
    }
}
