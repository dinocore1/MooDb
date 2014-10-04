/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DupKeyData;
import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.PutMode;
import com.sleepycat.je.dbi.RangeConstraint;
import com.sleepycat.je.dbi.RangeRestartException;
import com.sleepycat.je.dbi.TriggerManager;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.tree.CountEstimator;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.txn.BuddyLocker;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.utilint.DatabaseUtil;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * A database cursor. Cursors are used for operating on collections of records,
 * for iterating over a database, and for saving handles to individual records,
 * so that they can be modified after they have been read.
 *
 * <p>Cursors which are opened with a transaction instance are transactional
 * cursors and may be used by multiple threads, but only serially.  That is,
 * the application must serialize access to the handle. Non-transactional
 * cursors, opened with a null transaction instance, may not be used by
 * multiple threads.</p>
 *
 * <p>If the cursor is to be used to perform operations on behalf of a
 * transaction, the cursor must be opened and closed within the context of that
 * single transaction.</p>
 *
 * <p>Once the cursor {@link #close} method has been called, the handle may not
 * be accessed again, regardless of the {@code close} method's success or
 * failure, with one exception:  the {@code close} method itself may be called
 * any number of times to simplify error handling.</p>
 *
 * <p>To obtain a cursor with default attributes:</p>
 *
 * <blockquote><pre>
 *     Cursor cursor = myDatabase.openCursor(txn, null);
 * </pre></blockquote>
 *
 * <p>To customize the attributes of a cursor, use a CursorConfig object.</p>
 *
 * <blockquote><pre>
 *     CursorConfig config = new CursorConfig();
 *     config.setReadUncommitted(true);
 *     Cursor cursor = myDatabase.openCursor(txn, config);
 * </pre></blockquote>
 *
 * <p>Modifications to the database during a sequential scan will be reflected
 * in the scan; that is, records inserted behind a cursor will not be returned
 * while records inserted in front of a cursor will be returned.</p>
 *
 * <a name="partialEntry"><h3>Using Partial DatabaseEntry Parameters</h3></a>
 *
 * <p>The {@link DatabaseEntry#setPartial DatabaseEntry Partial} property can
 * be used to optimize in certain cases.  This provides varying degrees of
 * performance benefits that depend on the specific operation and use of {@code
 * READ_UNCOMMITTED} isolation, as described below.</p>
 *
 * <p>When retrieving a record with a {@link Database} or {@link Cursor}
 * method, if only the key is needed by the application then the retrieval of
 * the data item can be suppressed using the Partial property. If {@code
 * setPartial(0, 0, true)} is called for the {@code DatabaseEntry} passed as
 * the data parameter, the data item will not be returned by the {@code
 * Database} or {@code Cursor} method.</p>
 *
 * <p>Suppressing the return of the data item potentially has a large
 * performance benefit when the {@code READ_UNCOMMITTED} isolation mode is
 * used.  In this case, if the record data is not in the JE cache, it will not
 * be read from disk.  The performance benefit is potentially large because
 * random access disk reads may be reduced.  Examples are:</p>
 * <ul>
 * <li>Scanning all records in key order, when the data is not needed and
 * {@code READ_UNCOMMITTED} isolation is acceptable.</li>
 * <li>Skipping over records quickly to perform approximate pagination with
 * {@code READ_UNCOMMITTED} isolation.</li>
 * </ul>
 *
 * <p>For other isolation modes ({@code READ_COMMITTED}, {@code
 * REPEATABLE_READ} and {@code SERIALIZABLE}), the performance benefit is
 * not as significant.  In this case, the data item must be read into the JE
 * cache if it is not already present, in order to lock the record.  The only
 * performance benefit is that the data will not be copied from the JE cache to
 * the application's entry parameter.</p>
 *
 * <p>For information on specifying isolation modes, see {@link LockMode},
 * {@link CursorConfig} and {@link TransactionConfig}.</p>
 *
 * <p>The Partial property may also be used to retrieve or update only a
 * portion of a data item.  This avoids copying the entire record between the
 * JE cache and the application data parameter. However, this feature is not
 * currently fully optimized, since the entire record is always read or written
 * to the database, and the entire record is cached.  A partial update may
 * be performed only with {@link Cursor#putCurrent Cursor.putCurrent}.</p>
 *
 * <p>In limited cases, the Partial property may also be used to retrieve a
 * partial key item.  For example, a {@code DatabaseEntry} with a Partial
 * property may be passed to {@link #getNext getNext}.  However, in practice
 * this has limited value since the entire key is usually needed by the
 * application, and the benefit of copying a portion of the key is generally
 * very small.  Partial key items may not be passed to methods that use the key
 * as an input parameter, for example, {@link #getSearchKey getSearchKey}.  In
 * general, the usefulness of partial key items is very limited.</p>
 */
public class Cursor implements ForwardCursor {

    /*
     * A large number of retries are performed before giving up and reporting a
     * secondary integrity error.  The assumption is that corruption is rare.
     * With 10,000 retries and a 1 millis sleep time, a test operation retried
     * for 11 seconds before throwing an integrity exception.
     */
    static final int READ_PRIMARY_MAX_RETRIES = 10000;
    static final int SLEEP_BEFORE_READ_PRIMARY_RETRY = 1;

    /**
     * The underlying cursor.
     */
    CursorImpl cursorImpl; // Used by subclasses.

    /**
     * The CursorConfig used to configure this cursor.
     */
    CursorConfig config;

    /**
     * True if update operations are prohibited through this cursor.  Update
     * operations are prohibited if the database is read-only or:
     *
     * (1) The database is transactional,
     *
     * and
     *
     * (2) The user did not supply a txn to the cursor ctor (meaning, the
     * locker is non-transactional).
     */
    private boolean updateOperationsProhibited;

    /**
     * Handle under which this cursor was created; may be null when the cursor
     * is used internally.
     */
    private Database dbHandle;

    /**
     * Database implementation.
     */
    private DatabaseImpl dbImpl;

    /* Attributes */
    private boolean readUncommittedDefault;
    private boolean serializableIsolationDefault;

    private Logger logger;

    private boolean nonCloning = false;

    private CacheMode cacheMode;
    private boolean cacheModeOverridden;

    private RangeConstraint cursorRangeConstraint;

    /* User Transacational, or null if none. */
    private Transaction transaction;

    /**
     * Creates a cursor for a given user transaction with
     * retainNonTxnLocks=false.
     *
     * <p>If txn is null, a non-transactional cursor will be created that
     * releases locks for the prior operation when the next operation
     * succeeds.</p>
     */
    Cursor(final Database dbHandle,
           final Transaction txn,
           CursorConfig cursorConfig) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        if (dbHandle != null) {
            dbHandle.checkOpen("Can't access Database:");
        }

        /* Do not allow auto-commit when creating a user cursor. */
        Locker locker = LockerFactory.getReadableLocker
            (dbHandle.getEnvironment(),
             txn,
             dbHandle.isTransactional(),
             cursorConfig.getReadCommitted());

        init(dbHandle, dbHandle.getDatabaseImpl(), locker, cursorConfig,
             false /*retainNonTxnLocks*/);
    }

    /**
     * Creates a cursor for a given locker with retainNonTxnLocks=false.
     *
     * <p>If locker is null or is non-transactional, a non-transactional cursor
     * will be created that releases locks for the prior operation when the
     * next operation succeeds.</p>
     */
    Cursor(final Database dbHandle, Locker locker, CursorConfig cursorConfig) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        if (dbHandle != null) {
            dbHandle.checkOpen("Can't access Database:");
        }

        locker = LockerFactory.getReadableLocker
            (dbHandle.getEnvironment(),
             dbHandle,
             locker,
             cursorConfig.getReadCommitted());

        init(dbHandle, dbHandle.getDatabaseImpl(), locker, cursorConfig,
             false /*retainNonTxnLocks*/);
    }

    /**
     * Creates a cursor for a given locker and retainNonTxnLocks parameter.
     *
     * <p>The locker parameter must be non-null.  With this constructor, we use
     * the given locker and retainNonTxnLocks parameter without applying any
     * special rules for different lockers -- the caller must supply the
     * correct locker and retainNonTxnLocks combination.</p>
     */
    Cursor(final Database dbHandle,
           final Locker locker,
           CursorConfig cursorConfig,
           final boolean retainNonTxnLocks) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        if (dbHandle != null) {
            dbHandle.checkOpen("Can't access Database:");
        }

        init(dbHandle, dbHandle.getDatabaseImpl(), locker, cursorConfig,
             retainNonTxnLocks);
    }

    /**
     * Creates a cursor for internal use, without a Database handle.
     */
    Cursor(final DatabaseImpl databaseImpl,
           final Locker locker,
           CursorConfig cursorConfig,
           final boolean retainNonTxnLocks) {

        if (cursorConfig == null) {
            cursorConfig = CursorConfig.DEFAULT;
        }

        /* Check that Database is open for internal Cursor usage. */
        if (dbHandle != null) {
            dbHandle.checkOpen("Can't access Database:");
        }

        init(null /*dbHandle*/, databaseImpl, locker, cursorConfig,
             retainNonTxnLocks);
    }

    private void init(final Database dbHandle,
                      final DatabaseImpl databaseImpl,
                      final Locker locker,
                      final CursorConfig cursorConfig,
                      final boolean retainNonTxnLocks) {
        assert locker != null;

        /*
         * Allow locker to perform "open cursor" actions, such as consistency
         * checks for a non-transactional locker on a Replica.
         */
        try {
            locker.openCursorHook(databaseImpl);
        } catch (RuntimeException e) {
            locker.operationEnd();
            throw e;
        }

        cursorImpl = new CursorImpl(databaseImpl, locker, retainNonTxnLocks);

        transaction = locker.getTransaction();

        cacheMode = databaseImpl.getDefaultCacheMode();

        /* Perform eviction for user cursors. */
        cursorImpl.setAllowEviction(true);

        readUncommittedDefault =
            cursorConfig.getReadUncommitted() ||
            locker.isReadUncommittedDefault();

        serializableIsolationDefault =
            cursorImpl.getLocker().isSerializableIsolation();

        updateOperationsProhibited =
            (databaseImpl.isTransactional() && !locker.isTransactional()) ||
            (dbHandle != null && !dbHandle.isWritable());

        this.dbImpl = databaseImpl;
        if (dbHandle != null) {
            this.dbHandle = dbHandle;
            dbHandle.addCursor(this);
        }
        this.config = cursorConfig;
        this.logger = databaseImpl.getDbEnvironment().getLogger();
    }

    /**
     * Copy constructor.
     */
    Cursor(final Cursor cursor, final boolean samePosition) {
        readUncommittedDefault = cursor.readUncommittedDefault;
        serializableIsolationDefault = cursor.serializableIsolationDefault;
        updateOperationsProhibited = cursor.updateOperationsProhibited;

        cursorImpl = cursor.cursorImpl.dup(samePosition);
        dbImpl = cursor.dbImpl;
        dbHandle = cursor.dbHandle;
        if (dbHandle != null) {
            dbHandle.addCursor(this);
        }
        config = cursor.config;
        logger = dbImpl.getDbEnvironment().getLogger();
        cacheMode = cursor.cacheMode;
        cacheModeOverridden = cursor.cacheModeOverridden;
    }

    /**
     * Prevents this cursor from being cloned to perform an operation that
     * changes the cursor position.  [#13879]
     *
     * NonCloning is an optimization used for Database.get/put operations.
     * Normally cloning is used before an operation to allow use of the old
     * cursor position after the operation fails.  With the Database
     * operations, if an operation fails the cursor is simply discarded.
     *
     * NonCloning cursors are also used for internal operations.
     *
     * They can also be used to avoid deadlocks for non-transactional or
     * read-committed modes, because with a NonCloning cursor the lock at the
     * old position is released before getting the lock at the new position.
     * With a regular cursor, the lock at the old position is held until
     * getting the lock at the new position, meaning that two locks are held at
     * a time and there is a possibility of deadlocks.
     */
    void setNonCloning(final boolean nonCloning) {
        this.nonCloning = nonCloning;
    }

    /**
     * Internal entrypoint.
     */
    CursorImpl getCursorImpl() {
        return cursorImpl;
    }

    /**
     * Returns the Database handle associated with this Cursor.
     *
     * @return The Database handle associated with this Cursor.
     */
    public Database getDatabase() {
        return dbHandle;
    }

    /**
     * Always returns non-null, while getDatabase() returns null if no handle
     * is associated with this cursor.
     */
    DatabaseImpl getDatabaseImpl() {
        return dbImpl;
    }

    /**
     * Returns this cursor's configuration.
     *
     * <p>This may differ from the configuration used to open this object if
     * the cursor existed previously.</p>
     *
     * @return This cursor's configuration.
     */
    public CursorConfig getConfig() {
        try {
            return config.clone();
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the {@code CacheMode} used for subsequent operations performed
     * using this cursor.  If {@link #setCacheMode} has not been called with a
     * non-null value, the configured Database or Environment default is
     * returned.
     *
     * @return the {@code CacheMode} used for subsequent operations using
     * this cursor.
     *
     * @see #setCacheMode
     */
    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the {@code CacheMode} used for subsequent operations performed
     * using this cursor.  This method may be used to override the defaults
     * specified using {@link DatabaseConfig#setCacheMode} and {@link
     * EnvironmentConfig#setCacheMode}.
     *
     * @param cacheMode is the {@code CacheMode} used for subsequent operations
     * using this cursor, or null to configure the Database or Environment
     * default.
     *
     * @see CacheMode for further details.
     */
    public void setCacheMode(final CacheMode cacheMode) {
        cacheModeOverridden = (cacheMode != null);
        this.cacheMode =
            (cacheMode != null) ? cacheMode : dbImpl.getDefaultCacheMode();
    }

    /**
     * @hidden
     * For internal use only.
     *
     * A RangeConstraint is used by search-range and next/previous methods to
     * prevent keys that are not inside the range from being returned.
     *
     * This method is not yet part of the public API because it has not been
     * designed with future-proofing or generality in mind, and has not been
     * reviewed.
     */
    public void setRangeConstraint(RangeConstraint rangeConstraint) {
        cursorRangeConstraint = rangeConstraint;
    }

    private boolean checkRangeConstraint(final DatabaseEntry key) {
        assert key.getOffset() == 0;
        assert key.getData().length == key.getSize();

        if (cursorRangeConstraint == null) {
            return true;
        }

        return cursorRangeConstraint.inBounds(key.getData());
    }

    /**
     * Discards the cursor.
     *
     * <p>The cursor handle may not be used again after this method has been
     * called, regardless of the method's success or failure, with one
     * exception:  the {@code close} method itself may be called any number of
     * times.</p>
     *
     * <p>WARNING: To guard against memory leaks, the application should
     * discard all references to the closed handle.  While BDB makes an effort
     * to discard references from closed objects to the allocated memory for an
     * environment, this behavior is not guaranteed.  The safe course of action
     * for an application is to discard all references to closed BDB
     * objects.</p>
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public void close()
        throws DatabaseException {

        try {
            if (cursorImpl.isClosed()) {
                return;
            }

            /*
             * Do not call checkState here, to allow closing a cursor after an
             * operation failure.  [#17015]
             */
            checkEnv();
            cursorImpl.close();
            if (dbHandle != null) {
                dbHandle.removeCursor(this);
                dbHandle = null;
            }
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Returns a count of the number of data items for the key to which the
     * cursor refers.
     *
     * <p>If the database is configured for duplicates, the database is scanned
     * internally, without taking any record locks, to count the number of
     * non-deleted entries.  Although the internal scan is more efficient under
     * some conditions, the result is the same as if a cursor were used to
     * iterate over the entries using {@link LockMode#READ_UNCOMMITTED}.</p>
     *
     * <p>If the database is not configured for duplicates, the count returned
     * is always zero or one, depending on the record at the cursor position is
     * deleted or not.</p>
     *
     * <p>The cost of this method is directly proportional to the number of
     * records scanned.</p>
     *
     * @return A count of the number of data items for the key to which the
     * cursor refers.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     */
    public int count()
        throws DatabaseException {

        checkState(true);
        trace(Level.FINEST, "Cursor.count: ", null);

        return countInternal();
    }
  
    /**
     * Returns a rough estimate of the count of the number of data items for
     * the key to which the cursor refers.
     *
     * <p>If the database is configured for duplicates, a quick estimate of the
     * number of records is computed using information in the Btree.  Because
     * the Btree is unbalanced, in some cases the estimate may be off by a
     * factor of two or more.  The estimate is accurate when the number of
     * records is less than the configured {@link
     * DatabaseConfig#setNodeMaxEntries NodeMaxEntries}.</p>
     *
     * <p>If the database is not configured for duplicates, the count returned
     * is always zero or one, depending on the record at the cursor position is
     * deleted or not.</p>
     *
     * <p>The cost of this method is fixed, rather than being proportional to
     * the number of records scanned.  Because its accuracy is variable, this
     * method should normally be used when accuracy is not required, such as
     * for query optimization, and a fixed cost operation is needed. For
     * example, this method is used internally for determining the index
     * processing order in a {@link JoinCursor}.</p>
     *
     * @return an estimate of the count of the number of data items for the key
     * to which the cursor refers.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     */
    public long countEstimate()
        throws DatabaseException {

        checkState(true);
        trace(Level.FINEST, "Cursor.countEstimate: ", null);

        return countEstimateInternal();
    }

    /**
     * Returns a new cursor with the same transaction and locker ID as the
     * original cursor.
     *
     * <p>This is useful when an application is using locking and requires
     * two or more cursors in the same thread of control.</p>
     *
     * @param samePosition If true, the newly created cursor is initialized
     * to refer to the same position in the database as the original cursor
     * (if any) and hold the same locks (if any). If false, or the original
     * cursor does not hold a database position and locks, the returned
     * cursor is uninitialized and will behave like a newly created cursor.
     *
     * @return A new cursor with the same transaction and locker ID as the
     * original cursor.
     *
     * @throws com.sleepycat.je.rep.DatabasePreemptedException in a replicated
     * environment if the master has truncated, removed or renamed the
     * database.
     *
     * @throws OperationFailureException if this exception occurred earlier and
     * caused the transaction to be invalidated.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed.
     */
    public Cursor dup(final boolean samePosition)
        throws DatabaseException {

        try {
            checkState(false);
            return new Cursor(this, samePosition);
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Deletes the key/data pair to which the cursor refers.
     *
     * <p>When called on a cursor opened on a database that has been made into
     * a secondary index, this method the key/data pair from the primary
     * database and all secondary indices.</p>
     *
     * <p>The cursor position is unchanged after a delete, and subsequent calls
     * to cursor functions expecting the cursor to refer to an existing key
     * will fail.</p>
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     */
    public OperationStatus delete()
        throws LockConflictException,
               DatabaseException,
               UnsupportedOperationException  {

        checkState(true);
        checkUpdatesAllowed("delete");
        trace(Level.FINEST, "Cursor.delete: ", null);

        return deleteInternal(dbImpl.getRepContext());
    }

    /**
     * Stores a key/data pair into the database.
     *
     * <p>If the put method succeeds, the cursor is always positioned to refer
     * to the newly inserted item.  If the put method fails for any reason, the
     * state of the cursor will be unchanged.</p>
     *
     * <p>If the key already appears in the database and duplicates are
     * supported, the new data value is inserted at the correct sorted
     * location.  If the key already appears in the database and duplicates are
     * not supported, the data associated with the key will be replaced.</p>
     *
     * @param key the key {@link com.sleepycat.je.DatabaseEntry
     * DatabaseEntry} operated on.
     *
     * @param data the data {@link com.sleepycat.je.DatabaseEntry
     * DatabaseEntry} stored.
     *
     * @return an OperationStatus for the operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus put(final DatabaseEntry key,
                               final DatabaseEntry data)
        throws DatabaseException, UnsupportedOperationException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        DatabaseUtil.checkForPartialKey(key);
        checkUpdatesAllowed("put");
        trace(Level.FINEST, "Cursor.put: ", key, data, null);

        return putInternal(key, data, PutMode.OVERWRITE);
    }

    /**
     * Stores a key/data pair into the database.
     *
     * <p>If the putNoOverwrite method succeeds, the cursor is always
     * positioned to refer to the newly inserted item.  If the putNoOverwrite
     * method fails for any reason, the state of the cursor will be
     * unchanged.</p>
     *
     * <p>If the key already appears in the database, putNoOverwrite will
     * return {@link com.sleepycat.je.OperationStatus#KEYEXIST
     * OperationStatus.KEYEXIST}.</p>
     *
     * @param key the key {@link com.sleepycat.je.DatabaseEntry
     * DatabaseEntry} operated on.
     *
     * @param data the data {@link com.sleepycat.je.DatabaseEntry
     * DatabaseEntry} stored.
     *
     * @return an OperationStatus for the operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus putNoOverwrite(final DatabaseEntry key,
                                          final DatabaseEntry data)
        throws DatabaseException, UnsupportedOperationException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        DatabaseUtil.checkForPartialKey(key);
        checkUpdatesAllowed("putNoOverwrite");
        trace(Level.FINEST, "Cursor.putNoOverwrite: ", key, data, null);

        return putInternal(key, data, PutMode.NO_OVERWRITE);
    }

    /**
     * Stores a key/data pair into the database.
     *
     * <p>If the putNoDupData method succeeds, the cursor is always positioned
     * to refer to the newly inserted item.  If the putNoDupData method fails
     * for any reason, the state of the cursor will be unchanged.</p>
     *
     * <p>Insert the specified key/data pair into the database, unless a
     * key/data pair comparing equally to it already exists in the database.
     * If a matching key/data pair already exists in the database, {@link
     * com.sleepycat.je.OperationStatus#KEYEXIST OperationStatus.KEYEXIST} is
     * returned.</p>
     *
     * @param key the key {@link com.sleepycat.je.DatabaseEntry DatabaseEntry}
     * operated on.
     *
     * @param data the data {@link com.sleepycat.je.DatabaseEntry
     * DatabaseEntry} stored.
     *
     * @return an OperationStatus for the operation.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter, or
     * the database is read-only, or the database is not configured for
     * duplicates.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus putNoDupData(final DatabaseEntry key,
                                        final DatabaseEntry data)
        throws DatabaseException, UnsupportedOperationException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        DatabaseUtil.checkForPartialKey(key);
        checkUpdatesAllowed("putNoDupData");
        trace(Level.FINEST, "Cursor.putNoDupData: ", key, data, null);

        return putInternal(key, data, PutMode.NO_DUP_DATA);
    }

    /**
     * Replaces the data in the key/data pair at the current cursor position.
     *
     * <p>Whether the putCurrent method succeeds or fails for any reason, the
     * state of the cursor will be unchanged.</p>
     *
     * <p>Overwrite the data of the key/data pair to which the cursor refers
     * with the specified data item. This method will return
     * OperationStatus.NOTFOUND if the cursor currently refers to an
     * already-deleted key/data pair.</p>
     *
     * <p>For a database that does not support duplicates, the data may be
     * changed by this method.  If duplicates are supported, the data may be
     * changed only if a custom partial comparator is configured and the
     * comparator considers the old and new data to be equal (that is, the
     * comparator returns zero).  For more information on partial comparators
     * see {@link DatabaseConfig#setDuplicateComparator}.</p>
     *
     * <p>If the old and new data are unequal according to the comparator, a
     * {@link DuplicateDataException} is thrown.  Changing the data in this
     * case would change the sort order of the record, which would change the
     * cursor position, and this is not allowed.  To change the sort order of a
     * record, delete it and then re-insert it.</p>
     *
     * @param data - the data DatabaseEntry stored.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for partial data update.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws DuplicateDataException if the old and new data are not equal
     * according to the configured duplicate comparator or default comparator.
     *
     * @throws OperationFailureException if one of the <a
     * href="../je/OperationFailureException.html#writeFailures">Write
     * Operation Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws UnsupportedOperationException if the database is transactional
     * but this cursor was not opened with a non-null transaction parameter,
     * or the database is read-only.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus putCurrent(final DatabaseEntry data)
        throws DatabaseException, UnsupportedOperationException {

        checkState(true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
        checkUpdatesAllowed("putCurrent");
        trace(Level.FINEST, "Cursor.putCurrent: ", null, data, null);

        return putInternal(null /*key*/, data, PutMode.CURRENT);
    }

    /**
     * Returns the key/data pair to which the cursor refers.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getCurrent(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        try {
            checkState(true);
            checkArgsNoValRequired(key, data);
            trace(Level.FINEST, "Cursor.getCurrent: ", lockMode);

            return getCurrentInternal(key, data, lockMode);
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Moves the cursor to the first key/data pair of the database, and returns
     * that pair.  If the first key has duplicate values, the first data item
     * in the set of duplicates is returned.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getFirst(final DatabaseEntry key,
                                    final DatabaseEntry data,
                                    final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getFirst: ",lockMode);

        return position(key, data, lockMode, true);
    }

    /**
     * Moves the cursor to the last key/data pair of the database, and returns
     * that pair.  If the last key has duplicate values, the last data item in
     * the set of duplicates is returned.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getLast(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getLast: ", lockMode);

        return position(key, data, lockMode, false);
    }

    /**
     * Moves the cursor to the next key/data pair and returns that pair.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the first
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next key/data pair of the database, and that pair
     * is returned.  In the presence of duplicate key values, the value of the
     * key may not change.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getNext(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getNext: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, true);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.NEXT);
        }
    }

    /**
     * If the next key/data pair of the database is a duplicate data record for
     * the current key/data pair, moves the cursor to the next key/data pair of
     * the database and returns that pair.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getNextDup(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getNextDup: ", lockMode);

        return retrieveNext(key, data, lockMode, GetMode.NEXT_DUP);
    }

    /**
     * Moves the cursor to the next non-duplicate key/data pair and returns
     * that pair.  If the matching key has duplicate values, the first data
     * item in the set of duplicates is returned.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the first
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next non-duplicate key of the database, and that
     * key/data pair is returned.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getNextNoDup(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getNextNoDup: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, true);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.NEXT_NODUP);
        }
    }

    /**
     * Moves the cursor to the previous key/data pair and returns that pair.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the last
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the previous key/data pair of the database, and that
     * pair is returned. In the presence of duplicate key values, the value of
     * the key may not change.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getPrev(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getPrev: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, false);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.PREV);
        }
    }

    /**
     * If the previous key/data pair of the database is a duplicate data record
     * for the current key/data pair, moves the cursor to the previous key/data
     * pair of the database and returns that pair.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getPrevDup(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getPrevDup: ", lockMode);

        return retrieveNext(key, data, lockMode, GetMode.PREV_DUP);
    }

    /**
     * Moves the cursor to the previous non-duplicate key/data pair and returns
     * that pair.  If the matching key has duplicate values, the last data item
     * in the set of duplicates is returned.
     *
     * <p>If the cursor is not yet initialized, move the cursor to the last
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the previous non-duplicate key of the database, and
     * that key/data pair is returned.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getPrevNoDup(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, data);
        trace(Level.FINEST, "Cursor.getPrevNoDup: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, data, lockMode, false);
        } else {
            return retrieveNext(key, data, lockMode, GetMode.PREV_NODUP);
        }
    }

    /**
     * Skips forward a given number of key/data pairs and returns the number by
     * which the cursor is moved.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #getNext getNext} with {@link
     * LockMode#READ_UNCOMMITTED} to skip over the desired number of key/data
     * pairs, and then calling {@link #getCurrent getCurrent} with the {@code
     * lockMode} parameter to return the final key/data pair.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * key/value pairs using a smaller number of Btree operations.  When there
     * is no contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When there is contention
     * on BINs or fetching BINs is required, the scan is broken up into smaller
     * operations to avoid blocking other threads for long time periods.</p>
     *
     * <p>If the returned count is greater than zero, then the key/data pair at
     * the new cursor position is also returned.  If zero is returned, then
     * there are no key/value pairs that follow the cursor position and a
     * key/data pair is not returned.</p>
     *
     * <p>If this method fails for any reason or if zero is returned, the
     * position of the cursor will be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param maxCount the maximum number of key/data pairs to skip, i.e., the
     * maximum number by which the cursor should be moved; must be greater
     * than zero.
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return the number of key/data pairs skipped, i.e., the number by which
     * the cursor has moved; if zero is returned, the cursor position is
     * unchanged and the key/data pair is not returned.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public long skipNext(final long maxCount,
                         final DatabaseEntry key,
                         final DatabaseEntry data,
                         final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        if (maxCount <= 0) {
            throw new IllegalArgumentException("maxCount must be positive: " +
                                               maxCount);
        }
        trace(Level.FINEST, "Cursor.skipNext: ", lockMode);

        return skipInternal(maxCount, true /*forward*/, key, data, lockMode);
    }

    /**
     * Skips backward a given number of key/data pairs and returns the number
     * by which the cursor is moved.
     *
     * <p>Without regard to performance, calling this method is equivalent to
     * repeatedly calling {@link #getPrev getPrev} with {@link
     * LockMode#READ_UNCOMMITTED} to skip over the desired number of key/data
     * pairs, and then calling {@link #getCurrent getCurrent} with the {@code
     * lockMode} parameter to return the final key/data pair.</p>
     *
     * <p>With regard to performance, this method is optimized to skip over
     * key/value pairs using a smaller number of Btree operations.  When there
     * is no contention on the bottom internal nodes (BINs) and all BINs are in
     * cache, the number of Btree operations is reduced by roughly two orders
     * of magnitude, where the exact number depends on the {@link
     * EnvironmentConfig#NODE_MAX_ENTRIES} setting.  When there is contention
     * on BINs or fetching BINs is required, the scan is broken up into smaller
     * operations to avoid blocking other threads for long time periods.</p>
     *
     * <p>If the returned count is greater than zero, then the key/data pair at
     * the new cursor position is also returned.  If zero is returned, then
     * there are no key/value pairs that follow the cursor position and a
     * key/data pair is not returned.</p>
     *
     * <p>If this method fails for any reason or if zero is returned, the
     * position of the cursor will be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param maxCount the maximum number of key/data pairs to skip, i.e., the
     * maximum number by which the cursor should be moved; must be greater
     * than zero.
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return the number of key/data pairs skipped, i.e., the number by which
     * the cursor has moved; if zero is returned, the cursor position is
     * unchanged and the key/data pair is not returned.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the cursor is uninitialized (not positioned on a record), or the
     * non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public long skipPrev(final long maxCount,
                         final DatabaseEntry key,
                         final DatabaseEntry data,
                         final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        if (maxCount <= 0) {
            throw new IllegalArgumentException("maxCount must be positive: " +
                                               maxCount);
        }
        trace(Level.FINEST, "Cursor.skipPrev: ", lockMode);

        return skipInternal(maxCount, false /*forward*/, key, data, lockMode);
    }

    private long skipInternal(final long maxCount,
                              final boolean forward,
                              final DatabaseEntry key,
                              final DatabaseEntry data,
                              final LockMode lockMode) {
        final LockType lockType = getLockType(lockMode, false);
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            while (true) {
                final CursorImpl dup = beginMoveCursor(true);
                boolean success = false;
                try {
                    final long count = dup.skip(forward, maxCount,
                                                null /*rangeConstraint*/);
                    if (count <= 0) {
                        return 0;
                    }
                    final OperationStatus status =
                        getCurrentWithCursorImpl(dup, key, data, lockType);
                    if (status == OperationStatus.KEYEMPTY) {
                        /* Retry if deletion occurs while unlatched. */
                        continue;
                    }
                    success = true;
                    return count;
                } finally {
                    endMoveCursor(dup, success);
                }
            }
        }
    }

    /**
     * Moves the cursor to the given key of the database, and returns the datum
     * associated with the given key.  If the matching key has duplicate
     * values, the first data item in the set of duplicates is returned.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key used as input.  It must be initialized with a
     * non-null byte array by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchKey(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "Cursor.getSearchKey: ", key, null, lockMode);

        return search(key, data, lockMode, SearchMode.SET);
    }

    /**
     * Moves the cursor to the closest matching key of the database, and
     * returns the data item associated with the matching key.  If the matching
     * key has duplicate values, the first data item in the set of duplicates
     * is returned.
     *
     * <p>The returned key/data pair is for the smallest key greater than or
     * equal to the specified key (as determined by the key comparison
     * function), permitting partial key matches and range searches.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key used as input and returned as output.  It must be
     * initialized with a non-null byte array by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     *
     * @param lockMode the locking attributes; if null, default attributes
     * are used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchKeyRange(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "Cursor.getSearchKeyRange: ", key, null, lockMode);

        return search(key, data, lockMode, SearchMode.SET_RANGE);
    }

    /**
     * Moves the cursor to the specified key/data pair, where both the key and
     * data items must match.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key used as input.  It must be initialized with a
     * non-null byte array by the caller.
     *
     * @param data the data used as input.  It must be initialized with a
     * non-null byte array by the caller.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchBoth(final DatabaseEntry key,
                                         final DatabaseEntry data,
                                         final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsValRequired(key, data);
        trace(Level.FINEST, "Cursor.getSearchBoth: ", key, data, lockMode);

        return search(key, data, lockMode, SearchMode.BOTH);
    }

    /**
     * Moves the cursor to the specified key and closest matching data item of
     * the database.
     *
     * <p>In the case of any database supporting sorted duplicate sets, the
     * returned key/data pair is for the smallest data item greater than or
     * equal to the specified data item (as determined by the duplicate
     * comparison function), permitting partial matches and range searches in
     * duplicate data sets.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the key used as input and returned as output.  It must be
     * initialized with a non-null byte array by the caller.
     *
     * @param data the data used as input and returned as output.  It must be
     * initialized with a non-null byte array by the caller.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#NOTFOUND
     * OperationStatus.NOTFOUND} if no matching key/data pair is found;
     * otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
     * OperationStatus.SUCCESS}.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     *
     * @throws IllegalStateException if the cursor or database has been closed,
     * or the non-transactional cursor was created in a different thread.
     *
     * @throws IllegalArgumentException if an invalid parameter is specified.
     */
    public OperationStatus getSearchBothRange(final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsValRequired(key, data);
        trace(Level.FINEST, "Cursor.getSearchBothRange: ", key, data,
              lockMode);

        return search(key, data, lockMode, SearchMode.BOTH_RANGE);
    }

    /**
     * @hidden
     * For internal use only.
     *
     * Returns the record following all records having the given key prefix.
     *
     * This method is not yet part of the public API because it has not been
     * designed with future-proofing or generality in mind, and has not been
     * reviewed.
     */
    public OperationStatus getNextAfterPrefix(final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final LockMode lockMode) {
        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "Cursor.getNextAfterPrefix: ", key, null,
              lockMode);

        final Comparator<byte[]> searchComparator = new Comparator<byte[]>() {
            public int compare(final byte[] prefix, final byte[] checkKey) {
                final int prefixLen = prefix.length;
                final int checkLen = Math.min(checkKey.length, prefixLen);
                final Comparator<byte[]> btreeComparator =
                    dbImpl.getBtreeComparator();
                final int cmp;
                if (btreeComparator == null) {
                    cmp = Key.compareUnsignedBytes(prefix, 0, prefixLen,
                                                   checkKey, 0, checkLen);
                } else {
                    final byte[] key2 = new byte[checkLen];
                    System.arraycopy(checkKey, 0, key2, 0, checkLen);
                    cmp = btreeComparator.compare(prefix, key2);
                }
                return (cmp != 0) ? cmp : 1;
            }
        };

        OperationStatus status = OperationStatus.NOTFOUND;
        final LockType lockType = getLockType(lockMode, false);
        final CursorImpl dup =
            beginMoveCursor(false /* searchAndPosition will add cursor */);
        try {
            final KeyChangeStatus result =
                searchInternal(dup, key, data,
                               lockType /*searchLockType*/,
                               lockType /*advanceLockType*/,
                               SearchMode.SET_RANGE,
                               searchComparator,
                               cursorRangeConstraint);
            status = result.status;
        } finally {
            endMoveCursor(dup, status == OperationStatus.SUCCESS);
        }
        return status;
    }

    /**
     * Counts duplicates without parameter checking.  No need to dup the cursor
     * because we never change the position.
     */
    int countInternal() {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            if (dbImpl.getSortedDuplicates()) {
                return countHandleDups();
            }
            return countNoDups();
        }
    }

    /**
     * Estimates duplicate count without parameter checking.  No need to dup
     * the cursor because we never change the position.
     */
    long countEstimateInternal() {
        if (dbImpl.getSortedDuplicates()) {
            return countEstimateHandleDups();
        }
        return countNoDups();
    }

    /**
     * When there are no duplicates, the count is either 0 or 1, and is very
     * cheap to determine.
     */
    private int countNoDups() {
        try {
            beginUseExistingCursor();
            final OperationStatus status = cursorImpl.getCurrent
                (null /*foundKey*/, null /*foundData*/, LockType.NONE);
            endUseExistingCursor();
            return (status == OperationStatus.SUCCESS) ? 1 : 0;
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Internal version of delete() that does no parameter checking.  Calls
     * deleteNoNotify() and notifies triggers (performs secondary updates).
     */
    OperationStatus deleteInternal(final ReplicationContext repContext) {
        try {
            /* Get existing data if updating secondaries. */
            DatabaseEntry oldKey = null;
            DatabaseEntry oldData = null;
            final boolean doNotifyTriggers =
                dbHandle != null && dbHandle.hasTriggers();
            final boolean hasUserTriggers =
                dbImpl != null && (dbImpl.getTriggers() != null);
            if (doNotifyTriggers || hasUserTriggers) {
                oldKey = new DatabaseEntry();
                oldData = new DatabaseEntry();
                final OperationStatus status =
                    getCurrentInternal(oldKey, oldData, LockMode.RMW);
                if (status != OperationStatus.SUCCESS) {
                    return OperationStatus.KEYEMPTY;
                }
            }

            /*
             * Notify triggers before the actual deletion so that a primary
             * record never exists while secondary keys refer to it.  This is
             * relied on by secondary read-uncommitted.
             */
            if (doNotifyTriggers) {
                dbHandle.notifyTriggers(cursorImpl.getLocker(),
                                        oldKey, oldData, null);
            }

            /* The actual deletion. */
            OperationStatus status = deleteNoNotify(repContext);

            if ((status == OperationStatus.SUCCESS) && hasUserTriggers) {
                // TODO: Mark, do we need "pre" triggers for our indexes
                // as above?
                TriggerManager.runDeleteTriggers(cursorImpl.getLocker(),
                                                 dbImpl, oldKey, oldData);
            }
            return status;
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Delete at current position.   Does not notify triggers (does not perform
     * secondary updates).
     */
    OperationStatus deleteNoNotify(final ReplicationContext repContext) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
         
            /*
             * No need to use a dup cursor, since this operation does not
             * change the cursor position.
             */
            beginUseExistingCursor();
            final OperationStatus status = cursorImpl.delete(repContext);
            endUseExistingCursor();
            return status;
       }
    }

    /**
     * Internal version of put that does no parameter checking.  Interprets
     * duplicates, notifies triggers, and prevents phantoms.
     */
    OperationStatus putInternal(final DatabaseEntry key,
                                final DatabaseEntry data,
                                final PutMode putMode) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            if (dbImpl.getSortedDuplicates()) {
                return putHandleDups(key, data, putMode);
            }
            if (putMode == PutMode.NO_DUP_DATA) {
                throw new UnsupportedOperationException
                    ("Database is not configured for duplicate data.");
            }
            return putNoDups(key, data, putMode);
        }
    }

    /**
     * Version of putInternal that allows passing an existing LN and does not
     * interpret duplicates.  Used for replication stream replay.  Notifies
     * triggers and prevents phantoms.
     */
    OperationStatus putForReplay(final byte[] key,
                                 final LN ln,
                                 final PutMode putMode,
                                 final ReplicationContext repContext) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            assert putMode != PutMode.CURRENT;
            return putNotify(new DatabaseEntry(key),
                             new DatabaseEntry(ln.getData()),
                             ln, putMode, repContext);
        }
    }

    /**
     * Does not interpret duplicates.
     */
    private OperationStatus
        putNoDups(final DatabaseEntry key,
                  final DatabaseEntry data,
                  final PutMode putMode) {
        final LN ln = (putMode == PutMode.CURRENT) ?
            null :
            LN.makeLN(dbImpl.getDbEnvironment(), data);
        return putNotify(key, data, ln, putMode, dbImpl.getRepContext());
    }

    /**
     * This single method is used for all put operations in order to notify
     * triggers (perform secondary updates) in one place.  Prevents phantoms.
     * Does not interpret duplicates.
     *
     * WARNING: When the cursor has no Database handle, which is true when
     * called from the replication replayer, this method notifies user triggers
     * but not secondary triggers.  This is correct for replication because
     * secondary updates are part of the replication stream.  However, it is
     * fragile because other operations, when no Database handle is used, will
     * not notify secondary triggers.  This isn't currently a problem because
     * a Database handle is present for all user operations.  But it is fragile
     * and needs work.
     *
     * @param data must be null if putMode is not CURRENT and vice versa.
     *
     * @param ln must be null if putMode is CURRENT and vice versa.
     */
    private OperationStatus
        putNotify(DatabaseEntry key,
                  final DatabaseEntry data,
                  final LN ln,
                  final PutMode putMode,
                  final ReplicationContext repContext) {
        try {
            /* Need to get old and new data if updating secondaries. */
            DatabaseEntry oldData = null;
            DatabaseEntry newData = null;
            DatabaseEntry returnNewData = null;
            final boolean doNotifyTriggers =
                dbHandle != null && dbHandle.hasTriggers();
            final boolean hasUserTriggers =
                dbImpl != null &&  (dbImpl.getTriggers() != null);

            if (doNotifyTriggers || hasUserTriggers) {
                if (data.getPartial()) {
                    /* Return a copy after resolving partial data. [#16932] */
                    returnNewData = new DatabaseEntry();
                    newData = returnNewData;
                } else {
                    /* No copy needed if partial data is not used. */
                    newData = data;
                }
                /* Data is returned for an existing record. */
                oldData = new DatabaseEntry();
            }

            /* Perform the put operation. */
            final OperationStatus commitStatus;
            if (putMode == PutMode.CURRENT) {
                assert ln == null;
                final byte[] replaceKey =
                    (key != null) ? Key.makeKey(key) : null;
                if (doNotifyTriggers && key == null) {
                    /* Key is returned by CursorImpl.putCurrent. */
                    key = new DatabaseEntry();
                }
                commitStatus = putCurrentNoNotify
                    (replaceKey, data, key, oldData, returnNewData,
                     repContext);
            } else {
                assert ln != null;
                commitStatus = putNoNotify
                    (key, data, ln, putMode, oldData, returnNewData,
                     repContext);
            }

            /* Notify triggers (update secondaries). */
            if (commitStatus == OperationStatus.SUCCESS) {
                if (doNotifyTriggers || hasUserTriggers) {

                    if (oldData != null && oldData.getData() == null) {
                        oldData = null;
                    }

                    if (newData != null && newData.getData() == null) {
                        newData = null;
                    }

                    if (doNotifyTriggers) {
                        /* Pass newData, since data may be partial. */
                        dbHandle.notifyTriggers(cursorImpl.getLocker(), key,
                                                oldData, newData);
                    }

                    if (hasUserTriggers) {
                        TriggerManager.runPutTriggers(cursorImpl.getLocker(),
                                                      dbImpl,
                                                      key,
                                                      oldData, newData);
                    }
                }
            }

            return commitStatus;
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Performs the put operation but does not notify triggers (does not
     * perform secondary updates).  Prevents phantoms.
     *
     * @param data is a separate param only to support partial data for user
     * operations.
     *
     * @param ln is normally a new LN node that is created for insertion, and
     * will be discarded if an update occurs.  However, HA will pass an
     * existing node.
     *
     * @param putMode is required. PutMode.CURRENT is not allowed -- use
     * putCurrentNoNotify instead.
     */
    private OperationStatus
        putNoNotify(final DatabaseEntry key,
                    final DatabaseEntry data,
                    final LN ln,
                    final PutMode putMode,
                    final DatabaseEntry returnOldData,
                    final DatabaseEntry returnNewData,
                    final ReplicationContext repContext) {
        assert key != null;
        assert ln != null;
        assert putMode != null;
        assert putMode != PutMode.CURRENT;

        Locker nextKeyLocker = null;
        CursorImpl nextKeyCursor = null;
        try {
            /* If other transactions are serializable, lock the next key. */
            Locker cursorLocker = cursorImpl.getLocker();
            if (dbImpl.getDbEnvironment().
                getTxnManager().
                areOtherSerializableTransactionsActive(cursorLocker)) {
                nextKeyLocker = BuddyLocker.createBuddyLocker
                    (dbImpl.getDbEnvironment(), cursorLocker);
                nextKeyCursor = new CursorImpl(dbImpl, nextKeyLocker);
                /* Perform eviction for user cursors. */
                nextKeyCursor.setAllowEviction(true);
                nextKeyCursor.lockNextKeyForInsert(key);
            }

            /* Perform the put operation. */
            return putAllowPhantoms
                (key, data, ln, putMode, returnOldData, returnNewData,
                 nextKeyCursor, repContext);
        } finally {
            /* Release the next-key lock. */
            if (nextKeyCursor != null) {
                nextKeyCursor.close();
            }
            if (nextKeyLocker != null) {
                nextKeyLocker.operationEnd();
            }
        }
    }

    /**
     * Clones the cursor, put key/data according to PutMode, and if successful,
     * swap cursors.  Does not notify triggers (does not perform secondary
     * updates).  Does not prevent phantoms.
     *
     * @param nextKeyCursor is the cursor used to lock the next key during
     * phantom prevention.  If this cursor is non-null and initialized, it's
     * BIN will be used to initialize the dup cursor used to perform insertion.
     * This enables an optimization in Tree.findBinForInsert that skips the
     * search for the BIN.
     */
    private OperationStatus
        putAllowPhantoms(final DatabaseEntry key,
                         final DatabaseEntry data,
                         final LN ln,
                         final PutMode putMode,
                         final DatabaseEntry returnOldData,
                         final DatabaseEntry returnNewData,
                         final CursorImpl nextKeyCursor,
                         final ReplicationContext repContext) {
        OperationStatus status = OperationStatus.NOTFOUND;

        /*
         * Do not call addCursor when inserting.  Copy the position of
         * nextKeyCursor if available.
         */
        final CursorImpl dup = beginMoveCursor(false, nextKeyCursor);
        try {
            /* Perform operation. */
            status = dup.put(key, data, ln, putMode, returnOldData,
                             returnNewData, repContext);
            /* Note that status is used in the finally. */
            return status;
        } finally {
            endMoveCursor(dup, status == OperationStatus.SUCCESS);
        }
    }

    /**
     * Update the data at the current position.  No new LN, dup cursor, or
     * phantom handling is needed.  Does not interpret duplicates.
     */
    private OperationStatus
        putCurrentNoNotify(final byte[] replaceKey,
                           final DatabaseEntry replaceData,
                           final DatabaseEntry returnKey,
                           final DatabaseEntry returnOldData,
                           final DatabaseEntry returnNewData,
                           final ReplicationContext repContext) {
        assert replaceData != null;
        beginUseExistingCursor();
        final OperationStatus status = cursorImpl.putCurrent
            (replaceKey, replaceData, returnKey, returnOldData, returnNewData,
             repContext);
        endUseExistingCursor();
        return status;
    }

    /**
     * Internal version of getFirst/getLast that does no parameter checking.
     * Interprets duplicates.
     */
    OperationStatus position(final DatabaseEntry key,
                             final DatabaseEntry data,
                             final LockMode lockMode,
                             final boolean first) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            if (dbImpl.getSortedDuplicates()) {
                return positionHandleDups(key, data, lockMode, first);
            }
            return positionNoDups(key, data, lockMode, first);
        }
    }

    /**
     * Does not interpret duplicates.  Prevents phantoms.
     */
    private OperationStatus positionNoDups(final DatabaseEntry key,
                                           final DatabaseEntry data,
                                           final LockMode lockMode,
                                           final boolean first) {
        try {
            if (!isSerializableIsolation(lockMode)) {
                return positionAllowPhantoms
                    (key, data, getLockType(lockMode, false), first);
            }

            /*
             * Perform range locking to prevent phantoms and handle restarts.
             */
            while (true) {
                try {
                    /* Range lock the EOF node before getLast. */
                    if (!first) {
                        cursorImpl.lockEof(LockType.RANGE_READ);
                    }

                    /* Use a range lock for getFirst. */
                    final LockType lockType = getLockType(lockMode, first);

                    /* Perform operation. */
                    final OperationStatus status =
                        positionAllowPhantoms(key, data, lockType, first);

                    /*
                     * Range lock the EOF node when getFirst returns NOTFOUND.
                     */
                    if (first && status != OperationStatus.SUCCESS) {
                        cursorImpl.lockEof(LockType.RANGE_READ);
                    }

                    return status;
                } catch (RangeRestartException e) {
                    continue;
                }
            }
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Positions without preventing phantoms.
     */
    private OperationStatus positionAllowPhantoms(final DatabaseEntry key,
                                                  final DatabaseEntry data,
                                                  final LockType lockType,
                                                  final boolean first) {
        assert (key != null && data != null);

        OperationStatus status = OperationStatus.NOTFOUND;

        /*
         * Pass false: no need to call addCursor here because
         * CursorImpl.position will be adding it after it finds the bin.
         */
        final CursorImpl dup = beginMoveCursor(false);
        try {
            /* Search for first or last. */
            if (!dup.positionFirstOrLast(first)) {
                /* Tree is empty. */
                status = OperationStatus.NOTFOUND;
                assert LatchSupport.countLatchesHeld() == 0:
                    LatchSupport.latchesHeldToString();
            } else {
                /* Found something in this tree. */
                assert LatchSupport.countLatchesHeld() == 1:
                    LatchSupport.latchesHeldToString();

                status = dup.getCurrentAlreadyLatched(key, data, lockType);
                if (status != OperationStatus.SUCCESS) {
                    /* The record we're pointing at may be deleted. */
                    status = dup.getNext(key, data, lockType, first, false,
                                         null /*rangeConstraint*/);
                }
            }
        } finally {

            /*
             * positionFirstOrLast returns with the target BIN latched, so it
             * is the responsibility of this method to make sure the latches
             * are released.
             */
            cursorImpl.releaseBIN();
            endMoveCursor(dup, status == OperationStatus.SUCCESS);
        }
        return status;
    }

    /**
     * Performs search by key, data, or both.  Prevents phantoms.
     */
    OperationStatus search(final DatabaseEntry key,
                           final DatabaseEntry data,
                           final LockMode lockMode,
                           final SearchMode searchMode) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            if (dbImpl.getSortedDuplicates()) {
                return searchHandleDups(key, data, lockMode, searchMode);
            }
            return searchNoDups(key, data, lockMode, searchMode,
                                null /*searchComparator*/);
        }
    }

    /**
     * Version of search that does not interpret duplicates.  Used for
     * replication stream replay.  Notifies triggers and prevents phantoms.
     */
    OperationStatus searchForReplay(final DatabaseEntry key,
                                    final DatabaseEntry data,
                                    final LockMode lockMode,
                                    final SearchMode searchMode) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            return searchNoDups(key, data, lockMode, searchMode,
                                null /*searchComparator*/);
        }
    }

    /**
     * Does not interpret duplicates.  Prevents phantoms.
     */
    private OperationStatus searchNoDups
        (final DatabaseEntry key,
         final DatabaseEntry data,
         final LockMode lockMode,
         final SearchMode searchMode,
         final Comparator<byte[]> searchComparator) {

        try {
            if (!isSerializableIsolation(lockMode)) {
                final LockType lockType = getLockType(lockMode, false);
                final KeyChangeStatus result = searchAllowPhantoms
                    (key, data, lockType, lockType, searchMode,
                     searchComparator, cursorRangeConstraint);
                return result.status;
            }

            /*
             * Perform range locking to prevent phantoms and handle restarts.
             */
            while (true) {
                try {
                    /* Do not use a range lock for the initial search. */
                    final LockType searchLockType =
                        getLockType(lockMode, false);

                    /* Switch to a range lock when advancing forward. */
                    final LockType advanceLockType =
                        getLockType(lockMode, true);

                    /* Do not modify key/data params until SUCCESS. */
                    final DatabaseEntry tryKey = cloneEntry(key);
                    final DatabaseEntry tryData = cloneEntry(data);
                    final KeyChangeStatus result;

                    if (searchMode.isExactSearch()) {

                        /*
                         * Artificial range search to range lock the next key.
                         */
                        result = searchExactAndRangeLock
                            (tryKey, tryData, searchLockType, advanceLockType,
                             searchMode, searchComparator);
                    } else {
                        /* Normal range search with null rangeConstraint. */
                        result = searchAllowPhantoms
                            (tryKey, tryData, searchLockType, advanceLockType,
                             searchMode, searchComparator,
                             null /*rangeConstraint*/);

                        /* Lock the EOF node if no records follow the key. */
                        if (result.status != OperationStatus.SUCCESS) {
                            cursorImpl.lockEof(LockType.RANGE_READ);
                        }

                        /* Finally check rangeConstraint. */
                        if (result.status == OperationStatus.SUCCESS &&
                            !checkRangeConstraint(tryKey)) {
                            result.status = OperationStatus.NOTFOUND;
                        }
                    }

                    /*
                     * Only overwrite key/data on SUCCESS, after all locking.
                     */
                    if (result.status == OperationStatus.SUCCESS) {
                        copyEntry(tryKey, key);
                        copyEntry(tryData, data);
                    }

                    return result.status;
                } catch (RangeRestartException e) {
                    continue;
                }
            }
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * For an exact search, performs a range search and returns NOTFOUND if the
     * key changes (or if the data changes for BOTH) during the search.
     * If no exact match is found the range search will range lock the
     * following key for phantom prevention.  Importantly, the cursor position
     * is not changed if an exact match is not found, even though we advance to
     * the following key in order to range lock it.
     */
    private KeyChangeStatus
        searchExactAndRangeLock(final DatabaseEntry key,
                                final DatabaseEntry data,
                                final LockType searchLockType,
                                final LockType advanceLockType,
                                final SearchMode searchMode,
                                final Comparator<byte[]> searchComparator) {
        KeyChangeStatus result = null;
        DatabaseEntry origData = new DatabaseEntry
            (data.getData(), data.getOffset(), data.getSize());

        final CursorImpl dup =
            beginMoveCursor(false /* searchAndPosition will add cursor */);

        try {

            /*
             * Perform a range search and return NOTFOUND if an exact match is
             * not found.
             */
            result = searchInternal
                (dup, key, data, searchLockType, advanceLockType,
                 SearchMode.SET_RANGE, searchComparator,
                 null /*rangeConstraint*/);

            /* If the key changed, then we do not have an exact match. */
            if (result.keyChange && result.status == OperationStatus.SUCCESS) {
                result.status = OperationStatus.NOTFOUND;
            }
            /* Check for data match. */
            if (!checkDataMatch(searchMode, origData, data)) {
                result.status = OperationStatus.NOTFOUND;
            }
        } finally {
            endMoveCursor(dup, result != null &&
                               result.status == OperationStatus.SUCCESS);
        }

        /*
         * Lock the EOF node if there was no exact match and we did not
         * range-lock the next record.
         */
        if (result.status != OperationStatus.SUCCESS && !result.keyChange) {
            cursorImpl.lockEof(LockType.RANGE_READ);
        }

        return result;
    }

    /**
     * Performs search without preventing phantoms.
     */
    private KeyChangeStatus
        searchAllowPhantoms(final DatabaseEntry key,
                            final DatabaseEntry data,
                            final LockType searchLockType,
                            final LockType advanceLockType,
                            final SearchMode searchMode,
                            final Comparator<byte[]> searchComparator,
                            final RangeConstraint rangeConstraint) {
        OperationStatus status = OperationStatus.NOTFOUND;
        DatabaseEntry origData = new DatabaseEntry
            (data.getData(), data.getOffset(), data.getSize());

        final CursorImpl dup =
            beginMoveCursor(false /* searchAndPosition will add cursor */);

        try {
            final KeyChangeStatus result = searchInternal
                (dup, key, data, searchLockType, advanceLockType, searchMode,
                 searchComparator, rangeConstraint);

            /* Check for data match. */
            if (!checkDataMatch(searchMode, origData, data)) {
                result.status = OperationStatus.NOTFOUND;
            }
            status = result.status;
            /* Note that status is used in the finally. */
            return result;
        } finally {
            endMoveCursor(dup, status == OperationStatus.SUCCESS);
        }
    }

    /**
     * For a non-duplicates database, the data must match exactly when
     * getSearchBoth or getSearchBothRange is called.
     */
    private boolean checkDataMatch(SearchMode searchMode,
                                   DatabaseEntry data1,
                                   DatabaseEntry data2) {
        if (!searchMode.isDataSearch()) {
            return true;
        }
        final int size1 = data1.getSize();
        final int size2 = data2.getSize();
        if (size1 != size2) {
            return false;
        }
        return Key.compareUnsignedBytes
            (data1.getData(), data1.getOffset(), size1,
             data2.getData(), data2.getOffset(), size2) == 0;
    }

    /**
     * Holder for an OperationStatus and a keyChange flag.  Is used for search.
     */
    private static class KeyChangeStatus {

        /**
         * Operation status;
         */
        public OperationStatus status;

        /**
         * Whether a SET_RANGE operation moved to key past the specified
         * match key.
         */
        public boolean keyChange;

        public KeyChangeStatus(OperationStatus status, boolean keyChange) {
            this.status = status;
            this.keyChange = keyChange;
        }
    }

    /**
     * Performs search for a given CursorImpl.
     */
    private KeyChangeStatus
        searchInternal(final CursorImpl dup,
                       final DatabaseEntry key,
                       final DatabaseEntry data,
                       final LockType searchLockType,
                       final LockType advanceLockType,
                       final SearchMode searchModeParam,
                       final Comparator<byte[]> searchComparator,
                       final RangeConstraint rangeConstraint) {
        assert key != null && data != null;

        final SearchMode searchMode;
        switch (searchModeParam) {
        case BOTH:
            searchMode = SearchMode.SET;
            break;
        case BOTH_RANGE:
            searchMode = SearchMode.SET_RANGE;
            break;
        default:
            searchMode = searchModeParam;
            break;
        }
        assert rangeConstraint == null || searchMode == SearchMode.SET_RANGE;
        OperationStatus status = OperationStatus.NOTFOUND;
        boolean keyChange = false;

        try {
            /* search */
            final int searchResult = dup.searchAndPosition
                (key, searchMode, searchLockType, searchComparator);
            if ((searchResult & CursorImpl.FOUND) != 0) {

                /*
                 * The search found a possibly valid record.
                 * CursorImpl.searchAndPosition's job is to settle the cursor
                 * at a particular location on a BIN. In some cases, the
                 * current position may not actually hold a valid record, so
                 * it's this layer's responsiblity to judge if it might need to
                 * bump the cursor along and search more. For example, we might
                 * have to do so if the position holds a deleted record.
                 *
                 * Note that searchResult has three bits possibly set:
                 *
                 * FOUND has already been checked above.
                 *
                 * EXACT_KEY means an exact match on the key portion was made.
                 *
                 * FOUND_LAST means that the cursor is positioned at the last
                 * record in the database.
                 */
                final boolean exactKeyMatch =
                    ((searchResult & CursorImpl.EXACT_KEY) != 0);
                final boolean foundLast =
                    ((searchResult & CursorImpl.FOUND_LAST) != 0);

                /*
                 * If rangeMatch is true this means that SET_RANGE was
                 * specified and there wasn't an exact match.
                 */
                boolean rangeMatch =
                   (searchMode == SearchMode.SET_RANGE && !exactKeyMatch);

                /*
                 * Pass null for key to getCurrentAlreadyLatched if searchMode
                 * is SET since the key is not supposed to be returned.
                 */
                final DatabaseEntry useKey =
                    (searchModeParam == SearchMode.SET) ?
                    null : key;

                /*
                 * If rangeMatch is true, then cursor is currently on some
                 * entry, but that entry is prior to the target key.  It is
                 * also possible that rangeMatch is false (we have an exact
                 * match) but the entry is deleted.  So we test for rangeMatch
                 * or a deleted entry, and if either is true then we advance to
                 * the next non-deleted entry.
                 */
                if (rangeMatch ||
                    (status = dup.getCurrentAlreadyLatched
                     (useKey, data, searchLockType)) ==
                    OperationStatus.KEYEMPTY) {

                    if (foundLast) {
                        /* Cannot advance past last record. */
                        status = OperationStatus.NOTFOUND;
                    } else if (searchMode == SearchMode.SET) {

                        /*
                         * SET is an exact operation and we should not advance
                         * past a deleted record.  However, this API should
                         * return NOTFOUND instead of KEYEMPTY (which must have
                         * been set above).
                         */
                        assert status == OperationStatus.KEYEMPTY;
                        status = OperationStatus.NOTFOUND;
                    } else {
                        assert searchMode == SearchMode.SET_RANGE;

                        /*
                         * This may be a deleted record or a rangeMatch, and in
                         * either case we should advance.  The key changes when
                         * we advance.
                         */
                        status = dup.getNext
                            (key, data, advanceLockType, true, rangeMatch,
                             rangeConstraint);
                        keyChange = (status == OperationStatus.SUCCESS);
                    }
                }
            }
        } finally {

            /*
             * searchAndPosition returns with the target BIN latched, so it is
             * the responsibility of this method to make sure the latches are
             * released.
             */
            cursorImpl.releaseBIN();
            if (status != OperationStatus.SUCCESS && dup != cursorImpl) {
                dup.releaseBIN();
            }
        }

        return new KeyChangeStatus(status, keyChange);
    }

    /**
     * Retrieves the next or previous record.  Prevents phantoms.
     */
    OperationStatus retrieveNext(final DatabaseEntry key,
                                 final DatabaseEntry data,
                                 final LockMode lockMode,
                                 final GetMode getMode) {
        if (dbImpl.getSortedDuplicates()) {
            return retrieveNextHandleDups(key, data, lockMode, getMode);
        }
        return retrieveNextNoDups(key, data, lockMode, getMode);
    }

    /**
     * Does not interpret duplicates.  Prevents phantoms.
     */
    private OperationStatus retrieveNextNoDups(final DatabaseEntry key,
                                               final DatabaseEntry data,
                                               final LockMode lockMode,
                                               final GetMode getModeParam) {
        final GetMode getMode;
        switch (getModeParam) {
        case NEXT_DUP:
        case PREV_DUP:
            return OperationStatus.NOTFOUND;
        case NEXT_NODUP:
            getMode = GetMode.NEXT;
            break;
        case PREV_NODUP:
            getMode = GetMode.PREV;
            break;
        default:
            getMode = getModeParam;
        }
        try {
            if (!isSerializableIsolation(lockMode)) {
                return retrieveNextAllowPhantoms
                    (key, data, getLockType(lockMode, false), getMode,
                     cursorRangeConstraint);
            }

            /*
             * Perform range locking to prevent phantoms and handle restarts.
             */
            while (true) {
                try {
                    /* Get a range lock for 'prev' operations. */
                    if (!getMode.isForward()) {
                        rangeLockCurrentPosition(getMode);
                    }
                    /* Use a range lock if performing a 'next' operation. */
                    final LockType lockType =
                        getLockType(lockMode, getMode.isForward());

                    /* Do not modify key/data params until SUCCESS. */
                    final DatabaseEntry tryKey = cloneEntry(key);
                    final DatabaseEntry tryData = cloneEntry(data);
                    final KeyChangeStatus result;

                    /* Perform the operation with a null rangeConstraint. */
                    OperationStatus status = retrieveNextAllowPhantoms
                        (tryKey, tryData, lockType, getMode,
                         null /*rangeConstraint*/);

                    if (getMode.isForward() &&
                        status != OperationStatus.SUCCESS) {
                        /* NEXT: lock the EOF node. */
                        cursorImpl.lockEof(LockType.RANGE_READ);
                    }

                    /* Finally check rangeConstraint. */
                    if (status == OperationStatus.SUCCESS &&
                        !checkRangeConstraint(tryKey)) {
                        status = OperationStatus.NOTFOUND;
                    }

                    /*
                     * Only overwrite key/data on SUCCESS, after all locking.
                     */
                    if (status == OperationStatus.SUCCESS) {
                        copyEntry(tryKey, key);
                        copyEntry(tryData, data);
                    }

                    return status;
                } catch (RangeRestartException e) {
                    continue;
                }
            }
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * For 'prev' operations, upgrades to a range lock at the current position.
     * If there are no records at the current position, get a range lock on the
     * next record or, if not found, on the logical EOF node.  Do not modify
     * the current cursor position, use a separate cursor.
     */
    private void rangeLockCurrentPosition(final GetMode getMode) {
        final DatabaseEntry tempKey = new DatabaseEntry();
        final DatabaseEntry tempData = new DatabaseEntry();
        tempKey.setPartial(0, 0, true);
        tempData.setPartial(0, 0, true);

        OperationStatus status;
        CursorImpl dup = cursorImpl.cloneCursor(true /*addCursor*/, cacheMode);
        try {
            status = dup.getCurrent
                (tempKey, tempData, LockType.RANGE_READ);
            if (status != OperationStatus.SUCCESS) {
                while (true) {
                    assert LatchSupport.countLatchesHeld() == 0;

                    status = dup.getNext
                        (tempKey, tempData, LockType.RANGE_READ, true, false,
                         null /*rangeConstraint*/);

                    if (checkForInsertion(GetMode.NEXT, cursorImpl, dup)) {
                        dup.close(cursorImpl);
                        dup = cursorImpl.cloneCursor(true /*addCursor*/,
                                                     cacheMode);
                        continue;
                    } else {
                        assert LatchSupport.countLatchesHeld() == 0;
                        break;
                    }
                }
            }
        } finally {
            dup.close(cursorImpl);
        }

        if (status != OperationStatus.SUCCESS) {
            cursorImpl.lockEof(LockType.RANGE_READ);
        }
    }

    /**
     * Retrieves without preventing phantoms.
     */
    private OperationStatus
        retrieveNextAllowPhantoms(final DatabaseEntry key,
                                  final DatabaseEntry data,
                                  final LockType lockType,
                                  final GetMode getMode,
                                  final RangeConstraint rangeConstraint) {
        assert (key != null && data != null);

        OperationStatus status;

        while (true) {
            assert LatchSupport.countLatchesHeld() == 0;
            final CursorImpl dup = beginMoveCursor(true);

            try {
                if (getMode == GetMode.NEXT) {
                    status = dup.getNext(key, data, lockType, true, false,
                                         rangeConstraint);
                } else if (getMode == GetMode.PREV) {
                    status = dup.getNext(key, data, lockType, false, false,
                                         rangeConstraint);
                } else {
                    throw EnvironmentFailureException.unexpectedState
                        ("unknown GetMode: " + getMode);
                }
            } catch (DatabaseException DBE) {
                endMoveCursor(dup, false);
                throw DBE;
            }

            if (checkForInsertion(getMode, cursorImpl, dup)) {
                endMoveCursor(dup, false);
                continue;
            } else {
                endMoveCursor(dup, status == OperationStatus.SUCCESS);
                assert LatchSupport.countLatchesHeld() == 0;
                break;
            }
        }
        return status;
    }

    /**
     * Returns the current key and data.  There is no need to use a dup cursor
     * or prevent phantoms.
     */
    OperationStatus getCurrentInternal(final DatabaseEntry key,
                                       final DatabaseEntry data,
                                       final LockMode lockMode) {
        synchronized (getTxnSynchronizer()) {
            checkTxnState();
            if (dbImpl.getSortedDuplicates()) {
                return getCurrentHandleDups(key, data, lockMode);
            }
            return getCurrentNoDups(key, data, lockMode);
        }
    }

    private OperationStatus getCurrentNoDups(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode) {

        /* Do not use a range lock. */
        final LockType lockType = getLockType(lockMode, false);

        beginUseExistingCursor();
        final OperationStatus status =
            cursorImpl.getCurrent(key, data, lockType);
        endUseExistingCursor();
        return status;
    }

    /*
     * Something may have been added to the original cursor (cursorImpl) while
     * we were getting the next BIN.  cursorImpl would have been adjusted
     * properly but we would have skipped a BIN in the process.  This can
     * happen when all INs are unlatched in Tree.getNextBin.
     *
     * Note that when we call LN.isDeleted(), we do not need to lock the LN.
     * If we see a non-committed deleted entry, we'll just iterate around in
     * the caller.  So a false positive is ok.
     *
     * @return true if an unaccounted for insertion happened.
     */
    private boolean checkForInsertion(final GetMode getMode,
                                      final CursorImpl origCursor,
                                      final CursorImpl dupCursor) {

        /* If fetchTarget returns null below, a deleted LN was cleaned. */
        boolean forward = getMode.isForward();
        boolean ret = false;
        if (origCursor.getBIN() != dupCursor.getBIN()) {

            /*
             * We jumped to the next BIN during getNext().
             *
             * Be sure to operate on the BIN returned by latchBIN, not a cached
             * var [#21121].
             *
             * Note that a cursor BIN can change after the check above, but
             * that's not relavent; what we're trying to detect are BIN changes
             * during the operation that has already completed.
             */
            final BIN origBIN = origCursor.latchBIN();

            try {
                if (forward) {
                    if (origBIN.getNEntries() - 1 >
                        origCursor.getIndex()) {

                        /*
                         * We were adjusted to something other than the
                         * last entry so some insertion happened.
                         */
                        for (int i = origCursor.getIndex() + 1;
                             i < origBIN.getNEntries();
                             i++) {
                            if (!origBIN.isEntryKnownDeleted(i)) {
                                final Node n = origBIN.fetchTarget(i);
                                if (n != null) {
                                    final LN ln = (LN) n;
                                    /* See comment above about locking. */
                                    if (!ln.isDeleted()) {
                                        ret = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (origCursor.getIndex() > 0) {

                        /*
                         * We were adjusted to something other than the
                         * first entry so some insertion happened.
                         */
                        for (int i = 0; i < origCursor.getIndex(); i++) {
                            if (!origBIN.isEntryKnownDeleted(i)) {
                                final Node n = origBIN.fetchTarget(i);
                                if (n != null) {
                                    final LN ln = (LN) n;
                                    /* See comment above about locking. */
                                    if (!ln.isDeleted()) {
                                        ret = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            } finally {
                origCursor.releaseBIN();
            }
            return ret;
        }
        return false;
    }

    /*
     * Methods to adapt operations to a duplicates DB.
     */
    private static final DatabaseEntry EMPTY_DUP_DATA =
        new DatabaseEntry(new byte[0]);
    private static final DatabaseEntry NO_RETURN_DATA = new DatabaseEntry();
    static {
        NO_RETURN_DATA.setPartial(0, 0, true);
    }

    /**
     * Count duplicates by skipping over the entries in the dup set key range.
     */
    private int countHandleDups() {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);

        final Cursor c = dup(false /*samePosition*/);
        try {
            c.setNonCloning(true);
            setPrefixConstraint(c, currentKey);

            /* Move cursor to first key in this dup set. */
            OperationStatus status = c.searchNoDups
                (twoPartKey, NO_RETURN_DATA, LockMode.READ_UNCOMMITTED,
                 SearchMode.SET_RANGE, null /*searchComparator*/);
            if (status != OperationStatus.SUCCESS) {
                return 0;
            }

            /* Skip over entries in the dup set. */
            long count = c.cursorImpl.skip(true /*forward*/, 0 /*maxCount*/,
                                           c.cursorRangeConstraint) + 1;
            if (count > Integer.MAX_VALUE) {
                throw new IllegalStateException
                    ("count exceeded integer size: " + count);
            }
            return (int) count;
        } finally {
            c.close();
        }
    }
  
    /**
     * Estimate duplicate count using the end point positions.
     */
    private long countEstimateHandleDups() {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);

        final Cursor c1 = dup(false /*samePosition*/);
        try {
            c1.setNonCloning(true);
            setPrefixConstraint(c1, currentKey);

            /* Move cursor 1 to first key in this dup set. */
            OperationStatus status = c1.searchNoDups
                (twoPartKey, NO_RETURN_DATA, LockMode.READ_UNCOMMITTED,
                 SearchMode.SET_RANGE, null /*searchComparator*/);
            if (status != OperationStatus.SUCCESS) {
                return 0;
            }

            /* Move cursor 2 to first key in the following dup set. */
            final Cursor c2 = c1.dup(true /*samePosition*/);
            try {
                c2.setNonCloning(true);
                status = c2.dupsGetNextNoDup(twoPartKey, NO_RETURN_DATA,
                                             LockMode.READ_UNCOMMITTED);
                final boolean c2Inclusive;
                if (status == OperationStatus.SUCCESS) {
                    c2Inclusive = false;
                } else {
                    c2Inclusive = true;

                    /*
                     * There is no following dup set.  Go to the last record in
                     * the database.  If we land on a newly inserted dup set,
                     * go to the prev record until we find the last record in
                     * the original dup set.
                     */
                    status = c2.positionNoDups(twoPartKey, NO_RETURN_DATA,
                         LockMode.READ_UNCOMMITTED, false /*first*/);
                    if (status != OperationStatus.SUCCESS) {
                        return 0;
                    }
                    while (!haveSameDupPrefix(twoPartKey, currentKey)) {
                        status = c2.retrieveNextNoDups
                            (twoPartKey, NO_RETURN_DATA,
                             LockMode.READ_UNCOMMITTED, GetMode.PREV);
                        if (status != OperationStatus.SUCCESS) {
                            return 0;
                        }
                    }
                }

                /* Estimate the count between the two cursor positions. */
                return CountEstimator.count(dbImpl, c1.cursorImpl, true,
                                            c2.cursorImpl, c2Inclusive);
            } finally {
                c2.close();
            }
        } finally {
            c1.close();
        }
    }

    /**
     * Interpret duplicates for 'put' operations.
     */
    private OperationStatus putHandleDups(final DatabaseEntry key,
                                          final DatabaseEntry data,
                                          final PutMode putMode) {
        switch (putMode) {
        case OVERWRITE:
            return dupsPutOverwrite(key, data);
        case NO_OVERWRITE:
            return dupsPutNoOverwrite(key, data);
        case NO_DUP_DATA:
            return dupsPutNoDupData(key, data);
        case CURRENT:
            return dupsPutCurrent(data);
        default:
            throw EnvironmentFailureException.unexpectedState
                (putMode.toString());
        }
    }

    /**
     * Interpret duplicates for putOverwrite operation.
     */
    private OperationStatus dupsPutOverwrite(final DatabaseEntry key,
                                             final DatabaseEntry data) {
        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);
        return putNoDups(twoPartKey, EMPTY_DUP_DATA, PutMode.OVERWRITE);
    }

    /**
     * Interpret duplicates for putNoOverwrite operation.
     *
     * The main purpose of this method is to guarantee that when two threads
     * call putNoOverwrite concurrently, only one of them will succeed. In
     * other words, if putNoOverwrite is called for all dup insertions, there
     * will always be at most one dup per key.
     *
     * Next key locking must be used to prevent two insertions, since there is
     * no other way to block an insertion of dup Y in another thread, while
     * inserting dup X in the current thread.  This is tested by AtomicPutTest.
     *
     * Although this method does extra searching and locking compared to
     * putNoOverwrite for a non-dup DB (or to putNoDupData for a dup DB), that
     * is not considered a significant issue because this method is rarely, if
     * ever, used by applications (for dup DBs that is).  It exists primarily
     * for compatibility with the DB core API.
     */
    private OperationStatus dupsPutNoOverwrite(final DatabaseEntry key,
                                               final DatabaseEntry data) {

        final DatabaseEntry key2 = new DatabaseEntry();
        final DatabaseEntry data2 = new DatabaseEntry();
        final Cursor c = dup(false /*samePosition*/);
        try {
            c.setNonCloning(true);

            /* Lock next key (or EOF if none) exclusively, before we insert. */
            copyEntry(key, key2);
            OperationStatus status =
                c.dupsGetSearchKeyRange(key2, data2, LockMode.RMW);
            if (status == OperationStatus.SUCCESS && key.equals(key2)) {
                /* Key exists, no need for further checks. */
                return OperationStatus.KEYEXIST;
            }
            if (status != OperationStatus.SUCCESS) {
                /* No next key exists, lock EOF. */
                c.cursorImpl.lockEof(LockType.WRITE);
            }

            /* While next key is locked, check for key existence again. */
            copyEntry(key, key2);
            status = c.dupsGetSearchKey(key2, data2, LockMode.RMW);
            if (status == OperationStatus.SUCCESS) {
                return OperationStatus.KEYEXIST;
            }

            /* Insertion can safely be done now. */
            status = c.dupsPutNoDupData(key, data);
            if (status != OperationStatus.SUCCESS) {
                return status;
            }

            /* We successfully inserted the first dup for the key. */
            swapCursor(c);
            return OperationStatus.SUCCESS;
        } finally {
            c.close();
        }
    }

    /**
     * Interpret duplicates for putNoDupData operation.
     */
    private OperationStatus dupsPutNoDupData(final DatabaseEntry key,
                                             final DatabaseEntry data) {
        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);
        return putNoDups(twoPartKey, EMPTY_DUP_DATA, PutMode.NO_OVERWRITE);
    }

    /**
     * Interpret duplicates for putCurrent operation.
     *
     * Get old key/data, replace data portion, and put new key/data.
     *
     * Arguably we could skip the replacement if there is no user defined
     * comparison function and the new data is the same.
     */
    private OperationStatus dupsPutCurrent(final DatabaseEntry newData) {

        final DatabaseEntry oldTwoPartKey = new DatabaseEntry();
        final OperationStatus status =
            getCurrentNoDups(oldTwoPartKey, NO_RETURN_DATA, LockMode.RMW);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }

        final DatabaseEntry key = new DatabaseEntry();
        DupKeyData.split(oldTwoPartKey, key, null);

        final DatabaseEntry newTwoPartKey = DupKeyData.combine(key, newData);
        return putNoDups(newTwoPartKey, EMPTY_DUP_DATA, PutMode.CURRENT);
    }

    /**
     * Interpret duplicates for getCurrent operation.
     */
    private OperationStatus getCurrentHandleDups(final DatabaseEntry key,
                                                 final DatabaseEntry data,
                                                 final LockMode lockMode) {
        final DatabaseEntry twoPartKey = new DatabaseEntry();
        final OperationStatus status =
            getCurrentNoDups(twoPartKey, NO_RETURN_DATA, lockMode);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }
        DupKeyData.split(twoPartKey, key, data);
        return OperationStatus.SUCCESS;
    }

    /**
     * Convenience method that does getCurrent, with and without dups, using
     * a CursorImpl.  Does no setup or save/restore of cursor state.
     */
    private OperationStatus getCurrentWithCursorImpl(final CursorImpl c,
                                                     final DatabaseEntry key,
                                                     final DatabaseEntry data,
                                                     final LockType lockType) {
        if (!dbImpl.getSortedDuplicates()) {
            return c.getCurrent(key, data, lockType);
        }
        final DatabaseEntry twoPartKey = new DatabaseEntry();
        final OperationStatus status =
            c.getCurrent(twoPartKey, NO_RETURN_DATA, lockType);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }
        DupKeyData.split(twoPartKey, key, data);
        return OperationStatus.SUCCESS;
    }

    /**
     * Interpret duplicates for getFirst and getLast operations.
     */
    private OperationStatus positionHandleDups(final DatabaseEntry key,
                                               final DatabaseEntry data,
                                               final LockMode lockMode,
                                               final boolean first) {
        final DatabaseEntry twoPartKey = new DatabaseEntry();
        final OperationStatus status =
            positionNoDups(twoPartKey, NO_RETURN_DATA, lockMode, first);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }
        DupKeyData.split(twoPartKey, key, data);
        return OperationStatus.SUCCESS;
    }

    /**
     * Interpret duplicates for getNext/Prev/etc operations.
     */
    private OperationStatus retrieveNextHandleDups(final DatabaseEntry key,
                                                   final DatabaseEntry data,
                                                   final LockMode lockMode,
                                                   final GetMode getMode) {
        switch (getMode) {
        case NEXT:
        case PREV:
            return dupsGetNextOrPrev(key, data, lockMode, getMode);
        case NEXT_DUP:
            return dupsGetNextOrPrevDup(key, data, lockMode, GetMode.NEXT);
        case PREV_DUP:
            return dupsGetNextOrPrevDup(key, data, lockMode, GetMode.PREV);
        case NEXT_NODUP:
            return dupsGetNextNoDup(key, data, lockMode);
        case PREV_NODUP:
            return dupsGetPrevNoDup(key, data, lockMode);
        default:
            throw EnvironmentFailureException.unexpectedState
                (getMode.toString());
        }
    }

    /**
     * Interpret duplicates for getNext and getPrev.
     */
    private OperationStatus dupsGetNextOrPrev(final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final LockMode lockMode,
                                              final GetMode getMode) {
        final DatabaseEntry twoPartKey = new DatabaseEntry();
        final OperationStatus status =
            retrieveNextNoDups(twoPartKey, NO_RETURN_DATA, lockMode, getMode);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }
        DupKeyData.split(twoPartKey, key, data);
        return OperationStatus.SUCCESS;
    }

    /**
     * Interpret duplicates for getNextDup and getPrevDup.
     *
     * Move the cursor forward or backward by one record, and check the key
     * prefix to detect going out of the bounds of the duplicate set.
     */
    private OperationStatus dupsGetNextOrPrevDup(final DatabaseEntry key,
                                                 final DatabaseEntry data,
                                                 final LockMode lockMode,
                                                 final GetMode getMode) {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final Cursor c = dup(true /*samePosition*/);
        try {
            c.setNonCloning(true);
            setPrefixConstraint(c, currentKey);
            final DatabaseEntry twoPartKey = new DatabaseEntry();
            final OperationStatus status = c.retrieveNextNoDups
                (twoPartKey, NO_RETURN_DATA, lockMode, getMode);
            if (status != OperationStatus.SUCCESS) {
                return status;
            }
            DupKeyData.split(twoPartKey, key, data);
            swapCursor(c);
            return OperationStatus.SUCCESS;
        } finally {
            c.close();
        }
    }

    /**
     * Interpret duplicates for getNextNoDup.
     *
     * Using a special comparator, search for first duplicate in the duplicate
     * set following the one for the current key.  For details see
     * DupKeyData.NextNoDupComparator.
     */
    private OperationStatus dupsGetNextNoDup(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode) {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);
        final Cursor c = dup(false /*samePosition*/);
        try {
            c.setNonCloning(true);
            final Comparator<byte[]> searchComparator =
                new DupKeyData.NextNoDupComparator
                    (dbImpl.getBtreeComparator());
            final OperationStatus status = c.searchNoDups
                (twoPartKey, NO_RETURN_DATA, lockMode, SearchMode.SET_RANGE,
                 searchComparator);
            if (status != OperationStatus.SUCCESS) {
                return status;
            }
            DupKeyData.split(twoPartKey, key, data);
            swapCursor(c);
            return OperationStatus.SUCCESS;
        } finally {
            c.close();
        }
    }

    /**
     * Interpret duplicates for getPrevNoDup.
     *
     * Move the cursor to the first duplicate in the duplicate set, then to the
     * previous record. If this fails because all dups at the current position
     * have been deleted, move the cursor backward to find the previous key.
     *
     * Note that we lock the first duplicate to enforce Serializable isolation.
     */
    private OperationStatus dupsGetPrevNoDup(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode) {
        final byte[] currentKey = cursorImpl.getCurrentKey();
        final DatabaseEntry twoPartKey = DupKeyData.removeData(currentKey);
        Cursor c = dup(false /*samePosition*/);
        try {
            c.setNonCloning(true);
            setPrefixConstraint(c, currentKey);
            OperationStatus status = c.searchNoDups
                (twoPartKey, NO_RETURN_DATA, lockMode, SearchMode.SET_RANGE,
                 null /*searchComparator*/);
            if (status == OperationStatus.SUCCESS) {
                c.setRangeConstraint(null);
                status = c.retrieveNextNoDups
                    (twoPartKey, NO_RETURN_DATA, lockMode, GetMode.PREV);
                if (status != OperationStatus.SUCCESS) {
                    return status;
                }
                DupKeyData.split(twoPartKey, key, data);
                swapCursor(c);
                return OperationStatus.SUCCESS;
            }
        } finally {
            c.close();
        }
        c = dup(true /*samePosition*/);
        try {
            c.setNonCloning(true);
            while (true) {
                final OperationStatus status = c.retrieveNextNoDups
                    (twoPartKey, NO_RETURN_DATA, lockMode, GetMode.PREV);
                if (status != OperationStatus.SUCCESS) {
                    return status;
                }
                if (!haveSameDupPrefix(twoPartKey, currentKey)) {
                    DupKeyData.split(twoPartKey, key, data);
                    swapCursor(c);
                    return OperationStatus.SUCCESS;
                }
            }
        } finally {
            c.close();
        }
    }

    /**
     * Interpret duplicates for getSearchKey/Range/Both operations.
     */
    private OperationStatus searchHandleDups(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode,
                                             final SearchMode searchMode) {
        switch (searchMode) {
        case SET:
            return dupsGetSearchKey(key, data, lockMode);
        case SET_RANGE:
            return dupsGetSearchKeyRange(key, data, lockMode);
        case BOTH:
            return dupsGetSearchBoth(key, data, lockMode);
        case BOTH_RANGE:
            return dupsGetSearchBothRange(key, data, lockMode);
        default:
            throw EnvironmentFailureException.unexpectedState
                (searchMode.toString());
        }
    }

    /**
     * Interpret duplicates for getSearchKey operation.
     *
     * Use key as prefix to find first duplicate using a range search.  Compare
     * result to prefix to see whether we went out of the bounds of the
     * duplicate set, i.e., whether NOTFOUND should be returned.
     */
    private OperationStatus dupsGetSearchKey(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode) {
        final DatabaseEntry twoPartKey = new DatabaseEntry
            (DupKeyData.makePrefixKey(key.getData(), key.getOffset(),
                                      key.getSize()));
        final Cursor c = dup(false /*samePosition*/);
        try {
            c.setNonCloning(true);
            setPrefixConstraint(c, key);
            final OperationStatus status = c.searchNoDups
                (twoPartKey, NO_RETURN_DATA, lockMode, SearchMode.SET_RANGE,
                 null /*searchComparator*/);
            if (status != OperationStatus.SUCCESS) {
                return OperationStatus.NOTFOUND;
            }
            DupKeyData.split(twoPartKey, key, data);
            swapCursor(c);
            return OperationStatus.SUCCESS;
        } finally {
            c.close();
        }
    }

    /**
     * Interpret duplicates for getSearchKeyRange operation.
     *
     * Do range search for key prefix.
     */
    private OperationStatus dupsGetSearchKeyRange(final DatabaseEntry key,
                                                  final DatabaseEntry data,
                                                  final LockMode lockMode) {
        final DatabaseEntry twoPartKey = new DatabaseEntry
            (DupKeyData.makePrefixKey(key.getData(), key.getOffset(),
                                      key.getSize()));
        final OperationStatus status = searchNoDups
            (twoPartKey, NO_RETURN_DATA, lockMode, SearchMode.SET_RANGE,
             null /*searchComparator*/);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }
        DupKeyData.split(twoPartKey, key, data);
        return OperationStatus.SUCCESS;
    }

    /**
     * Interpret duplicates for getSearchBoth operation.
     *
     * Do exact search for combined key.
     */
    private OperationStatus dupsGetSearchBoth(final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final LockMode lockMode) {
        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);
        final OperationStatus status = searchNoDups
            (twoPartKey, NO_RETURN_DATA, lockMode, SearchMode.BOTH,
             null /*searchComparator*/);
        if (status != OperationStatus.SUCCESS) {
            return status;
        }
        DupKeyData.split(twoPartKey, key, data);
        return OperationStatus.SUCCESS;
    }

    /**
     * Interpret duplicates for getSearchBothRange operation.
     *
     * Do range search for combined key.  Compare result to prefix to see
     * whether we went out of the bounds of the duplicate set, i.e., whether
     * NOTFOUND should be returned.
     */
    private OperationStatus dupsGetSearchBothRange(final DatabaseEntry key,
                                                   final DatabaseEntry data,
                                                   final LockMode lockMode) {
        final DatabaseEntry twoPartKey = DupKeyData.combine(key, data);
        final byte[] currentKey = twoPartKey.getData();
        final Cursor c = dup(false /*samePosition*/);
        try {
            c.setNonCloning(true);
            setPrefixConstraint(c, key);
            final OperationStatus status = c.searchNoDups
                (twoPartKey, NO_RETURN_DATA, lockMode, SearchMode.SET_RANGE,
                 null /*searchComparator*/);
            if (status != OperationStatus.SUCCESS) {
                return OperationStatus.NOTFOUND;
            }
            DupKeyData.split(twoPartKey, key, data);
            swapCursor(c);
            return OperationStatus.SUCCESS;
        } finally {
            c.close();
        }
    }

    /**
     * Reads the primary data for a primary key that was read via a secondary
     * cursor, or a regular Cursor in the role of a secondary cursor.  This
     * method is in the Cursor class, rather than in SecondaryCursor, to
     * support joins with plain Cursors [#21258].
     *
     * When SUCCESS is returned by this method, the caller should return
     * SUCCESS.  When KEYEMPTY is returned, the caller should treat this as a
     * deleted record and either retry the operation (in the case of position,
     * search, and retrieveNext) or return KEYEMPTY (in the case of
     * getCurrent).  KEYEMPTY is only returned when read-uncommitted is used.
     *
     * @return SUCCESS if the primary was read succesfully, or KEYEMPTY if
     * using read-uncommitted and the primary has been deleted, or KEYEMPTY if
     * using read-uncommitted and the primary has been updated and no longer
     * contains the secondary key.
     *
     * @throws SecondaryIntegrityException to indicate a corrupt secondary
     * reference if the primary record is not found and read-uncommitted is not
     * used (or read-uncommitted is used, but we cannot verify that a valid
     * deletion has occured or the number of retries has been exceeded).
     */
    OperationStatus readPrimaryAfterGet(final Database priDb,
                                        final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        DatabaseEntry data,
                                        final LockMode lockMode,
                                        final int retries)
        throws DatabaseException {

        /*
         * There is no need to read the primary if no data and no locking
         * (read-uncommitted) are requested by the caller.  However, if partial
         * data is requested along with read-uncommitted, then we must read all
         * data in order to call the key creator below. [#14966]
         */
        DatabaseEntry copyToPartialEntry = null;
        final boolean readUncommitted = isReadUncommittedMode(lockMode);
        if (readUncommitted && data.getPartial()) {
            if (data.getPartialLength() == 0) {
                /* No need to read the primary. */
                data.setData(LogUtils.ZERO_LENGTH_BYTE_ARRAY);
                return OperationStatus.SUCCESS;
            } else {
                /* Read all data and then copy the requested partial data. */
                copyToPartialEntry = data;
                data = new DatabaseEntry();
            }
        }

        final Locker locker = cursorImpl.getLocker();
        Cursor cursor = null;
        try {

            /*
             * Do not release non-transactional locks when reading the primary
             * cursor.  They are held until all locks for this operation are
             * released by the secondary cursor.  [#15573]
             */
            cursor = new Cursor(priDb, locker, null,
                                true /*retainNonTxnLocks*/);

            /*
             * If read-uncommmitted is used for this operation, also use it for
             * reading the primary.  Note that the cursor config is not
             * available so we have to specify the lock mode.
             */
            final LockMode primaryLockMode =
                readUncommitted ? LockMode.READ_UNCOMMITTED : lockMode;
            OperationStatus status =
                cursor.search(pKey, data, primaryLockMode, SearchMode.SET);
            if (status != OperationStatus.SUCCESS) {

                /*
                 * If using read-uncommitted and the primary is deleted, check
                 * to see if the secondary key has been deleted.  If so, the
                 * primary was deleted in between reading the secondary and the
                 * primary.  It is not corrupt, so we return KEYEMPTY.
                 */
                if (readUncommitted) {
                    status = getCurrentInternal(key, pKey, lockMode);
                    if (status == OperationStatus.KEYEMPTY) {
                        return status;
                    }
                }

                /*
                 * When the primary is deleted, secondary keys are deleted
                 * first (via triggers).  So if the above check fails, we know
                 * the secondary reference is corrupt and retries will not be
                 * productive.
                 */
                throw dbHandle.secondaryRefersToMissingPrimaryKey
                    (locker, key, pKey);
            }

            /*
             * If using read-uncommitted and the primary was found, check to
             * see if primary was updated so that it no longer contains the
             * secondary key.  If it has been, return KEYEMPTY.
             */
            if (readUncommitted) {
                final boolean possibleIntegrityError =
                    checkForPrimaryUpdate(key, pKey, data, retries);
                
                if (possibleIntegrityError) {

                    /*
                     * When the primary has been updated, the secondaries are
                     * deleted after the primary.  Allow retries to account for
                     * a delay between the primary and secondary writes.
                     */
                    if (retries < READ_PRIMARY_MAX_RETRIES) {
                        return OperationStatus.KEYEMPTY;
                    }

                    /*
                     * Retries have failed, so assume the secondary reference
                     * is corrupt.
                     */
                    throw dbHandle.secondaryRefersToMissingPrimaryKey
                        (locker, key, pKey);
                }
            }

            /*
             * When a partial entry was requested but we read all the data,
             * copy the requested partial data to the caller's entry. [#14966]
             */
            if (copyToPartialEntry != null) {
                LN.setEntry(copyToPartialEntry, data.getData());
            }
            return OperationStatus.SUCCESS;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    /**
     * Checks for a secondary corruption caused by a primary record update 
     * during a read-uncommitted read.  Checking in this method is not possible
     * because there is no secondary key creator available.  It is overridden
     * by SecondaryCursor.
     *
     * This method is in the Cursor class, rather than in SecondaryCursor, to
     * support joins with plain Cursors [#21258].
     */
    boolean checkForPrimaryUpdate(final DatabaseEntry key,
                                  final DatabaseEntry pKey,
                                  final DatabaseEntry data,
                                  final int retries) {
        return false;
    }

    /**
     * Returns whether the two keys have the same prefix.
     *
     * @param twoPartKey1 combined key with zero offset and size equal to the
     * data array length.
     *
     * @param keyBytes2 combined key byte array.
     */
    private boolean haveSameDupPrefix(final DatabaseEntry twoPartKey1,
                                      final byte[] keyBytes2) {
        assert twoPartKey1.getOffset() == 0;
        assert twoPartKey1.getData().length == twoPartKey1.getSize();

        return DupKeyData.compareMainKey
            (twoPartKey1.getData(), keyBytes2,
             dbImpl.getBtreeComparator()) == 0;
    }

    private void setPrefixConstraint(final Cursor c, final byte[] keyBytes2) {
        c.setRangeConstraint(new RangeConstraint() {
            public boolean inBounds(byte[] checkKey) {
                return DupKeyData.compareMainKey
                    (checkKey, keyBytes2, dbImpl.getBtreeComparator()) == 0;
            }
        });
    }

    private void setPrefixConstraint(final Cursor c,
                                     final DatabaseEntry key2) {
        c.setRangeConstraint(new RangeConstraint() {
            public boolean inBounds(byte[] checkKey) {
                return DupKeyData.compareMainKey
                    (checkKey, key2.getData(), key2.getOffset(),
                     key2.getSize(), dbImpl.getBtreeComparator()) == 0;
            }
        });
    }

    /**
     * Called to start an operation that potentially moves the cursor.
     *
     * If the cursor is initialized and cloning is enabled, dups it and returns
     * the dup; otherwise, return the original.  This avoids the overhead of
     * duping when the original is uninitialized or cloning is not needed.  The
     * cursor returned must be passed to endMoveCursor() to close the correct
     * cursor.
     *
     * Perform eviction before each cursor operation.  This is done by
     * CursorImpl.cloneCursor()/reset(), or is done here explicitly when the
     * cursor is not cloned or reset.
     *
     * @param addCursor is true to add the current position (BIN) to the
     * duplicated cursor.  If false, the caller will not remove the cursor from
     * the current position when moving it to a new position.  Therefore, if
     * this param is false, and cloning is disabled, and the cursor is
     * intialized, we must reset the cursor [#16280].
     *
     * @param usePosition copy this cursor position when duplicating, to
     * enable an optimization that uses the current position for an insert
     * after a search.  This optimization currently only applies when cloning
     * is enabled and the cursor is already initialized.
     *
     * @see CursorImpl#performCacheEviction for a description of how the
     * cacheMode field is used.  This method ensures that the correct cache
     * mode is used before each operation.
     */
    private CursorImpl beginMoveCursor(final boolean addCursor,
                                       final CursorImpl usePosition) {
        /* Must set cache mode before calling criticalEviction or reset. */
        if (cursorImpl.isNotInitialized()) {
            cursorImpl.setCacheMode(cacheMode);
            cursorImpl.criticalEviction();
            return cursorImpl;
        }
        if (nonCloning) {
            cursorImpl.setCacheMode(cacheMode);
            if (addCursor) {
                cursorImpl.criticalEviction();
            } else {
                cursorImpl.reset();
            }
            return cursorImpl;
        }
        CursorImpl dup =
            cursorImpl.cloneCursor(addCursor, cacheMode, usePosition);
        dup.setClosingLocker(cursorImpl);
        return dup;
    }

    private CursorImpl beginMoveCursor(final boolean addCursor) {
        return beginMoveCursor(addCursor, null);
    }

    /**
     * Called to end an operation that potentially moves the cursor.
     *
     * If the operation is successful, swaps cursors and closes the original
     * cursor; otherwise, closes the duped cursor.  In the case where the
     * original cursor was not duped by beginMoveCursor because it was
     * uninitialized, just resets the original cursor if the operation did not
     * succeed.
     *
     * Perform eviction after each cursor operation.  This is done by
     * CursorImpl.reset() and close(), or is done here explicitly when the
     * cursor is not cloned.
     */
    private void endMoveCursor(final CursorImpl dup, final boolean success) {
        dup.clearClosingLocker();
        if (dup == cursorImpl) {
            if (success) {
                cursorImpl.criticalEviction();
            } else {
                cursorImpl.reset();
            }

            /*
             * Do not refresh the default CacheMode here to avoid calling
             * dbImpl.getDefaultCacheMode() twice when a cursor is only used
             * for a single operation.
             */
        } else {
            if (success) {
                cursorImpl.close(dup);
                cursorImpl = dup;
            } else {
                dup.close(cursorImpl);
            }

            /*
             * In case the default CacheMode is changed when a cursor is used
             * for many operations, get a fresh default CacheMode for the next
             * operation.
             */
            if (!cacheModeOverridden) {
                cacheMode = dbImpl.getDefaultCacheMode();
            }
        }
    }

    /**
     * Called to start an operation that does not move the cursor, and
     * therefore does not clone the cursor.  Either beginUseExistingCursor /
     * endUseExistingCursor or beginMoveCursor / endMoveCursor must be used for
     * each operation.
     */
    private void beginUseExistingCursor() {
        /* Must set cache mode before calling criticalEviction. */
        cursorImpl.setCacheMode(cacheMode);
        cursorImpl.criticalEviction();
    }

    /**
     * Called to end an operation that does not move the cursor.
     */
    private void endUseExistingCursor() {
        cursorImpl.criticalEviction();
    }

    /**
     * Swaps CursorImpl of this cursor and the other cursor given.
     */
    private void swapCursor(Cursor other) {
        final CursorImpl otherImpl = other.cursorImpl;
        other.cursorImpl = this.cursorImpl;
        this.cursorImpl = otherImpl;
    }

    boolean advanceCursor(final DatabaseEntry key, final DatabaseEntry data) {
        return cursorImpl.advanceCursor(key, data);
    }

    private LockType getLockType(final LockMode lockMode,
                                 final boolean rangeLock) {

        if (isReadUncommittedMode(lockMode)) {
            return LockType.NONE;
        } else if (lockMode == null || lockMode == LockMode.DEFAULT) {
            return rangeLock ? LockType.RANGE_READ: LockType.READ;
        } else if (lockMode == LockMode.RMW) {
            return rangeLock ? LockType.RANGE_WRITE: LockType.WRITE;
        } else if (lockMode == LockMode.READ_COMMITTED) {
            throw new IllegalArgumentException
                (lockMode.toString() + " not allowed with Cursor methods, " +
                 "use CursorConfig.setReadCommitted instead.");
        } else {
            assert false : lockMode;
            return LockType.NONE;
        }
    }

    /**
     * Returns whether the given lock mode will cause a read-uncommitted when
     * used with this cursor, taking into account the default cursor
     * configuration.
     */
    boolean isReadUncommittedMode(final LockMode lockMode) {

        return (lockMode == LockMode.READ_UNCOMMITTED ||
                (readUncommittedDefault &&
                 (lockMode == null || lockMode == LockMode.DEFAULT)));
    }

    private boolean isSerializableIsolation(final LockMode lockMode) {

        return serializableIsolationDefault &&
               !isReadUncommittedMode(lockMode);
    }

    /**
     * @hidden
     * For internal use only.
     *
     * @throws UnsupportedOperationException via all Cursor write methods.
     */
    protected void checkUpdatesAllowed(final String operation) {
        if (updateOperationsProhibited) {
            throw new UnsupportedOperationException
                ("A transaction was not supplied when opening this cursor: " +
                 operation);
        }
    }

    /**
     * Note that this flavor of checkArgs allows the key and data to be null.
     */
    static void checkArgsNoValRequired(final DatabaseEntry key,
                                       final DatabaseEntry data) {
        DatabaseUtil.checkForNullDbt(key, "key", false);
        DatabaseUtil.checkForNullDbt(data, "data", false);
    }

    /**
     * Note that this flavor of checkArgs requires that the key and data are
     * not null.
     */
    static void checkArgsValRequired(final DatabaseEntry key,
                                     final DatabaseEntry data) {
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(data, "data", true);
    }

    /**
     * Checks the environment and cursor state.
     */
    void checkState(final boolean mustBeInitialized) {
        checkEnv();
        if (dbHandle != null) {
            dbHandle.checkOpen("Can't call Cursor method:");
        }
        cursorImpl.checkCursorState(mustBeInitialized);
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     */
    void checkEnv() {
        cursorImpl.checkEnv();
    }

    /**
     * Returns an object used for synchronizing transactions that are used in
     * multiple threads.
     *
     * For a transactional locker, the Transaction is returned to prevent
     * concurrent access using this transaction from multiple threads.  The
     * Transaction.commit and abort methods are synchronized so they do not run
     * concurrently with operations using the Transaction.  Note that the Txn
     * cannot be used for synchronization because locking order is BIN first,
     * then Txn.
     *
     * For a non-transactional locker, 'this' is returned because no special
     * blocking is needed.  Other mechanisms are used to prevent
     * non-transactional usage access by multiple threads (see ThreadLocker).
     * In the future we may wish to use the getTxnSynchronizer for
     * synchronizing non-transactional access as well; however, note that a new
     * locker is created for each operation.
     */
    private Object getTxnSynchronizer() {
        return (transaction != null) ? transaction : this;
    }

    private void checkTxnState() {
        if (transaction == null) {
            return;
        }
        transaction.checkOpen();
        transaction.getTxn().checkState(false /*calledByAbort*/);
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(final Level level,
               final String methodName,
               final DatabaseEntry key,
               final DatabaseEntry data,
               final LockMode lockMode) {
        if (logger.isLoggable(level)) {
            final StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            traceCursorImpl(sb);
            if (key != null) {
                sb.append(" key=").append(key.dumpData());
            }
            if (data != null) {
                sb.append(" data=").append(data.dumpData());
            }
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg
                (logger, dbImpl.getDbEnvironment(), level, sb.toString());
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    void trace(final Level level,
               final String methodName,
               final LockMode lockMode) {
        if (logger.isLoggable(level)) {
            final StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            traceCursorImpl(sb);
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg
                (logger, dbImpl.getDbEnvironment(), level, sb.toString());
        }
    }

    private void traceCursorImpl(final StringBuilder sb) {
        sb.append(" locker=").append(cursorImpl.getLocker().getId());
        if (cursorImpl.getBIN() != null) {
            sb.append(" bin=").append(cursorImpl.getBIN().getNodeId());
        }
        sb.append(" idx=").append(cursorImpl.getIndex());
    }

    /**
     * Clone entry contents in a new returned entry.
     */
    private static DatabaseEntry cloneEntry(DatabaseEntry from) {
        final DatabaseEntry to = new DatabaseEntry();
        copyEntry(from, to);
        return to;
    }

    /**
     * Copy entry contents to another entry.
     */
    private static void copyEntry(DatabaseEntry from, DatabaseEntry to) {
        to.setPartial(from.getPartialOffset(), from.getPartialLength(),
                      from.getPartial());
        to.setData(from.getData(), from.getOffset(), from.getSize());
    }
}
