/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DiskOrderedCursorImpl;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * DiskOrderedCursor returns records in unsorted order in exchange for
 * generally faster retrieval times. Instead of key order, an approximation of
 * disk order is used, which results in less I/O. This can be useful when the
 * application needs to scan all records in a database, and will be applying
 * filtering logic which does not need key ordered retrieval.
 * DiskOrderedCursor is created using the {@link
 * Database#openCursor(DiskOrderedCursorConfig)} method.
 * <p>
 * All cursors can consume resources and should be closed as soon as the
 * application is finished using them. In particular, a DiskOrderedCursor
 * disables the file deletion done by log cleaning, and will therefore delay
 * the compaction of log files.
 * <p>
 * <em>Optional configurations:</em> the following options are available to
 * tune the DiskOrderedCursor.
 * <p>
 * The DiskOrderedCursor creates a background producer thread which prefetches
 * some target records and inserts them in a queue for use by the cursor. The
 * parameter {@link EnvironmentConfig#DOS_PRODUCER_QUEUE_TIMEOUT} applies to
 * this background thread, and controls the timeout which governs the blocking
 * queue.
 * <p>
 * When the DiskOrderedCursor is first created, it is "seeded" with in-memory
 * internal B-Tree nodes.  This seeding causes the root and all seed INs to be
 * latched for read, for use by the cursor. Seeded B-Tree nodes help the
 * efficiency of the cursor, but penalize any concurrent update operations
 * which require write locks on those nodes.
 * {@link DiskOrderedCursorConfig#setMaxSeedMillisecs} can be used to
 * limit the time spent on the seeding process in exchange for reduced
 * performance during the cursor
 * walk.
 * <p>
 * {@link DiskOrderedCursorConfig#setMaxSeedNodes} can be used to
 * limit the number of nodes used to seed the DiskOrderedCursor, which also can
 * reduce cursor performance, but improves the performance of concurrent write
 * operations.
 * @since 5.0
 */
public class DiskOrderedCursor implements ForwardCursor {

    private final Database dbHandle;
    private final DatabaseImpl dbImpl;

    private final DiskOrderedCursorConfig config;
    private final DiskOrderedCursorImpl dosCursorImpl;

    private final Logger logger;

    DiskOrderedCursor(final Database dbHandle,
                      final DiskOrderedCursorConfig config) {
        this.dbHandle = dbHandle;
        this.config = config;
        dbImpl = dbHandle.getDatabaseImpl();
        dosCursorImpl = new DiskOrderedCursorImpl(dbImpl, config);
        dbHandle.addCursor(this);
        this.logger = dbImpl.getDbEnvironment().getLogger();
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
     * Discards the cursor.
     *
     * <p>The cursor handle may not be used again after this method has been
     * called, regardless of the method's success or failure.</p>
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

        if (dosCursorImpl.isClosed()) {
            return;
        }
        try {
            dosCursorImpl.checkEnv();
            dosCursorImpl.close();
            dbHandle.removeCursor(this);
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Returns the key/data pair to which the cursor refers.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * This argument should be supplied even if the DiskOrderedCursor has
     * been configured with keysOnly.
     *
     * @param lockMode the locking attributes.  For DiskOrderedCursors this
     * parameter must be either null or {@link
     * com.sleepycat.je.LockMode#READ_UNCOMMITTED} since no locking is
     * performed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if there are no more records in the
     * DiskOrderedCursor set, otherwise, {@link
     * com.sleepycat.je.OperationStatus#SUCCESS OperationStatus.SUCCESS}.  If
     * the record referred to by a DiskOrderedCursor is deleted after the
     * ForwardCursor is positioned at that record, getCurrent() will still
     * return the key and value of that record and OperationStatus.SUCCESS.
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
            checkState();
            Cursor.checkArgsNoValRequired(key, data);
            checkLockMode(lockMode);
            trace(Level.FINEST, "DiskOrderedCursor.getCurrent: ", lockMode);
            return dosCursorImpl.getCurrent(key, data);
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Moves the cursor to the next key/data pair and returns that pair.
     *
     * <p>If the cursor is not yet initialized, move the cursor to an arbitrary
     * key/data pair of the database, and return that pair.  Otherwise, the
     * cursor is moved to the next key/data pair of the set, and that pair
     * is returned.  </p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * @param key the key returned as output.  Its byte array does not need to
     * be initialized by the caller.
     *
     * @param data the data returned as output.  Its byte array does not need
     * to be initialized by the caller.
     * This argument should be supplied even if the DiskOrderedCursor has
     * been configured with keysOnly.
     *
     * @param lockMode the locking attributes.  For DiskOrderedCursors this
     * parameter must be either null or {@link
     * com.sleepycat.je.LockMode#READ_UNCOMMITTED} since no locking is
     * performed.
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

        try {
            checkState();
            Cursor.checkArgsNoValRequired(key, data);
            checkLockMode(lockMode);
            trace(Level.FINEST, "DiskOrderedCursor.getNext: ", lockMode);
            return dosCursorImpl.getNext(key, data);
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    /**
     * Returns this cursor's configuration.
     *
     * <p>This may differ from the configuration used to open this object if
     * the cursor existed previously.</p>
     *
     * @return This cursor's configuration.
     */
    public DiskOrderedCursorConfig getConfig() {
        try {
            return config.clone();
        } catch (Error E) {
            dbImpl.getDbEnvironment().invalidate(E);
            throw E;
        }
    }

    private void checkLockMode(final LockMode lockMode) {
        if (lockMode == null ||
            lockMode == LockMode.READ_UNCOMMITTED) {
            return;
        }

        throw new IllegalArgumentException
            ("lockMode must be null or LockMode.READ_UNCOMMITTED");
    }

    /**
     * Checks the environment and cursor state.
     */
    private void checkState() {
        dosCursorImpl.checkEnv();
        if (dbHandle != null) {
            dbHandle.checkOpen("Can't call ForwardCursor method:");
        }
    }

    /**
     * Sends trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void trace(final Level level,
                       final String methodName,
                       final LockMode lockMode) {
        if (logger.isLoggable(level)) {
            final StringBuilder sb = new StringBuilder();
            sb.append(methodName);
            if (lockMode != null) {
                sb.append(" lockMode=").append(lockMode);
            }
            LoggerUtils.logMsg
                (logger, dbImpl.getDbEnvironment(), level, sb.toString());
        }
    }
}
