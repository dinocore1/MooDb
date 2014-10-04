/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.io.Closeable;

/**
 * The interface for forward-moving Cursor operations.  Specific implementations
 * may modify the documented behavior on each of these methods.
 * @since 5.0
 */
public interface ForwardCursor extends Closeable {

    /**
     * Returns the Database handle associated with this ForwardCursor.
     *
     * @return The Database handle associated with this ForwardCursor.
     */
    public Database getDatabase();

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
        throws DatabaseException;

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
        throws DatabaseException;

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
        throws DatabaseException;
}
