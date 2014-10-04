/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.dbi.GetMode;
import com.sleepycat.je.dbi.CursorImpl.SearchMode;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.utilint.DatabaseUtil;

/**
 * A database cursor for a secondary database. Cursors are not thread safe and
 * the application is responsible for coordinating any multithreaded access to
 * a single cursor object.
 *
 * <p>Secondary cursors are returned by {@link SecondaryDatabase#openCursor
 * SecondaryDatabase.openCursor} and {@link
 * SecondaryDatabase#openSecondaryCursor
 * SecondaryDatabase.openSecondaryCursor}.  The distinguishing characteristics
 * of a secondary cursor are:</p>
 *
 * <ul> <li>Direct calls to <code>put()</code> methods on a secondary cursor
 * are prohibited.
 *
 * <li>The {@link #delete} method of a secondary cursor will delete the primary
 * record and as well as all its associated secondary records.
 *
 * <li>Calls to all get methods will return the data from the associated
 * primary database.
 *
 * <li>Additional get method signatures are provided to return the primary key
 * in an additional pKey parameter.
 *
 * <li>Calls to {@link #dup} will return a {@link SecondaryCursor}.
 *
 * </ul>
 *
 * <p>To obtain a secondary cursor with default attributes:</p>
 *
 * <blockquote><pre>
 *     SecondaryCursor cursor = myDb.openSecondaryCursor(txn, null);
 * </pre></blockquote>
 *
 * <p>To customize the attributes of a cursor, use a CursorConfig object.</p>
 *
 * <blockquote><pre>
 *     CursorConfig config = new CursorConfig();
 *     config.setReadUncommitted(true);
 *     SecondaryCursor cursor = myDb.openSecondaryCursor(txn, config);
 * </pre></blockquote>
 */
public class SecondaryCursor extends Cursor {

    private final SecondaryDatabase secondaryDb;
    private final Database primaryDb;

    /**
     * Cursor constructor. Not public. To get a cursor, the user should call
     * SecondaryDatabase.cursor();
     */
    SecondaryCursor(final SecondaryDatabase dbHandle,
                    final Transaction txn,
                    final CursorConfig cursorConfig)
        throws DatabaseException {

        super(dbHandle, txn, cursorConfig);
        secondaryDb = dbHandle;
        primaryDb = dbHandle.getPrimaryDatabase();
    }

    /**
     * Cursor constructor. Not public. To get a cursor, the user should call
     * SecondaryDatabase.cursor();
     */
    SecondaryCursor(final SecondaryDatabase dbHandle,
                    final Locker locker,
                    final CursorConfig cursorConfig)
        throws DatabaseException {

        super(dbHandle, locker, cursorConfig);
        secondaryDb = dbHandle;
        primaryDb = dbHandle.getPrimaryDatabase();
    }

    /**
     * Copy constructor.
     */
    private SecondaryCursor(final SecondaryCursor cursor,
                            final boolean samePosition)
        throws DatabaseException {

        super(cursor, samePosition);
        secondaryDb = cursor.secondaryDb;
        primaryDb = cursor.primaryDb;
    }

    /**
     * Returns the primary {@link com.sleepycat.je.Database Database}
     * associated with this cursor.
     *
     * <p>Calling this method is the equivalent of the following
     * expression:</p>
     *
     * <blockquote><pre>
     *         ((SecondaryDatabase) this.getDatabase()).getPrimaryDatabase()
     * </pre></blockquote>
     *
     * @return The primary {@link com.sleepycat.je.Database Database}
     * associated with this cursor.
     */
    public Database getPrimaryDatabase() {
        return primaryDb;
    }

    /**
     * Returns a new <code>SecondaryCursor</code> for the same transaction as
     * the original cursor.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public SecondaryCursor dup(final boolean samePosition)
        throws DatabaseException {

        checkState(false);
        return new SecondaryCursor(this, samePosition);
    }

    /**
     * Returns a new copy of the cursor as a <code>SecondaryCursor</code>.
     *
     * <p>Calling this method is the equivalent of calling {@link #dup} and
     * casting the result to {@link SecondaryCursor}.</p>
     *
     * @see #dup
     *
     * @deprecated As of JE 4.0.13, replaced by {@link Cursor#dup}.</p>
     */
    public SecondaryCursor dupSecondary(final boolean samePosition)
        throws DatabaseException {

        return (SecondaryCursor) dup(samePosition);
    }

    /**
     * Delete the key/data pair to which the cursor refers from the primary
     * database and all secondary indices.
     *
     * <p>This method behaves as if {@link Database#delete} were called for the
     * primary database, using the primary key obtained via the secondary key
     * parameter.</p>
     *
     * The cursor position is unchanged after a delete, and subsequent calls to
     * cursor functions expecting the cursor to refer to an existing key will
     * fail.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus delete()
        throws LockConflictException,
               DatabaseException,
               UnsupportedOperationException,
               IllegalStateException {

        checkState(true);
        checkUpdatesAllowed("delete");
        trace(Level.FINEST, "SecondaryCursor.delete: ", null);

        /* Read the primary key (the data of a secondary). */
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry pKey = new DatabaseEntry();
        OperationStatus status = getCurrentInternal(key, pKey,
                                                    LockMode.RMW);

        /* Delete the primary and all secondaries (including this one). */
        if (status == OperationStatus.SUCCESS) {
            Locker locker = cursorImpl.getLocker();
            status =
                primaryDb.deleteInternal(locker, pKey, null);
            if (status != OperationStatus.SUCCESS) {
                SecondaryDatabase secDb = (SecondaryDatabase) getDatabase();
                throw secDb.secondaryRefersToMissingPrimaryKey
                    (locker, key, pKey);
            }
        }
        return status;
    }

    /**
     * This operation is not allowed on a secondary database. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary database should be used instead.
     */
    @Override
    public OperationStatus put(final DatabaseEntry key,
                               final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary database. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary database should be used instead.
     */
    @Override
    public OperationStatus putNoOverwrite(final DatabaseEntry key,
                                          final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary database. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary database should be used instead.
     */
    @Override
    public OperationStatus putNoDupData(final DatabaseEntry key,
                                        final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * This operation is not allowed on a secondary database. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method on the primary database should be used instead.
     */
    @Override
    public OperationStatus putCurrent(final DatabaseEntry data) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getCurrent(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        return getCurrent(key, new DatabaseEntry(), data, lockMode);
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @param lockMode the locking attributes; if null, default attributes are
     * used. {@link LockMode#READ_COMMITTED} is not allowed.
     *
     * @return {@link com.sleepycat.je.OperationStatus#KEYEMPTY
     * OperationStatus.KEYEMPTY} if the key/pair at the cursor position has
     * been deleted; otherwise, {@link com.sleepycat.je.OperationStatus#SUCCESS
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getCurrent(final DatabaseEntry key,
                                      final DatabaseEntry pKey,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getCurrent: ", lockMode);

        return getCurrentInternal(key, pKey, data, lockMode);
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getFirst(final DatabaseEntry key,
                                    final DatabaseEntry data,
                                    final LockMode lockMode)
        throws DatabaseException {

        return getFirst(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the first key/data pair of the database, and return
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getFirst(final DatabaseEntry key,
                                    final DatabaseEntry pKey,
                                    final DatabaseEntry data,
                                    final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getFirst: ", lockMode);

        return position(key, pKey, data, lockMode, true);
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getLast(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        return getLast(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the last key/data pair of the database, and return
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getLast(final DatabaseEntry key,
                                   final DatabaseEntry pKey,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getLast: ", lockMode);

        return position(key, pKey, data, lockMode, false);
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getNext(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        return getNext(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the next key/data pair and return that pair.  If the
     * matching key has duplicate values, the first data item in the set of
     * duplicates is returned.
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getNext(final DatabaseEntry key,
                                   final DatabaseEntry pKey,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getNext: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, pKey, data, lockMode, true);
        } else {
            return retrieveNext(key, pKey, data, lockMode, GetMode.NEXT);
        }
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getNextDup(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        return getNextDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * If the next key/data pair of the database is a duplicate data record for
     * the current key/data pair, move the cursor to the next key/data pair of
     * the database and return that pair.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getNextDup(final DatabaseEntry key,
                                      final DatabaseEntry pKey,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getNextDup: ", lockMode);

        return retrieveNext(key, pKey, data, lockMode, GetMode.NEXT_DUP);
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getNextNoDup(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        return getNextNoDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the next non-duplicate key/data pair and return that
     * pair.  If the matching key has duplicate values, the first data item in
     * the set of duplicates is returned.
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getNextNoDup(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getNextNoDup: ", null, null,
              lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, pKey, data, lockMode, true);
        } else {
            return retrieveNext(key, pKey, data, lockMode,
                                GetMode.NEXT_NODUP);
        }
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getPrev(final DatabaseEntry key,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        return getPrev(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the previous key/data pair and return that pair. If
     * the matching key has duplicate values, the last data item in the set of
     * duplicates is returned.
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getPrev(final DatabaseEntry key,
                                   final DatabaseEntry pKey,
                                   final DatabaseEntry data,
                                   final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getPrev: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, pKey, data, lockMode, false);
        } else {
            return retrieveNext(key, pKey, data, lockMode, GetMode.PREV);
        }
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getPrevDup(final DatabaseEntry key,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        return getPrevDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * If the previous key/data pair of the database is a duplicate data record
     * for the current key/data pair, move the cursor to the previous key/data
     * pair of the database and return that pair.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getPrevDup(final DatabaseEntry key,
                                      final DatabaseEntry pKey,
                                      final DatabaseEntry data,
                                      final LockMode lockMode)
        throws DatabaseException {

        checkState(true);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getPrevDup: ", lockMode);

        return retrieveNext(key, pKey, data, lockMode, GetMode.PREV_DUP);
    }

    /**
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getPrevNoDup(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        return getPrevNoDup(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the previous non-duplicate key/data pair and return
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
     * @param key the secondary key returned as output.  Its byte array does
     * not need to be initialized by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getPrevNoDup(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        checkArgsNoValRequired(key, pKey, data);
        trace(Level.FINEST, "SecondaryCursor.getPrevNoDup: ", lockMode);

        if (cursorImpl.isNotInitialized()) {
            return position(key, pKey, data, lockMode, false);
        } else {
            return retrieveNext(key, pKey, data, lockMode,
                                GetMode.PREV_NODUP);
        }
    }

    /**
     * @param key the secondary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * @throws OperationFailureException if one of the <a
     * href="OperationFailureException.html#readFailures">Read Operation
     * Failures</a> occurs.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getSearchKey(final DatabaseEntry key,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        return getSearchKey(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the given key of the database, and return the datum
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
     * @param key the secondary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchKey(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data,
                                        final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", false);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "SecondaryCursor.getSearchKey: ", key, null,
              lockMode);

        return search(key, pKey, data, lockMode, SearchMode.SET);
    }

    /**
     * @param key the secondary key used as input and returned as output.  It
     * must be initialized with a non-null byte array by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
     * A <a href="Cursor.html#partialEntry">partial data item</a> may be
     * specified to optimize for key only or partial data retrieval.
     *
     * <!-- inherit other javadoc from overridden method -->
     */
    @Override
    public OperationStatus getSearchKeyRange(final DatabaseEntry key,
                                             final DatabaseEntry data,
                                             final LockMode lockMode)
        throws DatabaseException {

        return getSearchKeyRange(key, new DatabaseEntry(), data, lockMode);
    }

    /**
     * Move the cursor to the closest matching key of the database, and return
     * the data item associated with the matching key.  If the matching key has
     * duplicate values, the first data item in the set of duplicates is
     * returned.
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
     * @param key the secondary key used as input and returned as output.  It
     * must be initialized with a non-null byte array by the caller.
     *
     * @param pKey the primary key returned as output.  Its byte array does not
     * need to be initialized by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchKeyRange(final DatabaseEntry key,
                                             final DatabaseEntry pKey,
                                             final DatabaseEntry data,
                                             final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", false);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "SecondaryCursor.getSearchKeyRange: ", key, data,
              lockMode);

        return search(key, pKey, data, lockMode, SearchMode.SET_RANGE);
    }

    /**
     * This operation is not allowed with this method signature. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method with the <code>pKey</code> parameter should be
     * used instead.
     */
    @Override
    public OperationStatus getSearchBoth(final DatabaseEntry key,
                                         final DatabaseEntry data,
                                         final LockMode lockMode) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * Move the cursor to the specified secondary and primary key, where both
     * the primary and secondary key items must match.
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the secondary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param pKey the primary key used as input.  It must be initialized with
     * a non-null byte array by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchBoth(final DatabaseEntry key,
                                         final DatabaseEntry pKey,
                                         final DatabaseEntry data,
                                         final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "SecondaryCursor.getSearchBoth: ", key, data,
              lockMode);

        return search(key, pKey, data, lockMode, SearchMode.BOTH);
    }

    /**
     * This operation is not allowed with this method signature. {@link
     * UnsupportedOperationException} will always be thrown by this method.
     * The corresponding method with the <code>pKey</code> parameter should be
     * used instead.
     */
    @Override
    public OperationStatus getSearchBothRange(final DatabaseEntry key,
                                              final DatabaseEntry data,
                                              final LockMode lockMode) {
        throw SecondaryDatabase.notAllowedException();
    }

    /**
     * Move the cursor to the specified secondary key and closest matching
     * primary key of the database.
     *
     * <p>In the case of any database supporting sorted duplicate sets, the
     * returned key/data pair is for the smallest primary key greater than or
     * equal to the specified primary key (as determined by the key comparison
     * function), permitting partial matches and range searches in duplicate
     * data sets.</p>
     *
     * <p>If this method fails for any reason, the position of the cursor will
     * be unchanged.</p>
     *
     * <p>In a replicated environment, an explicit transaction must have been
     * specified when opening the cursor, unless read-uncommitted isolation is
     * specified via the {@link CursorConfig} or {@link LockMode}
     * parameter.</p>
     *
     * @param key the secondary key used as input and returned as output.  It
     * must be initialized with a non-null byte array by the caller.
     *
     * @param pKey the primary key used as input and returned as output.  It
     * must be initialized with a non-null byte array by the caller.
     *
     * @param data the primary data returned as output.  Its byte array does
     * not need to be initialized by the caller.
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
     * @throws IllegalArgumentException if an invalid parameter is specified,
     * for example, if a DatabaseEntry parameter is null or does not contain a
     * required non-null byte array.
     */
    public OperationStatus getSearchBothRange(final DatabaseEntry key,
                                              final DatabaseEntry pKey,
                                              final DatabaseEntry data,
                                              final LockMode lockMode)
        throws DatabaseException {

        checkState(false);
        DatabaseUtil.checkForNullDbt(key, "key", true);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", true);
        DatabaseUtil.checkForNullDbt(data, "data", false);
        trace(Level.FINEST, "SecondaryCursor.getSearchBothRange: ", key, data,
              lockMode);

        return search(key, pKey, data, lockMode, SearchMode.BOTH_RANGE);
    }

    /**
     * Returns the current key and data.
     */
    private OperationStatus getCurrentInternal(final DatabaseEntry key,
                                               final DatabaseEntry pKey,
                                               final DatabaseEntry data,
                                               final LockMode lockMode)
        throws DatabaseException {

        OperationStatus status = getCurrentInternal(key, pKey, lockMode);
        if (status == OperationStatus.SUCCESS) {

            /*
             * May return KEYEMPTY if read-uncommitted and the primary was
             * deleted.
             */
            status = readPrimaryAfterGet(key, pKey, data, lockMode, 0);
        }
        return status;
    }

    /**
     * Calls search() and retrieves primary data.
     */
    OperationStatus search(final DatabaseEntry key,
                           final DatabaseEntry pKey,
                           final DatabaseEntry data,
                           final LockMode lockMode,
                           final SearchMode searchMode)
        throws DatabaseException {

        /*
         * Perform retries to account for primary updates during a
         * read-uncommitted.
         */
        int retries = 0;
        byte[] lastKey = null;
        while (true) {
            OperationStatus status = search(key, pKey, lockMode, searchMode);
            if (status != OperationStatus.SUCCESS) {
                return status;
            }
            if (!Arrays.equals(lastKey, key.getData())) {
                retries = 0;
                lastKey = key.getData();
            }
            status = readPrimaryAfterGet(key, pKey, data, lockMode, retries);
            if (status == OperationStatus.SUCCESS) {
                return status;
            }
            retries += 1;
            try {
                Thread.sleep(SLEEP_BEFORE_READ_PRIMARY_RETRY);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * Calls position() and retrieves primary data.
     */
    OperationStatus position(final DatabaseEntry key,
                             final DatabaseEntry pKey,
                             final DatabaseEntry data,
                             final LockMode lockMode,
                             final boolean first)
        throws DatabaseException {

        /*
         * Perform retries to account for primary updates during a
         * read-uncommitted.
         */
        int retries = 0;
        byte[] lastKey = null;
        while (true) {
            OperationStatus status = position(key, pKey, lockMode, first);
            if (status != OperationStatus.SUCCESS) {
                return status;
            }
            if (!Arrays.equals(lastKey, key.getData())) {
                retries = 0;
                lastKey = key.getData();
            }
            status = readPrimaryAfterGet(key, pKey, data, lockMode, retries);
            if (status == OperationStatus.SUCCESS) {
                return status;
            }
            retries += 1;
            try {
                Thread.sleep(SLEEP_BEFORE_READ_PRIMARY_RETRY);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * Calls retrieveNext() and retrieves primary data.
     */
    OperationStatus retrieveNext(final DatabaseEntry key,
                                 final DatabaseEntry pKey,
                                 final DatabaseEntry data,
                                 final LockMode lockMode,
                                 final GetMode getMode)
        throws DatabaseException {

        /*
         * Perform retries to account for primary updates during a
         * read-uncommitted.
         */
        int retries = 0;
        byte[] lastKey = null;
        while (true) {
            OperationStatus status = retrieveNext(key, pKey, lockMode,
                                                  getMode);
            if (status != OperationStatus.SUCCESS) {
                return status;
            }
            if (!Arrays.equals(lastKey, key.getData())) {
                retries = 0;
                lastKey = key.getData();
            }
            status = readPrimaryAfterGet(key, pKey, data, lockMode, retries);
            if (status == OperationStatus.SUCCESS) {
                return status;
            }
            retries += 1;
            try {
                Thread.sleep(SLEEP_BEFORE_READ_PRIMARY_RETRY);
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * @see Cursor#readPrimaryAfterGet
     */
    private OperationStatus readPrimaryAfterGet(final DatabaseEntry key,
                                                final DatabaseEntry pKey,
                                                final DatabaseEntry data,
                                                final LockMode lockMode,
                                                final int retries)
        throws DatabaseException {

        return readPrimaryAfterGet(primaryDb, key, pKey, data, lockMode,
                                   retries);
    }

    /**
     * @see Cursor#checkForPrimaryUpdate
     */
    @Override
    boolean checkForPrimaryUpdate(final DatabaseEntry key,
                                  final DatabaseEntry pKey,
                                  final DatabaseEntry data,
                                  final int retries) {

        final SecondaryConfig conf = secondaryDb.getPrivateSecondaryConfig();
        boolean possibleIntegrityError = false;

        /*
         * If the secondary key is immutable, or the key creators are
         * null (the database is read only), then we can skip this
         * check.
         */
        if (conf.getImmutableSecondaryKey()) {
            /* Do nothing. */
        } else if (conf.getKeyCreator() != null) {

            /*
             * Check that the key we're using is equal to the key
             * returned by the key creator.
             */
            final DatabaseEntry secKey = new DatabaseEntry();
            if (!conf.getKeyCreator().createSecondaryKey
                    (secondaryDb, pKey, data, secKey) ||
                !secKey.equals(key)) {
                possibleIntegrityError = true;
            }
        } else if (conf.getMultiKeyCreator() != null) {

            /*
             * Check that the key we're using is in the set returned by
             * the key creator.
             */
            final Set<DatabaseEntry> results = new HashSet<DatabaseEntry>();
            conf.getMultiKeyCreator().createSecondaryKeys
                (secondaryDb, pKey, data, results);
            if (!results.contains(key)) {
                possibleIntegrityError = true;
            }
        }

        return possibleIntegrityError;
    }

    /**
     * Note that this flavor of checkArgs doesn't require that the dbt data is
     * set.
     */
    private void checkArgsNoValRequired(final DatabaseEntry key,
                                        final DatabaseEntry pKey,
                                        final DatabaseEntry data) {
        DatabaseUtil.checkForNullDbt(key, "key", false);
        DatabaseUtil.checkForNullDbt(pKey, "pKey", false);
        DatabaseUtil.checkForNullDbt(data, "data", false);
    }
}
