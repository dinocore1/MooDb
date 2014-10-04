/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.persist;

import java.util.Iterator;

/* <!-- begin JE only --> */
import com.sleepycat.je.CacheMode;
/* <!-- end JE only --> */
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.util.keyrange.RangeCursor;

/**
 * Implements EntityCursor and uses a ValueAdapter so that it can enumerate
 * either keys or entities.
 *
 * @author Mark Hayes
 */
class BasicCursor<V> implements EntityCursor<V> {

    RangeCursor cursor;
    ValueAdapter<V> adapter;
    boolean updateAllowed;
    DatabaseEntry key;
    DatabaseEntry pkey;
    DatabaseEntry data;

    BasicCursor(RangeCursor cursor,
                ValueAdapter<V> adapter,
                boolean updateAllowed) {
        this.cursor = cursor;
        this.adapter = adapter;
        this.updateAllowed = updateAllowed;
        key = adapter.initKey();
        pkey = adapter.initPKey();
        data = adapter.initData();
    }

    public V first()
        throws DatabaseException {

        return first(null);
    }

    public V first(LockMode lockMode)
        throws DatabaseException {

        return returnValue(cursor.getFirst(key, pkey, data, lockMode));
    }

    public V last()
        throws DatabaseException {

        return last(null);
    }

    public V last(LockMode lockMode)
        throws DatabaseException {

        return returnValue(cursor.getLast(key, pkey, data, lockMode));
    }

    public V next()
        throws DatabaseException {

        return next(null);
    }

    public V next(LockMode lockMode)
        throws DatabaseException {

        return returnValue(cursor.getNext(key, pkey, data, lockMode));
    }

    public V nextDup()
        throws DatabaseException {

        return nextDup(null);
    }

    public V nextDup(LockMode lockMode)
        throws DatabaseException {

        checkInitialized();
        return returnValue(cursor.getNextDup(key, pkey, data, lockMode));
    }

    public V nextNoDup()
        throws DatabaseException {

        return nextNoDup(null);
    }

    public V nextNoDup(LockMode lockMode)
        throws DatabaseException {

        return returnValue(cursor.getNextNoDup(key, pkey, data, lockMode));
    }

    public V prev()
        throws DatabaseException {

        return prev(null);
    }

    public V prev(LockMode lockMode)
        throws DatabaseException {

        return returnValue(cursor.getPrev(key, pkey, data, lockMode));
    }

    public V prevDup()
        throws DatabaseException {

        return prevDup(null);
    }

    public V prevDup(LockMode lockMode)
        throws DatabaseException {

        checkInitialized();
        return returnValue(cursor.getPrevDup(key, pkey, data, lockMode));
    }

    public V prevNoDup()
        throws DatabaseException {

        return prevNoDup(null);
    }

    public V prevNoDup(LockMode lockMode)
        throws DatabaseException {

        return returnValue(cursor.getPrevNoDup(key, pkey, data, lockMode));
    }

    public V current()
        throws DatabaseException {

        return current(null);
    }

    public V current(LockMode lockMode)
        throws DatabaseException {

        checkInitialized();
        return returnValue(cursor.getCurrent(key, pkey, data, lockMode));
    }

    public int count()
        throws DatabaseException {

        checkInitialized();
        return cursor.count();
    }

    /* <!-- begin JE only --> */
    public long countEstimate()
        throws DatabaseException {

        checkInitialized();
        return cursor.getCursor().countEstimate();
    }
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    /* for FUTURE use
    public long skipNext(long maxCount) {
        return skipNext(maxCount, null);
    }

    public long skipNext(long maxCount, LockMode lockMode) {
        checkInitialized();
        return cursor.getCursor().skipNext
            (maxCount, BasicIndex.NO_RETURN_ENTRY, BasicIndex.NO_RETURN_ENTRY,
             lockMode);
    }

    public long skipPrev(long maxCount) {
        return skipPrev(maxCount, null);
    }

    public long skipPrev(long maxCount, LockMode lockMode) {
        checkInitialized();
        return cursor.getCursor().skipPrev
            (maxCount, BasicIndex.NO_RETURN_ENTRY, BasicIndex.NO_RETURN_ENTRY,
             lockMode);
    }
    */
    /* <!-- end JE only --> */

    public Iterator<V> iterator() {
        return iterator(null);
    }

    public Iterator<V> iterator(LockMode lockMode) {
        return new BasicIterator(this, lockMode);
    }

    public boolean update(V entity)
        throws DatabaseException {

        if (!updateAllowed) {
            throw new UnsupportedOperationException
                ("Update not allowed on a secondary index");
        }
        checkInitialized();
        adapter.valueToData(entity, data);
        return cursor.putCurrent(data) == OperationStatus.SUCCESS;
    }

    public boolean delete()
        throws DatabaseException {

        checkInitialized();
        return cursor.delete() == OperationStatus.SUCCESS;
    }

    public EntityCursor<V> dup()
        throws DatabaseException {

        return new BasicCursor<V>(cursor.dup(true), adapter, updateAllowed);
    }

    public void close()
        throws DatabaseException {

        cursor.close();
    }

    /* <!-- begin JE only --> */
    public void setCacheMode(CacheMode cacheMode) {
        cursor.getCursor().setCacheMode(cacheMode);
    }
    /* <!-- end JE only --> */

    /* <!-- begin JE only --> */
    public CacheMode getCacheMode() {
        return cursor.getCursor().getCacheMode();
    }
    /* <!-- end JE only --> */

    void checkInitialized()
        throws IllegalStateException {

        if (!cursor.isInitialized()) {
            throw new IllegalStateException
                ("Cursor is not initialized at a valid position");
        }
    }

    V returnValue(OperationStatus status) {
        V value;
        if (status == OperationStatus.SUCCESS) {
            value = adapter.entryToValue(key, pkey, data);
        } else {
            value = null;
        }
        /* Clear entries to save memory. */
        adapter.clearEntries(key, pkey, data);
        return value;
    }
}
