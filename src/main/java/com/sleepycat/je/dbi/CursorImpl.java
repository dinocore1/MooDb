/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.logging.Level;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DuplicateDataException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINBoundary;
import com.sleepycat.je.tree.ChildReference;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.tree.LN;
import com.sleepycat.je.tree.SearchResult;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.TreeWalkerStatsAccumulator;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockInfo;
import com.sleepycat.je.txn.LockManager;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * A CursorImpl is the internal implementation of the cursor.
 */
public class CursorImpl implements Cloneable {

    private static final boolean DEBUG = false;

    private static final byte CURSOR_NOT_INITIALIZED = 1;
    private static final byte CURSOR_INITIALIZED = 2;
    private static final byte CURSOR_CLOSED = 3;
    private static final String TRACE_DELETE = "Delete";
    private static final String TRACE_MOD = "Mod:";
    private static final String TRACE_INSERT = "Ins:";

    /*
     * Cursor location in the databaseImpl, represented by a BIN and an index
     * in the BIN.  The bin is null if not established, and the index is
     * negative if not established.
     */
    volatile private BIN bin;
    volatile private int index;

    /*
     * BIN that is no longer referenced by this cursor but has not yet been
     * removed.  If non-null, the BIN will be removed soon.  BIN.adjustCursors
     * should ignore cursors that are to be removed.
     *
     * Removal of the prior BIN from the cursor is delayed to prevent its
     * compression and loss of the scan context.  [#12736]
     */
    volatile private BIN binToBeRemoved;

    /* The databaseImpl behind the handle. */
    private final DatabaseImpl databaseImpl;

    /* Owning transaction. */
    private Locker locker;
    private final boolean retainNonTxnLocks;

    /* State of the cursor. See CURSOR_XXX above. */
    private byte status;

    private CacheMode cacheMode;
    private boolean allowEviction;
    private TestHook testHook;

    /*
     * Unique id that we can return as a hashCode to prevent calls to
     * Object.hashCode(). [#13896]
     */
    private final int thisId;

    /*
     * Allocate hashCode ids from this. [#13896]
     */
    private static long lastAllocatedId = 0;

    private ThreadLocal<TreeWalkerStatsAccumulator> treeStatsAccumulatorTL;

    public enum SearchMode {
        SET(true, false, "SET"),
        BOTH(true, true, "BOTH"),
        SET_RANGE(false, false, "SET_RANGE"),
        BOTH_RANGE(false, true, "BOTH_RANGE");

        private final boolean exactSearch;
        private final boolean dataSearch;
        private final String name;

        private SearchMode(boolean exactSearch,
                           boolean dataSearch,
                           String name) {
            this.exactSearch = exactSearch;
            this.dataSearch = dataSearch;
            this.name = "SearchMode." + name;
        }

        /**
         * Returns true when the key or key/data search is exact, i.e., for SET
         * and BOTH.
         */
        public final boolean isExactSearch() {
            return exactSearch;
        }

        /**
         * Returns true when the data value is included in the search, i.e.,
         * for BOTH and BOTH_RANGE.
         */
        public final boolean isDataSearch() {
            return dataSearch;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Creates a cursor with retainNonTxnLocks=true.
     */
    public CursorImpl(DatabaseImpl database, Locker locker) {
        this(database, locker, true);
    }

    /**
     * Creates a cursor.
     *
     * A cursor always retains transactional locks when it is reset or closed.
     * Non-transaction locks may be retained or not, depending on the
     * retainNonTxnLocks parameter value.
     *
     * Normally a user-created non-transactional Cursor releases locks on reset
     * and close, and a ThreadLocker is normally used.  However, by passing
     * true for retainNonTxnLocks a ThreadLocker can be made to retain locks;
     * this capability is used by SecondaryCursor.readPrimaryAfterGet.
     *
     * For internal (non-user) cursors, a BasicLocker is often used and locks
     * are retained.  BasicLocker does not currently support releasing locks
     * per cursor operation, so true must be passed for retainNonTxnLocks. In
     * addition, in these internal use cases the caller explicitly calls
     * BasicLocker.operationEnd, and retainNonTxnLocks is set to true to
     * prevent operationEnd from being called when the cursor is closed.
     *
     * BasicLocker is also used for NameLN operations while opening a Database
     * handle.  Database handle locks must be retained, even if the Database is
     * opened non-transactionally.
     *
     * @param retainNonTxnLocks is true if non-transactional locks should be
     * retained (not released automatically) when the cursor is reset or
     * closed.
     */
    public CursorImpl(DatabaseImpl databaseImpl,
                      Locker locker,
                      boolean retainNonTxnLocks) {

        thisId = (int) getNextCursorId();
        bin = null;
        index = -1;

        this.retainNonTxnLocks = retainNonTxnLocks;

        /* Associate this cursor with the databaseImpl. */
        this.databaseImpl = databaseImpl;
        this.locker = locker;
        this.locker.registerCursor(this);

        /*
         * This default value is used only when the CursorImpl is used directly
         * (mainly for internal databases).  When the CursorImpl is created by
         * a Cursor, CursorImpl.setCacheMode will be called.
         */
        this.cacheMode = CacheMode.DEFAULT;

        status = CURSOR_NOT_INITIALIZED;

        /*
         * Do not perform eviction here because we may be synchronized on the
         * Database instance. For example, this happens when we call
         * Database.openCursor().  Also eviction may be disabled after the
         * cursor is constructed.
         */
    }

    /*
     * Allocate a new hashCode id.  Doesn't need to be synchronized since it's
     * ok for two objects to have the same hashcode.
     */
    private static long getNextCursorId() {
        return ++lastAllocatedId;
    }

    private void setInitialized() {
        status = CURSOR_INITIALIZED;
    }

    @Override
    public int hashCode() {
        return thisId;
    }

    private void maybeInitTreeStatsAccumulator() {
        if (treeStatsAccumulatorTL == null) {
            treeStatsAccumulatorTL =
                new ThreadLocal<TreeWalkerStatsAccumulator>();
        }
    }

    private TreeWalkerStatsAccumulator getTreeStatsAccumulator() {
        if (EnvironmentImpl.getThreadLocalReferenceCount() > 0) {
            maybeInitTreeStatsAccumulator();
            return treeStatsAccumulatorTL.get();
        } else {
            return null;
        }
    }

    public void incrementLNCount() {
        TreeWalkerStatsAccumulator treeStatsAccumulator =
            getTreeStatsAccumulator();
        if (treeStatsAccumulator != null) {
            treeStatsAccumulator.incrementLNCount();
        }
    }

    /**
     * Disables or enables eviction during cursor operations.  For example, a
     * cursor used to implement eviction (e.g., in some UtilizationProfile and
     * most DbTree and VLSNIndex methods) should not itself perform eviction,
     * but eviction should be enabled for user cursors.  Eviction is disabled
     * by default.
     */
    public void setAllowEviction(boolean allowed) {
        allowEviction = allowed;
    }

    public void criticalEviction() {

        /*
         * In addition to disabling critical eviction for internal cursors (see
         * setAllowEviction above), we do not perform critical eviction when
         * EVICT_BIN or MAKE_COLD is used.  Operations using MAKE_COLD and
         * EVICT_BIN generally do not add any net memory to the cache, so they
         * shouldn't have to perform critical eviction or block while another
         * thread performs eviction.
         */
        if (allowEviction &&
            cacheMode != CacheMode.MAKE_COLD &&
            cacheMode != CacheMode.EVICT_BIN) {
            databaseImpl.getDbEnvironment().
                criticalEviction(false /*backgroundIO*/);
        }
    }

    /**
     * Shallow copy.  addCursor() is optionally called.
     */
    public CursorImpl cloneCursor(boolean addCursor, CacheMode cacheMode)
        throws DatabaseException {

        return cloneCursor(addCursor, cacheMode, null /*usePosition*/);
    }

    /**
     * Performs a shallow copy.
     *
     * @param addCursor If true, addCursor() is called to register the new
     * cursor with the BINs.  This is done after the usePosition parameter is
     * applied, if any.  There are two cases where you may not want addCursor()
     * to be called: 1) When creating a fresh uninitialized cursor as in when
     * Cursor.dup(false) is called, or 2) when the caller will call addCursor()
     * as part of a larger operation.
     *
     * @param usePosition Is null to duplicate the position of this cursor, or
     * non-null to duplicate the position of the given cursor.
     */
    public CursorImpl cloneCursor(final boolean addCursor,
                                  final CacheMode cacheMode,
                                  final CursorImpl usePosition)
        throws DatabaseException {

        CursorImpl ret = null;
        try {
            latchBIN();
            ret = (CursorImpl) super.clone();
            /* Must set cache mode before calling criticalEviction. */
            ret.setCacheMode(cacheMode);

            if (!retainNonTxnLocks) {
                ret.locker = locker.newNonTxnLocker();
            }

            ret.locker.registerCursor(ret);
            if (usePosition != null &&
                usePosition.status == CURSOR_INITIALIZED) {
                ret.bin = usePosition.bin;
                ret.index = usePosition.index;
            }

            if (addCursor) {
                ret.addCursor();
            }
        } catch (CloneNotSupportedException cannotOccur) {
            return null;
        } finally {
            releaseBIN();
        }

        /* Perform eviction before and after each cursor operation. */
        criticalEviction();

        return ret;
    }

    /**
     * Called when a cursor has been duplicated prior to being moved.  The new
     * locker is informed of the old locker, so that a preempted lock taken by
     * the old locker can be ignored. [#16513]
     *
     * @param closingCursor the old cursor that will be closed if the new
     * cursor is moved successfully.
     */
    public void setClosingLocker(CursorImpl closingCursor) {

        /*
         * If the two lockers are different, then the old locker will be closed
         * when the operation is complete.  This is currently the case only for
         * ReadCommitted cursors and non-transactional cursors that do not
         * retain locks.
         */
        if (!retainNonTxnLocks && locker != closingCursor.locker) {
            locker.setClosingLocker(closingCursor.locker);
        }
    }

    /**
     * Called when a cursor move operation is complete.  Clears the
     * closingLocker so that a reference to the old closed locker is not held.
     */
    public void clearClosingLocker() {
        locker.setClosingLocker(null);
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int idx) {
        index = idx;
    }

    public BIN getBIN() {
        return bin;
    }

    public void setBIN(BIN newBin) {

        /*
         * Historical note. In the past we checked here that the cursor was
         * removed for the prior BIN by calling BIN.containsCursor [#16280].
         * Because the containsCursor method takes a latch on the prior BIN,
         * this causes a rare latch deadlock when newBin is latched (during an
         * insert, for example), since this thread will latch two BINs in
         * arbitrary order; so the assertion was removed [#21395].
         */
        bin = newBin;
    }

    public BIN getBINToBeRemoved() {
        return binToBeRemoved;
    }

    public CacheMode getCacheMode() {
        return cacheMode;
    }

    /**
     * Sets the effective cache mode to use for the next operation.  The
     * cacheMode field will never be set to null or DYNAMIC, and can be passed
     * directly to latching methods.
     *
     * @see #performCacheEviction
     */
    public void setCacheMode(final CacheMode mode) {
        cacheMode = databaseImpl.getEffectiveCacheMode(mode);
    }

    public void setTreeStatsAccumulator(TreeWalkerStatsAccumulator tSA) {
        maybeInitTreeStatsAccumulator();
        treeStatsAccumulatorTL.set(tSA);
    }

    /**
     * Advance a cursor.  Used so that verify can advance a cursor even in the
     * face of an exception [12932].
     * @param key on return contains the key if available, or null.
     * @param data on return contains the data if available, or null.
     */
    public boolean advanceCursor(DatabaseEntry key, DatabaseEntry data) {

        BIN oldBin = bin;
        int oldIndex = index;

        key.setData(null);
        data.setData(null);

        try {
            getNext(key, data, LockType.NONE,
                    true /*forward*/,
                    false /*alreadyLatched*/,
                    null /*rangeConstraint*/);
        } catch (DatabaseException ignored) {
            /* Klockwork - ok */
        }

        /*
         * If the position changed, regardless of an exception, then we believe
         * that we have advanced the cursor.
         */
        if (bin != oldBin ||
            index != oldIndex) {

            /*
             * Return the key and data from the BIN entries, if we were not
             * able to read it above.
             */
            if (key.getData() == null &&
                bin != null &&
                index > 0) {
                LN.setEntry(key, bin.getKey(index));
            }
            return true;
        } else {
            return false;
        }
    }

    public BIN latchBIN()
        throws DatabaseException {

        while (bin != null) {
            BIN waitingOn = bin;
            waitingOn.latch(cacheMode);
            if (bin == waitingOn) {
                return bin;
            }
            waitingOn.releaseLatch();
        }

        return null;
    }

    public void releaseBIN() {
        if (bin != null) {
            bin.releaseLatchIfOwner();
        }
    }

    public Locker getLocker() {
        return locker;
    }

    void addCursor(BIN bin) {
        if (bin != null) {
            assert bin.isLatchOwnerForWrite();
            bin.addCursor(this);
        }
    }

    /**
     * Add to the current cursor.
     */
    void addCursor() {
        if (bin != null) {
            addCursor(bin);
        }
    }

    /*
     * Update a cursor to refer to a new BIN following an insert.  Don't bother
     * removing this cursor from the previous bin.  Cursor will do that with a
     * cursor swap thereby preventing latch deadlocks down here.
     */
    public void updateBin(int index)
        throws DatabaseException {

        setIndex(index);
        setBIN(bin);
        addCursor(bin);
    }

    private void removeCursor()
        throws DatabaseException {

        BIN abin = latchBIN();
        if (abin != null) {
            abin.removeCursor(this);
            abin.releaseLatch();
        }
    }

    public void dumpTree() {
        databaseImpl.getTree().dump();
    }

    /**
     * @return true if this cursor is closed
     */
    public boolean isClosed() {
        return (status == CURSOR_CLOSED);
    }

    /**
     * @return true if this cursor is not initialized
     */
    public boolean isNotInitialized() {
        return (status == CURSOR_NOT_INITIALIZED);
    }

    public boolean isInternalDbCursor() {
        return databaseImpl.isInternalDb();
    }

    /**
     * Reset a cursor to an uninitialized state, but unlike close(), allow it
     * to be used further.
     */
    public void reset()
        throws DatabaseException {

        /* Must remove cursor before evicting BIN and releasing locks. */
        removeCursor();
        performCacheEviction(null /*newCursor*/);

        if (!retainNonTxnLocks) {
            locker.releaseNonTxnLocks();
        }

        bin = null;
        index = -1;

        status = CURSOR_NOT_INITIALIZED;

        /* Perform eviction before and after each cursor operation. */
        criticalEviction();
    }

    public void close()
        throws DatabaseException {

        close(null);
    }

    /**
     * Close a cursor.
     *
     * @param newCursor is another cursor that is kept open by the parent
     * Cursor object, or null if no other cursor is kept open.
     *
     * @throws DatabaseException if the cursor was previously closed.
     */
    public void close(final CursorImpl newCursor)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        /* Must remove cursor before evicting BIN and releasing locks. */
        removeCursor();
        performCacheEviction(newCursor);

        locker.unRegisterCursor(this);

        if (!retainNonTxnLocks) {
            locker.nonTxnOperationEnd();
        }

        status = CURSOR_CLOSED;

        /* Perform eviction before and after each cursor operation. */
        criticalEviction();
    }

    /**
     * When multiple operations are performed, CacheMode-based eviction is
     * performed for a given operation at the end of the next operation, which
     * calls close() or reset() on the CursorImpl of the previous operation.
     * Eviction for the last operation (including when only one operation is
     * performed) also occurs during Cursor.close(), which calls
     * CursorImpl.close().
     *
     * By default, the CacheMode returned by DatabaseImpl.getCacheMode is used,
     * and the defaults specified by the user for the Database or Environment
     * are applied.  However, the default mode can be overridden by the user by
     * calling Cursor.setCacheMode, and the mode may be changed prior to each
     * operation, if desired.
     *
     * To implement a per-operation CacheMode, two CacheMode fields are
     * maintained.  Cursor.cacheMode is the mode to use for the next operation.
     * CursorImpl.cacheMode is the mode that was used for the previous
     * operation, and that is used for eviction when that CursorImpl is closed
     * or reset.
     */
    private void performCacheEviction(final CursorImpl newCursor) {
        EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();
        if (cacheMode != CacheMode.EVICT_LN &&
            cacheMode != CacheMode.EVICT_BIN &&
            (cacheMode != CacheMode.MAKE_COLD ||
             !(envImpl.isCacheFull() || envImpl.wasCacheEverFull()))) {
            /* Eviction not configured, or for MAKE_COLD, cache never full. */
            return;
        }
        if (bin == null) {
            /* Nothing to evict. */
            return;
        }
        final BIN nextBin;
        final int nextIndex;
        if (newCursor != null) {
            nextBin = newCursor.bin;
            nextIndex = newCursor.index;
        } else {
            nextBin = null;
            nextIndex = -1;
        }
        switch (cacheMode) {
        case EVICT_LN:
            /* Evict the LN if we've moved to a new record. */
            if (bin != nextBin || index != nextIndex) {
                evict();
            }
            break;
        case EVICT_BIN:
            if (bin == nextBin) {
                /* BIN has not changed, do not evict it.  May evict the LN. */
                if (index != nextIndex) {
                    evict();
                }
            } else {
                /* BIN has changed, evict it. */
                envImpl.getEvictor().doEvictOneIN
                    (bin, EvictionSource.CACHEMODE);
            }
            break;
        case MAKE_COLD:
            if (bin == nextBin || !envImpl.isCacheFull()) {

                /*
                 * If BIN has not changed, do not evict it, but may still evict
                 * the LN. If cache not full, use LN eviction instead.
                 */
                if (index != nextIndex) {
                    evict();
                }
            } else {
                /* BIN has changed, evict it. */
                envImpl.getEvictor().doEvictOneIN
                    (bin, EvictionSource.CACHEMODE);
            }
            break;
        default:
            assert false;
        }
    }

    /**
     * Return a new copy of the cursor.
     *
     * @param samePosition If true, position the returned cursor at the same
     * position as this cursor; if false, return a new uninitialized cursor.
     */
    public CursorImpl dup(boolean samePosition)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        CursorImpl ret = cloneCursor(samePosition /*addCursor*/, cacheMode);

        if (!samePosition) {
            ret.bin = null;
            ret.index = -1;
            ret.status = CURSOR_NOT_INITIALIZED;
        }

        return ret;
    }

    /**
     * Evict the LN node at the cursor position.
     */
    public void evict()
        throws DatabaseException {

        evict(false); // alreadyLatched
    }

    /**
     * Evict the LN node at the cursor position.
     */
    public void evict(boolean alreadyLatched)
        throws DatabaseException {

        try {
            if (!alreadyLatched) {
                latchBIN();
            }
            if (index >= 0) {
                bin.evictLN(index);
            }
        } finally {
            if (!alreadyLatched) {
                releaseBIN();
            }
        }
    }

    /*
     * Put and Delete
     */

    /**
     * Performs all put operations except for CURRENT (use putCurrent instead).
     */
    public OperationStatus put(final DatabaseEntry key,
                               final DatabaseEntry data,
                               final LN ln,
                               final PutMode putMode,
                               final DatabaseEntry returnOldData,
                               final DatabaseEntry returnNewData,
                               final ReplicationContext repContext) {
        assert key != null;
        assert data != null;
        assert ln != null;
        assert putMode != null;
        assert assertCursorState(false) : dumpToString(true);
        assert LatchSupport.countLatchesHeld() == 0;

        final boolean allowOverwrite;

        switch (putMode) {
        case NO_OVERWRITE:
            allowOverwrite = false;
            break;
        case OVERWRITE:
            allowOverwrite = true;
            break;
        default:
            throw EnvironmentFailureException.unexpectedState
                (putMode.toString());
        }

        return putInternal
            (ln, Key.makeKey(key), allowOverwrite, data, returnOldData,
             returnNewData, repContext);
    }

    /**
     * Insert the given LN in the tree or return KEYEXIST if the key is already
     * present.
     *
     * <p>This method is called directly internally for putting tree map LNs
     * and file summary LNs.  It should not be used otherwise, and in the
     * future we should find a way to remove this special case.</p>
     *
     * @param returnNewData if non-null, is used to return a complete copy of
     * the resulting data after any partial data has been resolved.
     */
    public OperationStatus putLN(byte[] key,
                                 LN ln,
                                 DatabaseEntry returnNewData,
                                 ReplicationContext repContext)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);
        assert LatchSupport.countLatchesHeld() == 0;

        return putInternal
            (ln, key, false /*allowOverwrite*/, null /*overwriteData*/,
             null /*returnOldData*/, returnNewData, repContext);
    }

    /**
     * Insert the record, or reuse a slot containing a deleted record, or
     * update an existing record. Position the cursor as a side effect.
     *
     * @param ln The LN to insert into the tree.
     * @param key Key value for the node
     *
     * @throws IllegalArgumentException via db/cursor put methods
     */
    private OperationStatus putInternal(LN ln,
                                        byte[] key,
                                        boolean allowOverwrite,
                                        DatabaseEntry overwriteData,
                                        DatabaseEntry returnOldData,
                                        DatabaseEntry returnNewData,
                                        ReplicationContext repContext) {

        if (returnOldData != null) {
            returnOldData.setData(null);
        }
        if (returnNewData != null) {
            returnNewData.setData(null);
        }

        /* Find, latch and move cursor to the relevant BIN. */
        databaseImpl.getTree().findBinForInsert(key, this);
        assert getBIN().isLatchOwnerForWrite();
        try {

            /* If we can insert the LN in a new slot in the BIN, we're done. */
            if (insertNewSlot(ln, key, repContext)) {
                finishInsert(ln, returnNewData);
                return OperationStatus.SUCCESS;
            }

            /*
             * Now get enough info to decide whether to reuse an existing slot
             * containing a deleted entry.
             */
            final SlotReuseInfo slotInfo = getSlotReuseInfo(ln, key);

            /* If we can reuse an existing slot, we're done. */
            if (slotInfo.slotReuseAllowed) {
                reuseExistingSlot(ln, key, repContext, slotInfo.lockStanding);
                finishInsert(ln, returnNewData);
                return OperationStatus.SUCCESS;
            }

            /* Cannot insert. */
            if (!allowOverwrite) {
                return OperationStatus.KEYEXIST;
            }

            /* Cursor is positioned on existing record. */
            setInitialized();

            /*
             * Update the non-deleted record at the cursor position.  We have
             * optimized by preferring to take an uncontended lock.  The
             * slotInfo.lockStanding field is guaranteed to be non-null in this
             * case.  The BIN must remain latched when calling this method.
             */
            return putCurrentAlreadyLatchedAndLocked
                (key, overwriteData, null /*foundKey*/, returnOldData,
                 returnNewData, slotInfo.lockStanding, repContext);
        } finally {
            releaseBIN();
        }
    }

    private void finishInsert(LN ln, DatabaseEntry returnNewData) {
        if (returnNewData != null) {
            ln.setEntry(returnNewData);
        }
        /* Cursor is positioned on new record. */
        setInitialized();

        /*
         * It is desirable to evict the LN in a duplicates DB because it will
         * never be fetched again.  But for deferred-write DBs we should not
         * evict a dirty LN since it may be logged unnecessarily.
         */
        if (databaseImpl.getSortedDuplicates() &&
            !databaseImpl.isDeferredWriteMode() &&
            bin.getTarget(index) != null) {
            bin.evictLN(index);
        }
    }

    /**
     * Attempt to insert a new slot (meaning a new key) in the BIN and return
     * true if successful.  If false is returned, then an existing slot with
     * the new LN's key was found, and the cursor will be set to the index of
     * that slot.
     */
    private boolean insertNewSlot(final LN ln,
                                  final byte[] key,
                                  final ReplicationContext repContext) {
        final EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();

        /*
         * Prepare to undo, in case logging succeeds but locking fails, in the
         * case where we insert a new slot below.  Call this method BEFORE slot
         * insertion, in case it throws an exception which would leave the slot
         * with a null LSN.
         */
        locker.preLogWithoutLock(databaseImpl);
        
        /* Make a child reference as a candidate for insertion. */
        final ChildReference newLNRef =
            new ChildReference(ln, key, DbLsn.NULL_LSN);

        addCursor();

        int insertIndex = bin.insertEntry1(newLNRef);
        if ((insertIndex & IN.INSERT_SUCCESS) == 0) {

            /*
             * Entry exists. Insertion was not successful.  Move cursor to
             * potential reuse/update index before returning.
             */
            setIndex(insertIndex & ~IN.EXACT_MATCH);
            return false;
        }

        /*
         * Update the cursor to point to the entry that has been successfully
         * inserted.
         */
        setIndex(insertIndex &= ~IN.INSERT_SUCCESS);

        /* Log the new LN. */
        final WriteLockInfo writeLockInfo = LockStanding.prepareForInsert();
        long newLsn = DbLsn.NULL_LSN;
        try {
            newLsn = ln.optionalLog
                (envImpl, databaseImpl, key, null /*oldKey*/, DbLsn.NULL_LSN,
                 locker, writeLockInfo, repContext);
        } finally {
            if (newLsn == DbLsn.NULL_LSN) {

                /*
                 * Possible buffer overflow, out-of-memory, or I/O exception
                 * during logging.  The BIN entry will contain a NULL_LSN.  To
                 * prevent an exception during a fetch, we set the KnownDeleted
                 * flag.  We do not call BIN.deleteEntry because cursors will
                 * not be adjusted.  We do not add this entry to the compressor
                 * queue to avoid complexity (this is rare).
                 * [13126, 12605, 11271]
                 */
                bin.setKnownDeleted(index);
            }
        }

        /* Update LSN in BIN slot.  The LN is already in the slot. */
        bin.updateEntry(index, newLsn);

        /*
         * Account for FileSummaryLN's extra marshaled memory. [#17462]
         *
         * To avoid violating assertions (e.g., in IN.changeMemorySize), we
         * must finish the memory adjustment while the BIN is still latched.
         * [#20069]
         *
         * This special handling does not apply to slot reuse.  It only applies
         * when the LN is placed in the slot before logging.  [#20845]
         */
        ln.addExtraMarshaledMemorySize(bin);

        traceInsert(Level.FINER, bin, newLsn, index);
        return true;
    }

    /**
     * Replaces the contents of the existing slot (at the cursor position) with
     * the new LN.
     */
    private void reuseExistingSlot(final LN ln,
                                   final byte[] key,
                                   final ReplicationContext repContext,
                                   final LockStanding lockStanding) {

        final EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();

        /*
         * Current entry is a deleted entry. Replace it with LN.  Pass NULL_LSN
         * for the oldLsn parameter of the log() method because the old LN was
         * counted obsolete when it was deleted.
         */
        final long newLsn = ln.optionalLog(envImpl,
                                           databaseImpl,
                                           key,
                                           null /*oldKey*/,
                                           DbLsn.NULL_LSN,
                                           locker,
                                           lockStanding.prepareForUpdate(),
                                           repContext);

        /*
         * Update BIN.
         *
         * When reusing a slot, the key is replaced in the BIN slot.  This
         * ensures that the correct key value is used when the new key is
         * non-identical to the key in the slot but is considered equal by the
         * btree comparator.
         * [#15704]
         */
        bin.prepareForSlotReuse(index);
        bin.updateEntry(index, ln, newLsn, key);
        bin.clearKnownDeleted(index);
        bin.clearPendingDeleted(index);

        traceInsert(Level.FINER, bin, newLsn, index);
    }

    /* Holds information during insert to evaluate potential slot reuse. */
    private class SlotReuseInfo {

        /*
         * Is true if the existing slot contains a deleted LN, or the remnant
         * of a cleaned LN, and the slot may be used for the new LN.  Is false
         * if the slot contains a non-deleted LN.
         */
        boolean slotReuseAllowed;

        /*
         * If slotReuseAllowed is true, then this contains lock info for the
         * LN.
         */
        LockStanding lockStanding;
    }

    /**
     * Returns information about whether the slot at the cursor position can be
     * reused for a new LN.  When called, the slot at the cursor position has
     * the same key as the new LN.  On return, the cursor BIN and index may be
     * updated if they changed while attempting to lock the LN.
     */
    private SlotReuseInfo getSlotReuseInfo(final LN ln, final byte[] key) {

        /*
         * Lock the LSN for the existing LN slot, and check deleted-ness.
         *
         * An uncontended lock is permitted because we will log a new LN
         * (either reuse the slot or update the LN) before releasing the BIN
         * latch.
         *
         * Be careful not to fetch, in order to avoid a random read during an
         * update operation.
         */
        LockStanding lockStanding =
            lockLN(LockType.WRITE, true /*allowUncontended*/);

        /* Determine whether current LN is deleted/cleaned. */
        boolean isDeleted = !lockStanding.recordExists();

        /* Initialize slot info. */
        final SlotReuseInfo slotInfo = new SlotReuseInfo();
        slotInfo.lockStanding = lockStanding;

        if (isDeleted) {
            /* We can reuse a deleted LN slot. */
            slotInfo.slotReuseAllowed = true;
            assert lockStanding != null;
        } else {
            /* Can't reuse the slot if it contains a a non-deleted LN. */
            slotInfo.slotReuseAllowed = false;
        }
        return slotInfo;
    }

    /**
     * Modify the current record with the given data, and optionally replace
     * the key.
     *
     * @param key to overwrite, may be null.  If non-null, must be previously
     * determined to be equal to the current key, according to the comparator.
     *
     * @param data to overwrite, may be partial.
     *
     * @param foundKey if non-null, is used to return a copy of the existing
     * key, may be partial.
     *
     * @param foundData if non-null, is used to return a copy of the existing
     * data, may be partial.
     *
     * @param returnNewData if non-null, is used to return a complete copy of
     * the resulting data after any partial data has been resolved.
     */
    public OperationStatus putCurrent(byte[] key,
                                      DatabaseEntry data,
                                      DatabaseEntry foundKey,
                                      DatabaseEntry foundData,
                                      DatabaseEntry returnNewData,
                                      ReplicationContext repContext) {

        assert assertCursorState(true) : dumpToString(true);

        if (foundKey != null) {
            foundKey.setData(null);
        }
        if (foundData != null) {
            foundData.setData(null);
        }
        if (returnNewData != null) {
            returnNewData.setData(null);
        }

        if (bin == null) {
            return OperationStatus.NOTFOUND;
        }

        final LockStanding lockStanding;
        boolean lockSuccess = false;
        latchBIN();
        try {
            /* Get a write lock. */
            lockStanding = lockLN(LockType.WRITE, true /*allowUncontended*/);
            if (!lockStanding.recordExists()) {
                /* LN was deleted or cleaned. */
                revertLock(lockStanding);
                return OperationStatus.NOTFOUND;
            }
            lockSuccess = true;
        } finally {
            if (!lockSuccess) {
                /* Exception in flight or early return. */
                releaseBIN();
            }
        }

        return putCurrentAlreadyLatchedAndLocked
            (key, data, foundKey, foundData, returnNewData, lockStanding,
             repContext);
    }

    /**
     * Variant of putCurrent called by Tree.put with BINs already latched and
     * non-deleted LN already locked.
     */
    private OperationStatus
        putCurrentAlreadyLatchedAndLocked(byte[] key,
                                          DatabaseEntry data,
                                          DatabaseEntry foundKey,
                                          DatabaseEntry foundData,
                                          DatabaseEntry returnNewData,
                                          LockStanding lockStanding,
                                          ReplicationContext repContext) {

        final EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();
        final DbType dbType = databaseImpl.getDbType();

        assert lockStanding.recordExists();
        final long oldLsn = lockStanding.lsn;
        assert oldLsn != DbLsn.NULL_LSN;
        final long newLsn;

        try {

            /*
             * Must fetch LN if any of the following are true:
             *  - foundData is non-null: data needs to be returned
             *  - data is a partial entry: needs to be resolved
             *  - CLEANER_FETCH_OBSOLETE_SIZE is configured
             *  - this database does not use the standard LN class and we
             *    cannot call DbType.createdUpdatedLN further below
             * For other cases, we are careful not to fetch, in order to avoid
             * a random read during an update operation.
             *
             * Note that we checked for a deleted/cleaned LN above, so we know
             * that fetchTarget will not return null.
             */
            LN ln;
            if (foundData != null ||
                data.getPartial() ||
                envImpl.getCleaner().getFetchObsoleteSize() ||
                !dbType.mayCreateUpdatedLN()) {
                /* Must fetch. */
                ln = (LN) bin.fetchTarget(index);
            } else {
                /* If resident, use LN for obsolete size tracking. */
                ln = (LN) bin.getTarget(index);
            }
            final byte[] oldKey = bin.getKey(index);
            final byte[] foundDataBytes = (ln != null) ? ln.getData() : null;
            final byte[] newData = data.getPartial() ?
                LN.resolvePartialEntry(data, foundDataBytes) :
                LN.copyEntryData(data);

            /*
             * If the key is changed (according to the comparator), we assume
             * it is actually the data that has changed via putCurrent() for a
             * duplicate's DB.  It is not possible to change the key in a
             * non-duplicates DB, because 1) putCurrent() is not passed the
             * key, 2) for put() the key is used to perform the lookup.
             */
            if (key != null &&
                Key.compareKeys
                    (oldKey, key, databaseImpl.getKeyComparator()) != 0) {
                throw new DuplicateDataException
                    ("Can't replace a duplicate with data that is " +
                     "unequal according to the duplicate comparator.");
            }

            if (foundData != null) {
                assert foundDataBytes != null;
                LN.setEntry(foundData, foundDataBytes);
            }
            if (foundKey != null) {
                LN.setEntry(foundKey, oldKey);
            }

            /*
             * Update the existing LN, if resident, else create a new updated
             * LN.
             */
            final long oldLNMemSize;
            if (ln != null) { 
                /* LN is already resident, modify its data. */
                oldLNMemSize = ln.getMemorySizeIncludedByParent();
                ln.modify(newData);
            } else {
                /* LN is not resident, create updated LN for logging. */
                ln = dbType.createUpdatedLN(envImpl, newData);
                /* Make updated LN resident. */
                bin.updateNode(index, ln, null /*lnSlotKey*/);
                oldLNMemSize = ln.getMemorySizeIncludedByParent();
            }

            /*
             * Log the updated LN.
             *
             * Note that if the LN is not resident, the lastLoggedSize is
             * unknown and not counted during utilization tracking.
             */
            newLsn = ln.optionalLog
                (envImpl, databaseImpl, (key != null) ? key : oldKey, oldKey,
                 oldLsn, locker, lockStanding.prepareForUpdate(), repContext);

            /* Return a copy of resulting data, if requested. [#16932] */
            if (returnNewData != null) {
                ln.setEntry(returnNewData);
            }

            /*
             * Update the parent BIN.  Update the key, if changed.  [#15704]
             */
            bin.updateNode(index, oldLNMemSize, newLsn, key /*lnSlotKey*/,
                           ln /*nodeForLnSlotKey*/);

            /*
             * It is desirable to evict the LN in a duplicates DB because it
             * will never be fetched again.  But for deferred-write DBs we
             * should not evict a dirty LN since it may be logged
             * unnecessarily.
             */
            if (databaseImpl.getSortedDuplicates() &&
                !databaseImpl.isDeferredWriteMode() &&
                bin.getTarget(index) != null) {
                bin.evictLN(index);
            }
        } finally {
            releaseBIN();
        }

        /* Trace after releasing latches. */
        trace(Level.FINER, TRACE_MOD, bin, index, oldLsn, newLsn);

        return OperationStatus.SUCCESS;
    }

    /**
     * Delete the item pointed to by the cursor. If cursor is not initialized
     * or item is already deleted, return appropriate codes. Returns with
     * nothing latched.
     *
     * @return 0 on success, appropriate error code otherwise.
     */
    public OperationStatus delete(ReplicationContext repContext)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);
        final EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();
        final DbType dbType = databaseImpl.getDbType();

        if (bin == null) {
            return OperationStatus.KEYEMPTY;
        }

        final long oldLsn;
        final long newLsn;

        latchBIN();
        try {

            /*
             * Get a write lock.  An uncontended lock is permitted because we
             * will log a new LN before releasing the BIN latch.
             */
            final LockStanding lockStanding =
                lockLN(LockType.WRITE, true /*allowUncontended*/);
            if (!lockStanding.recordExists()) {
                revertLock(lockStanding);
                /* LN was deleted or cleaned. */
                return OperationStatus.KEYEMPTY;
            }
            oldLsn = lockStanding.lsn;
            assert oldLsn != DbLsn.NULL_LSN;

            /*
             * Must fetch LN if any of the following are true:
             *  - CLEANER_FETCH_OBSOLETE_SIZE is configured
             *  - this database does not use the standard LN class and we
             *    cannot call DbType.createdDeletedLN further below
             * For other cases, we are careful not to fetch, in order to avoid
             * a random read during a delete operation.
             *
             * Note that we checked for a deleted/cleaned LN above, so we know
             * that fetchTarget will not return null.
             */
            LN ln;
            if (envImpl.getCleaner().getFetchObsoleteSize() ||
                !dbType.mayCreateDeletedLN()) {
                /* Must fetch. */
                ln = (LN) bin.fetchTarget(index);
            } else {
                /* If resident, use LN for obsolete size tracking. */
                ln = (LN) bin.getTarget(index);
            }

            /*
             * Make the existing LN deleted, if resident, else create a new
             * deleted LN.
             */
            final byte[] oldKey = bin.getKey(index);
            final long oldLNMemSize;
            if (ln != null) { 
                /* Leave LN resident, make it deleted. */
                oldLNMemSize = ln.getMemorySizeIncludedByParent();
                ln.delete();
            } else {
                /* LN is not resident, create deleted LN for logging. */
                ln = dbType.createDeletedLN(envImpl);
                /* Make deleted LN resident. */
                oldLNMemSize = ln.getMemorySizeIncludedByParent();
                bin.updateNode(index, ln, null /*lnSlotKey*/);
            }

            /*
             * Log the deleted LN.
             *
             * Note that if the LN is not resident, the lastLoggedSize is
             * unknown and not counted during utilization tracking.
             */
            newLsn = ln.optionalLog
                (envImpl, databaseImpl, oldKey, oldKey, oldLsn, locker,
                 lockStanding.prepareForUpdate(), repContext);

            /*
             * Now update the parent BIN of the LN to correctly reference the
             * LN and adjust the memory sizing.
             */
            bin.updateNode(index, oldLNMemSize, newLsn, null /*lnSlotKey*/,
                           ln /*nodeForLnSlotKey*/);
            bin.setPendingDeleted(index);

            /*
             * It is desirable to evict the LN after deletion because it will
             * never be fetched again.  This is especially true because slot
             * compression is deferred until a full BIN is logged.  But for
             * deferred-write DBs we should not evict a dirty LN since it may
             * be logged unnecessarily.
             */
            if (!databaseImpl.isDeferredWriteMode() &&
                bin.getTarget(index) != null) {
                bin.evictLN(index);
            }

            locker.addDeleteInfo(bin);
        } finally {
            releaseBIN();
        }

        trace(Level.FINER, TRACE_DELETE, bin, index, oldLsn, newLsn);

        return OperationStatus.SUCCESS;
    }

    /*
     * Gets
     */

    /**
     * Retrieve the current record.
     */
    public OperationStatus getCurrent(DatabaseEntry foundKey,
                                      DatabaseEntry foundData,
                                      LockType lockType)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        // If not pointing at valid entry, return failure
        if (bin == null) {
            return OperationStatus.KEYEMPTY;
        }

        latchBIN();

        return getCurrentAlreadyLatched(foundKey, foundData, lockType);
    }

    /**
     * Retrieve the current record. Assume the bin is already latched.  Return
     * with the target bin unlatched.
     */
    public OperationStatus getCurrentAlreadyLatched(DatabaseEntry foundKey,
                                                    DatabaseEntry foundData,
                                                    LockType lockType)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);
        assert checkAlreadyLatched(true) : dumpToString(true);

        try {
            return fetchCurrent(foundKey, foundData, lockType);
        } finally {
            releaseBIN();
        }
    }

    /**
     * Retrieve the current LN, return with the target bin unlatched.
     */
    public LN getCurrentLN(LockType lockType)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);

        if (bin == null) {
            return null;
        } else {
            latchBIN();
            return getCurrentLNAlreadyLatched(lockType);
        }
    }

    /**
     * Retrieve the current LN, assuming the BIN is already latched.  Return
     * with the target BIN unlatched.
     */
    public LN getCurrentLNAlreadyLatched(LockType lockType)
        throws DatabaseException {

        try {
            assert assertCursorState(true) : dumpToString(true);
            assert checkAlreadyLatched(true) : dumpToString(true);

            if (bin == null) {
                return null;
            }

            addCursor(bin);

            LockStanding lockStanding = lockLN(lockType);
            if (!lockStanding.recordExists()) {
                revertLock(lockStanding);
                return null;
            }

            return (LN) bin.fetchTarget(index);

        } finally {
            releaseBIN();
        }
    }

    /**
     * Returns the VLSN and LSN for the record at the current position.  Must
     * be called when the cursor is positioned on a record.
     *
     * @param allowFetch is true to fetch the LN to get the VLSN, or false to
     * return -1 for the VLSN if both the LN and VLSN are not cached.
     */
    public RecordVersion getCurrentVersion(boolean allowFetch) {
        latchBIN();
        try {
            assert assertCursorState(true) : dumpToString(true);
            long vlsn = bin.getVLSN(index, allowFetch);
            return new RecordVersion(vlsn, bin.getLsn(index));
        } finally {
            releaseBIN();
        }
    }

    /**
     * Move the cursor forward and return the next record. This will cross BIN
     * boundaries.
     *
     * @param foundKey DatabaseEntry to use for returning key
     *
     * @param foundData DatabaseEntry to use for returning data
     *
     * @param forward if true, move forward, else move backwards
     *
     * @param alreadyLatched if true, the bin that we're on is already
     * latched.
     *
     * @param rangeConstraint if non-null, is called to determine whether a key
     * is out of range.
     *
     * @return the status.
     */
    public OperationStatus getNext(DatabaseEntry foundKey,
                                   DatabaseEntry foundData,
                                   LockType lockType,
                                   boolean forward,
                                   boolean alreadyLatched,
                                   RangeConstraint rangeConstraint)
        throws DatabaseException {

        assert assertCursorState(true) : dumpToString(true);
        assert checkAlreadyLatched(alreadyLatched) : dumpToString(true);

        OperationStatus result = OperationStatus.NOTFOUND;

        try {
            while (bin != null) {

                assert checkAlreadyLatched(alreadyLatched) :
                    dumpToString(true);
                if (!alreadyLatched) {
                    latchBIN();
                } else {
                    alreadyLatched = false;
                }

                if (DEBUG) {
                    verifyCursor(bin);
                }

                /* Is there anything left on this BIN? */
                if ((forward && ++index < bin.getNEntries()) ||
                    (!forward && --index > -1)) {

                    if (rangeConstraint != null &&
                        !rangeConstraint.inBounds(bin.getKey(index))) {
                        result = OperationStatus.NOTFOUND;
                        releaseBIN();
                        break;
                    }

                    OperationStatus ret = getCurrentAlreadyLatched
                        (foundKey, foundData, lockType);
                    if (ret == OperationStatus.SUCCESS) {
                        incrementLNCount();
                        result = OperationStatus.SUCCESS;
                        break;
                    } else {
                        assert LatchSupport.countLatchesHeld() == 0;

                        if (binToBeRemoved != null) {
                            flushBINToBeRemoved();
                        }

                        continue;
                    }

                } else {

                    /*
                     * binToBeRemoved is used to release a BIN earlier in the
                     * traversal chain when we move onto the next BIN. When
                     * we traverse across BINs, there is a point when two BINs
                     * point to the same cursor.
                     *
                     * Removal of the prior BIN from the cursor is delayed to
                     * prevent its compression and loss of the scan context.
                     * [#12736]
                     *
                     * Example:  BINa(empty) BINb(empty) BINc(populated)
                     *           Cursor (C) is traversing
                     * loop, leaving BINa:
                     *   binToBeRemoved is null, C points to BINa, and
                     *     BINa points to C
                     *   set binToBeRemoved to BINa
                     *   find BINb, make BINb point to C
                     *   note that BINa and BINb point to C.
                     * loop, leaving BINb:
                     *   binToBeRemoved == BINa, remove C from BINa
                     *   set binToBeRemoved to BINb
                     *   find BINc, make BINc point to C
                     *   note that BINb and BINc point to C
                     * finally, when leaving this method, remove C from BINb.
                     */
                    if (binToBeRemoved != null) {
                        releaseBIN();
                        flushBINToBeRemoved();
                        latchBIN();
                    }
                    binToBeRemoved = bin;
                    bin = null;

                    BIN newBin;

                    /*
                     * SR #12736
                     * Prune away oldBin. Assert has intentional side effect
                     */
                    assert TestHookExecute.doHookIfSet(testHook);

                    if (forward) {
                        newBin = databaseImpl.getTree().getNextBin
                            (binToBeRemoved, cacheMode);
                    } else {
                        newBin = databaseImpl.getTree().getPrevBin
                            (binToBeRemoved, cacheMode);
                    }
                    if (newBin == null) {
                        result = OperationStatus.NOTFOUND;
                        break;
                    } else {
                        if (forward) {
                            index = -1;
                        } else {
                            index = newBin.getNEntries();
                        }
                        addCursor(newBin);
                        /* Ensure that setting bin is under newBin's latch */
                        bin = newBin;
                        alreadyLatched = true;
                    }
                }
            }
        } finally {
            assert LatchSupport.countLatchesHeld() == 0 :
                LatchSupport.latchesHeldToString();
            if (binToBeRemoved != null) {
                flushBINToBeRemoved();
            }
        }
        
        return result;
    }

    private void flushBINToBeRemoved()
        throws DatabaseException {

        binToBeRemoved.latch(cacheMode);
        binToBeRemoved.removeCursor(this);
        binToBeRemoved.releaseLatch();
        binToBeRemoved = null;
    }

    /**
     * Skips over entries until a boundary condition is satisfied, either
     * because maxCount is reached or RangeConstraint.inBounds returns false.
     *
     * If a maxCount is passed, this allows advancing the cursor quickly by N
     * entries.  If a rangeConstraint is passed, this allows returning the
     * entry count after advancing until the predicate returns false, e.g., the
     * number of entries in a key range.  In either case, the number of entries
     * advanced is returned.
     *
     * Optimized to scan using level two of the tree when possible, to avoid
     * calling getNextBin/getPrevBin for every BIN of the database.  All BINs
     * beneath a level two IN can be skipped quickly, with the level two parent
     * IN latched, when all of its children BINs are resident and can be
     * latched without waiting.  When a child BIN is not resident or latching
     * waits, we revert to the getNextBin/getPrevBin approach, to avoid keeping
     * the parent IN latched for long time periods.
     *
     * Although this method positions the cursor on the last non-deleted entry
     * seen (before the boundary condition is satisfied), because it does not
     * lock the LN it is possible that it is deleted by another thread after
     * the BIN is unlatched.
     *
     * @param forward is true to skip forward, false to skip backward.
     *
     * @param maxCount is the maximum number of non-deleted entries to skip,
     * and may be LTE zero if no maximum is enforced.
     *
     * @param rangeConstraint is a predicate that returns false at a position
     * where advancement should stop, or null if no predicate is enforced.
     *
     * @return the number of non-deleted entries that were skipped.
     */
    public long skip(boolean forward,
                     long maxCount,
                     RangeConstraint rangeConstraint) {

        final CursorImpl c = cloneCursor(true /*addCursor*/,
                                         CacheMode.UNCHANGED);
        try {
            return c.skipInternal(forward, maxCount, rangeConstraint, this);
        } finally {
            c.close();
        }
    }

    /**
     * Use this cursor to reference the current BIN in the traversal, to
     * prevent the current BIN from being compressed away.  But set the given
     * finalPositionCursor (the 'user' cursor) position only at non-deleted
     * entries, since it should be positioned on a valid entry when this method
     * returns.
     */
    private long skipInternal(boolean forward,
                              long maxCount,
                              RangeConstraint rangeConstraint,
                              CursorImpl finalPositionCursor) {

        /* Start with the entry at the cursor position. */
        final Tree tree = databaseImpl.getTree();
        BIN curBin = latchBIN();
        int prevIndex = getIndex();
        long count = 0;

        try {
            while (true) {
                /* Skip entries in the current BIN. */
                count = skipEntries
                    (forward, maxCount, rangeConstraint, finalPositionCursor,
                     curBin, prevIndex, count);
                if (count < 0) {
                    return (- count);
                }

                /*
                 * Get the parent IN at level two.  The BIN is unlatched by
                 * getParentINForChildIN.  Before releasing the BIN latch, get
                 * the search key for the last entry.
                 */
                final byte[] idKey = (curBin.getNEntries() == 0) ?
                    curBin.getIdentifierKey() :
                    (forward ?
                     curBin.getKey(curBin.getNEntries() - 1) :
                     curBin.getKey(0));
                final BIN binToFind = curBin;
                curBin = null; // BIN latch will be released.

                final SearchResult result = tree.getParentINForChildIN
                    (binToFind, true /*requireExactMatch*/,
                     CacheMode.UNCHANGED);

                final IN parent = result.parent;
                boolean fetchOrWait = false;

                try {
                    if (!result.exactParentFound) {
                        throw EnvironmentFailureException.unexpectedState
                            ("Cannot get parent of BIN id=" +
                             binToFind.getNodeId() + " key=" +
                             Arrays.toString(idKey));
                    }

                    /*
                     * Find previous child BIN by matching idKey rather than
                     * using result.index, as in getNextBinInternal (see
                     * comments there).
                     */
                    int parentIndex = parent.findEntry(idKey, false, false);
                    if (forward ?
                        (parentIndex == parent.getNEntries() - 1) :
                        (parentIndex == 0)) {

                        /*
                         * This is the last entry in the parent.  Fetch and
                         * latch it, in preparation for getNextBin below.
                         */
                        curBin = (BIN) parent.fetchTargetWithExclusiveLatch
                            (parentIndex);
                        curBin.latch();
                    } else {

                        /*
                         * Skip entries for child BINs that are resident and
                         * can be latched no-wait.
                         */
                        final int incr = forward ? 1 : (-1);
                        for (parentIndex += incr;; parentIndex += incr) {
                            if (fetchOrWait ||
                                (forward ?
                                 (parentIndex >= parent.getNEntries()) :
                                 (parentIndex < 0))) {
                                break;
                            }
                            /* Release previous child BIN. */
                            if (curBin != null) {
                                curBin.releaseLatch();
                                curBin = null;
                            }
                            /* Fetch and latch next child BIN. */
                            curBin = (BIN) parent.getTarget(parentIndex);
                            if (curBin == null) {
                                fetchOrWait = true;
                                curBin =
                                    (BIN) parent.fetchTargetWithExclusiveLatch
                                    (parentIndex);
                            }
                            if (!curBin.latchNoWait(CacheMode.UNCHANGED)) {
                                fetchOrWait = true;
                                curBin.latch();
                            }
                            /* Position at new BIN to prevent compression. */
                            setPosition(curBin, -1);
                            /* Skip entries in new child BIN. */
                            count = skipEntries
                                (forward, maxCount, rangeConstraint,
                                 finalPositionCursor, curBin,
                                 forward ? (-1) : curBin.getNEntries(), count);
                            if (count < 0) {
                                return (- count);
                            }
                        }
                    }
                } finally {
                    if (parent != null) {
                        parent.releaseLatch();
                    }
                }

                /*
                 * Continue after releasing the parent latch.  The current BIN
                 * is still latched.
                 */
                if (fetchOrWait) {

                    /*
                     * A child BIN was not resident or we could not get a
                     * no-wait latch.  Skip over the current BIN, which has
                     * already been processed, and continue the loop within the
                     * same parent IN.
                     */
                    prevIndex = forward ? (curBin.getNEntries() - 1) : 0;
                } else {

                    /*
                     * There are no more entries in the parent IN.  Move to the
                     * next BIN, which will be in the next parent IN, and
                     * continue at the beginning of that BIN.
                     */
                    curBin = forward ?
                        tree.getNextBin(curBin, CacheMode.UNCHANGED) :
                        tree.getPrevBin(curBin, CacheMode.UNCHANGED);

                    if (curBin == null) {
                        return count;
                    }
                    prevIndex = forward ? (-1) : curBin.getNEntries();
                    /* Position at new BIN to prevent compression. */
                    setPosition(curBin, -1);
                }
            }
        } finally {
            if (curBin != null) {
                curBin.releaseLatch();
            }
        }
    }

    /**
     * Skip entries in curBin from one past prevIndex and onward.  Returns
     * non-negative count if skipping should continue, or negative count if
     * bounds is exceeded.
     */
    private long skipEntries(boolean forward,
                             long maxCount,
                             RangeConstraint rangeConstraint,
                             CursorImpl finalPositionCursor,
                             BIN curBin,
                             int prevIndex,
                             long count) {
        final int incr = forward ? 1 : (-1);
        for (int i = prevIndex + incr;; i += incr) {
            if (forward ? (i >= curBin.getNEntries()) : (i < 0)) {
                break;
            }
            if (rangeConstraint != null &&
                !rangeConstraint.inBounds(curBin.getKey(i))) {
                return (- count);
            }
            if (!curBin.isEntryKnownDeleted(i) &&
                !curBin.isEntryPendingDeleted(i)) {
                count += 1;
                finalPositionCursor.setPosition(curBin, i);
                if (maxCount > 0 && count >= maxCount) {
                    return (- count);
                }
            }
        }
        return count;
    }

    /**
     * Change cursor to point to the given BIN/index.  If the new BIN is
     * different, then old BIN must be unlatched and the new BIN must be
     * latched.
     */
    private void setPosition(BIN newBin, int newIndex) {
        if (bin != newBin) {
            removeCursor();
            setBIN(newBin);
            addCursor();
        }
        setIndex(newIndex);
    }

    /**
     * Position the cursor at the first or last record of the databaseImpl.
     * It's okay if this record is deleted.
     *
     * Returns with the target BIN latched!
     *
     * @return true if a first or last position is found, false if the
     * tree being searched is empty.
     */
    public boolean positionFirstOrLast(boolean first)
        throws DatabaseException {

        assert assertCursorState(false) : dumpToString(true);

        IN in = null;
        boolean found = false;
        try {
            removeCursor();
            if (first) {
                in = databaseImpl.getTree().getFirstNode(cacheMode);
            } else {
                in = databaseImpl.getTree().getLastNode(cacheMode);
            }

            if (in != null) {

                assert (in instanceof BIN);

                bin = (BIN) in;
                index = (first ? 0 : (bin.getNEntries() - 1));
                addCursor(bin);

                TreeWalkerStatsAccumulator treeStatsAccumulator =
                    getTreeStatsAccumulator();

                if (bin.getNEntries() == 0) {

                    /*
                     * An IN was found. Even if it's empty, let Cursor
                     * handle moving to the first non-deleted entry.
                     */
                    found = true;
                } else {

                    if (treeStatsAccumulator != null &&
                        !bin.isEntryKnownDeleted(index) &&
                        !bin.isEntryPendingDeleted(index)) {
                        treeStatsAccumulator.incrementLNCount();
                    }

                    /*
                     * Even if the entry is deleted, just leave our
                     * position here and return.
                     */
                    found = true;
                }
            }
            setInitialized();
            return found;
        } catch (DatabaseException e) {
            /* Release latch on error. */
            if (in != null) {
                in.releaseLatch();
            }
            throw e;
        }
    }

    public int searchAndPosition(DatabaseEntry matchKey,
                                 SearchMode searchMode,
                                 LockType lockType) {
        return searchAndPosition(matchKey, searchMode, lockType,
                                 null /*searchComparator*/);
    }

    public static final int FOUND = 0x1;
    /* Exact match on the key portion. */
    public static final int EXACT_KEY = 0x2;
    /* Record found is the last one in the databaseImpl. */
    public static final int FOUND_LAST = 0x4;

    /**
     * Position the cursor at the key. This returns a three part value that's
     * bitwise or'ed into the int. We find out if there was any kind of match
     * and if the match was exact. Note that this match focuses on whether the
     * searching criteria (key, or key and data, depending on the search type)
     * is met.
     *
     * <p>Note this returns with the BIN latched!</p>
     *
     * <p>If this method returns without the FOUND bit set, the caller can
     * assume that no match is possible.  Otherwise, if the FOUND bit is set,
     * the caller should check the EXACT_KEY  bit.  If EXACT_KEY is not set, an
     * approximate match was found.  In an approximate match, the cursor is
     * always positioned before the target key/data.  This allows the caller to
     * perform a 'next' operation to advance to the value that is equal or
     * higher than the target key/data.</p>
     *
     * <p>Even if the search returns an exact result, the record may be
     * deleted.  The caller must therefore check for both an approximate match
     * and for whether the cursor is positioned on a deleted record.</p>
     *
     * <p>If SET is specified, the FOUND bit will only be returned if an exact
     * match is found.  However, the record found may be deleted.</p>
     *
     * <p>There is one special case where this method may be called without
     * checking the EXACT_KEY bit and without checking for a deleted record:
     * If SearchMode.SET is specified then only the FOUND bit need be checked.
     * When SET is specified and FOUND is returned, it is guaranteed to be an
     * exact match on a non-deleted record.  It is for this case only that this
     * method is public.</p>
     *
     * <p>If FOUND is set, FOUND_LAST may also be set if the cursor is
     * positioned on the last record in the databaseImpl.  Note that this state
     * can only be counted on as long as the BIN is latched, so it is not set
     * if this method must release the latch to lock the record.  Therefore, it
     * should only be used for optimizations.  If FOUND_LAST is set, the cursor
     * is positioned on the last record and the BIN is latched.  If FOUND_LAST
     * is not set, the cursor may or may not be positioned on the last record.
     * Note that exact searches always perform an unlatch and a lock, so
     * FOUND_LAST will only be set for inexact (range) searches.</p>
     *
     * <p>Be aware that when an approximate match is returned, the index may be
     * set to -1.  This is done intentionally so that a 'next' operation will
     * increment it.</p>
     *
     * @param searchMode SearchMode.SET (position on a key EQ matchKey) or
     * SearchMode.SET_RANGE (position on a key LTE matchKey).
     */
    public int searchAndPosition(DatabaseEntry matchKey,
                                 SearchMode searchMode,
                                 LockType lockType,
                                 Comparator<byte[]> searchComparator) {

        assert assertCursorState(false) : dumpToString(true);

        assert searchMode == SearchMode.SET ||
               searchMode == SearchMode.SET_RANGE : searchMode;

        removeCursor();

        /* Reset the cursor. */
        bin = null;

        boolean foundSomething = false;
        boolean foundExactKey = false;
        boolean foundLast = false;
        boolean exactSearch = searchMode.isExactSearch();
        BINBoundary binBoundary = new BINBoundary();

        try {
            byte[] key = Key.makeKey(matchKey);
            bin = (BIN) databaseImpl.getTree().search
                (key, Tree.SearchType.NORMAL, binBoundary, cacheMode, 
                 searchComparator);

            if (bin != null) {
                addCursor(bin);

                /*
                 * If we're doing an exact search, tell bin.findEntry we
                 * require an exact match. If it's a range search, we don't
                 * need that exact match.
                 */
                index = bin.findEntry(key, true, exactSearch,
                                      searchComparator);

                /*
                 * If we're doing an exact search, as a starting point, we'll
                 * assume that we haven't found anything. If this is a range
                 * search, we'll assume the opposite, that we have found a
                 * record. That's because for a range search, the higher level
                 * will take care of sorting out whether anything is really
                 * there or not.
                 */
                foundSomething = !exactSearch;

                if (index >= 0) {
                    if ((index & IN.EXACT_MATCH) != 0) {

                        /*
                         * The binary search told us we had an exact match.
                         * Note that this really only tells us that the key
                         * matched. The LN may be deleted, but the next layer
                         * up will handle that case.
                         */
                        foundExactKey = true;

                        /*
                         * Now turn off the exact match bit so the index will
                         * be a valid value, before we use it to retrieve the
                         * child reference from the bin.
                         */
                        index &= ~IN.EXACT_MATCH;
                    }

                    if (!bin.isEntryKnownDeleted(index) &&
                        !bin.isEntryPendingDeleted(index)) {
                        foundSomething = true;
                        if (exactSearch) {
                            /* Lock LN, check if deleted. */
                            LockStanding lockStanding = lockLN(lockType);
                            if (!lockStanding.recordExists()) {
                                revertLock(lockStanding);
                                foundSomething = false;
                            }
                        }
                    }

                    /*
                     * Determine whether the last record was found.  This is
                     * only possible when we don't lock the record (supposedly
                     * because another insertion could occur while waiting for
                     * a lock).
                     */
                    foundLast = (searchMode == SearchMode.SET_RANGE &&
                                 foundSomething &&
                                 binBoundary.isLastBin &&
                                 index == bin.getNEntries() - 1);
                }
            }
            setInitialized();

            /* Return a multi-part status value */
            return (foundSomething ? FOUND : 0) |
                (foundExactKey ? EXACT_KEY : 0) |
                (foundLast ? FOUND_LAST : 0);
        } catch (DatabaseException e) {
            /* Release latch on error. */
            releaseBIN();
            throw e;
        }
    }

    /*
     * Lock and copy current record into the key and data DatabaseEntry. Enter
     * with the BIN latched.  May exit with latched BIN, and is caller's
     * responsibility to call releaseBIN.
     */
    private OperationStatus fetchCurrent(DatabaseEntry foundKey,
                                         DatabaseEntry foundData,
                                         LockType lockType)
        throws DatabaseException {

        TreeWalkerStatsAccumulator treeStatsAccumulator =
            getTreeStatsAccumulator();

        if (bin == null) {
            return OperationStatus.NOTFOUND;
        }

        assert bin.isLatchOwnerForWrite();

        /*
         * If we encounter a deleted entry, opportunistically add it to the
         * compressor queue.
         */
        if (index >= 0 && index < bin.getNEntries() &&
            (bin.isEntryKnownDeleted(index) ||
             bin.isEntryPendingDeleted(index))) {
            bin.queueSlotDeletion();
        }

        /*
         * Check the deleted flag in the BIN and make sure this isn't an empty
         * BIN.  The BIN could be empty by virtue of the compressor running the
         * size of this BIN to 0 but not having yet deleted it from the tree.
         *
         * The index may be negative if we're at an intermediate stage in an
         * higher level operation, and we expect a higher level method to do a
         * next or prev operation after this returns KEYEMPTY. [#11700]
         */
        if (index < 0 ||
            index >= bin.getNEntries() ||
            bin.isEntryKnownDeleted(index)) {
            /* Node is no longer present. */
            if (treeStatsAccumulator != null) {
                treeStatsAccumulator.incrementDeletedLNCount();
            }
            return OperationStatus.KEYEMPTY;
        }

        /*
         * We don't need to fetch the LN if the user has not requested that we
         * return the data. The key may still be returned, and it is available
         * in the BIN.
         */
        final boolean mustFetch =
            foundData != null &&
            (!foundData.getPartial() || foundData.getPartialLength() != 0);

        /*
         * Note that since we have the BIN latched, we can safely check the
         * node type.
         */
        addCursor(bin);

        assert TestHookExecute.doHookIfSet(testHook);

        /*
         * Lock the LN.  For dirty-read, the data of the LN can be set to null
         * at any time.  Cache the data in a local variable so its state does
         * not change before calling LN.setEntry further below.
         */
        final LockStanding lockStanding = lockLN(lockType);
        if (!lockStanding.recordExists()) {
            revertLock(lockStanding);
            if (treeStatsAccumulator != null) {
                treeStatsAccumulator.incrementDeletedLNCount();
            }
            return OperationStatus.KEYEMPTY;
        }
        final LN ln = (LN) (mustFetch ?  bin.fetchTarget(index) : null);
        byte[] lnData = (ln != null) ? ln.getData() : null;

        /*
         * Don't set the abort LSN here since we are not logging yet, even
         * if this is a write lock.  Tree.insert depends on the fact that
         * the abortLSN is not already set for deleted items.
         */

        /*
         * Return the key from the BIN because only the BIN is guaranteed to be
         * latched by lockLN above, and the key is not available as part of the
         * LN.  [#15704]
         */
        if (foundKey != null) {
            LN.setEntry(foundKey, bin.getKey(index));
        }

        /* Return the data. */
        if (foundData != null &&
            (!foundData.getPartial() ||
             foundData.getPartialLength() != 0)) {
            assert lnData != null;
            LN.setEntry(foundData, lnData);
        }

        return OperationStatus.SUCCESS;
    }

    /**
     * Returns the key at the current position, regardless of whether the
     * record is deleted.  Does not lock.  The key returned is not a copy and
     * may not be returned directly to the user without copying it first.
     * Returns null if the cursor is not initialized.
     */
    public byte[] getCurrentKey() {
        latchBIN();
        try {
            if (bin == null || index < 0) {
                return null;
            }
            return bin.getKey(index);
        } finally {
            releaseBIN();
        }
    }

    /**
     * Search for the next key following the given key, and acquire a range
     * insert lock on it.  If there are no more records following the given
     * key, lock the special EOF node for the databaseImpl.
     */
    public void lockNextKeyForInsert(DatabaseEntry key)
        throws DatabaseException {

        DatabaseEntry tempKey = new DatabaseEntry
            (key.getData(), key.getOffset(), key.getSize());
        tempKey.setPartial(0, 0, true);
        boolean lockedNextKey = false;
        boolean latched = true;
        try {
            /* Search. */
            int searchResult = searchAndPosition
                (tempKey, SearchMode.SET_RANGE, LockType.RANGE_INSERT);
            if ((searchResult & FOUND) != 0 &&
                (searchResult & FOUND_LAST) == 0) {

                /*
                 * If searchAndPosition found a record (other than the last
                 * one), in all cases we should advance to the next record:
                 *
                 * 1- found a deleted record,
                 * 2- found an exact match, or
                 * 3- found the record prior to the given key/data.
                 */
                DatabaseEntry tempData = new DatabaseEntry();
                tempData.setPartial(0, 0, true);
                OperationStatus status = getNext
                    (tempKey, tempData, LockType.RANGE_INSERT, true, true,
                     null /*rangeConstraint*/);
                if (status == OperationStatus.SUCCESS) {
                    lockedNextKey = true;
                }
                latched = false;
            }
        } finally {
            if (latched) {
                releaseBIN();
            }
        }

        /* Lock the EOF node if no next key was found. */
        if (!lockedNextKey) {
            lockEof(LockType.RANGE_INSERT);
        }
    }

    /*
     * Locking
     */

    /**
     * Holds the result of a lockLN operation.  A lock may not actually be
     * held (getLockResult may return null) if an uncontended lock is allowed.
     */
    public static class LockStanding {
        private long lsn;
        private boolean nullLsn;
        private boolean deleted;
        private boolean uncontended;
        private LockResult lockResult;

        public boolean recordExists() {
            return !nullLsn && !deleted;
        }

        public LockResult getLockResult() {
            return lockResult;
        }

        /**
         * Creates WriteLockInfo that is appropriate for an update or delete
         * operation, after lockLN has been called.  The return value is meant
         * to be passed to an LN logging method and copied into the
         * WriteLockInfo for the new LSN.  If the lock is not already held by
         * this locker, a WriteLockInfo is created with the old LSN as the
         * abortLsn.
         */
        public WriteLockInfo prepareForUpdate() {
            final boolean abortKnownDeleted = !recordExists();
            WriteLockInfo wri = null;
            if (lockResult != null) {
                lockResult.setAbortLsn(lsn, abortKnownDeleted);
                wri = lockResult.getWriteLockInfo();
            }
            if (wri == null) {
                wri = new WriteLockInfo();
                wri.setAbortLsn(lsn);
                wri.setAbortKnownDeleted(abortKnownDeleted);
            }
            return wri;
        }

        /**
         * Creates WriteLockInfo that is appropriate for a newly inserted slot.
         * The return value is meant to be passed to an LN logging method and
         * copied into the WriteLockInfo for the new LSN.  This method is
         * static because lockLN is never called prior to logging an LN for a
         * newly inserted slot.
         */
        public static WriteLockInfo prepareForInsert() {
            final WriteLockInfo wri = new WriteLockInfo();
            wri.setCreatedThisTxn(true);
            return wri;
        }
    }

    /** Does not allow uncontended locks.  See lockLN(LockType). */
    public LockStanding lockLN(LockType lockType)
        throws LockConflictException {

        return lockLN(lockType, false /*allowUncontended*/);
    }

    /**
     * Locks the LN at the cursor position.  Attempts to use a non-blocking
     * lock to avoid unlatching/relatching.
     *
     * Retries if necessary, to handle the case where the LSN is changed while
     * the BIN is unlatched.  Because it re-latches the BIN to check the LSN,
     * this serializes access to the LSN for locking, guaranteeing that two
     * lockers cannot obtain conflicting locks on the old and new LSNs.
     *
     * Preconditions: The BIN must be latched.
     *
     * Postconditions: The BIN is latched.
     *
     * LN Locking Rules
     * ----------------
     * The lock ID for an LN is its LSN in the parent BIN slot.  Because the
     * LSN changes when logging the LN, only two methods of locking an LN may
     * be used to support concurrent access:
     *
     * 1. This method may be called to lock the old LSN.  For read operations,
     * that is all that is necessary.  For write operations, the new LSN must
     * be locked after logging it, which is done by all the LN logging methods.
     * Be sure to pass a non-null locker to the LN logging method to lock the
     * LN, unless locking is not desired.
     *
     * 2. A non-blocking lock may be obtained on the old LSN (using
     * Locker.nonBlockingLock rather than this method), as long as the lock is
     * released before the BIN latch is released.  In this case a non-null
     * locker is not passed to the LN logging method; locking the new LSN is
     * unnecessary because no other thread can access the new LSN until the BIN
     * latch is released.
     *
     * The first method is used for all user operations.  The second method is
     * used by the cleaner, when flushing dirty deferred-write LNs, and by
     * certain btree operations.
     *
     * Uncontended Lock Optimization
     * -----------------------------
     * The allowUncontended param is passed as true for update and delete
     * operations as an optimization for the case where no lock on the old LSN
     * is held by any locker.  In this case we don't need to lock the old LSN
     * at all, as long as we log the new LSN before releasing the BIN latch.
     *
     * 1. Latch BIN
     * 2. Determine that no lock/waiter exists for oldLsn
     * 3. Log LN and get newLsn
     * 4. Lock newLsn
     * 5. Update BIN
     * 6. Release BIN latch
     *
     * The oldLsn is never locked, saving operations on the lock table.  The
     * assumption is that another locker will first have to latch the BIN to
     * get oldLsn, before requesting a lock.
     *
     * A potential problem is that the other locker may release the BIN latch
     * before requesting the lock.
     *
     * This Operation        Another Operation
     * --------------        -----------------
     *                       Latch BIN, get oldLsn, release BIN latch
     * Step 1 and 2
     *                       Request lock for oldLsn, granted
     * Step 3 and 4
     *
     * Both operations now believe they have an exclusive lock, but they have
     * locks on different LSNs.
     *
     * However, this problem is handled as long as the other lock is performed
     * using a lockLN method in this class, which will release the lock and
     * retry if the LSN changes while acquiring the lock.  Because it
     * re-latches the BIN to check the LSN, this will serialize access to the
     * LSN for locking, guaranteeing that two conflicting locks cannot be
     * granted on the old and new LSNs.
     *
     * Deferred-Write Locking
     * ----------------------
     * When one of the LN optionalLog methods is called, a deferred-write LN is
     * dirtied but not actually logged.  In order to lock an LN that has been
     * inserted but not yet assigned a true LSN, a transient LSNs is assigned.
     * These LSNs serve to lock the LN but never appear in the log.  See
     * LN.assignTransientLsn.
     *
     * A deferred-write LN is logged when its parent BIN is logged, or when the
     * LN is evicted.  This will replace transient LSNs with durable LSNs.  If
     * a lock is held by a cursor on a deferred-write LN when it is logged, the
     * same lock is acquired on the new LSN by the cursor.  See
     * lockAfterLsnChange.
     *
     * Cleaner Migration Locking
     * -------------------------
     * The cleaner takes a non-blocking read lock on the old LSN before
     * migrating/logging the LN, while holding the BIN latch.  It does not take
     * a lock on the new LSN, since it does not need to retain a lock after
     * releasing the BIN latch.
     *
     * Because a read, not write, lock is taken, other read locks may be held
     * during migration.  After logging, the cleaner calls lockAfterLsnChange
     * to lock the new LSN on behalf of other lockers.
     *
     * For more info on migration locking, see HandleLocker.
     *
     * Historical Notes
     * ----------------
     * In JE 4.1 and earlier, each LN had a node ID that was used for locking,
     * rather than using the LSN.  The node ID changed only if a deleted slot
     * was reused.  The node ID was stored in the LN, requiring that the LN be
     * fetched when locking the LN.  With LSN locking a fetch is not needed.
     *
     * When LN node IDs were used, deferred-write LNs were not assigned an LSN
     * until they were actually logged. Deferred-write LNs were initially
     * assigned a null LSN and transient LSNs were not needed.
     *
     * @param lockType the type of lock requested.
     *
     * @param allowUncontended is true to return immediately (no lock is taken)
     * when no locker holds or waits for the lock.
     *
     * @return all information about the lock; see LockStanding.
     *
     * @throws LockConflictException if the lsn is non-null, the lock is
     * contended, and a lock could not be obtained by blocking.
     */
    public LockStanding lockLN(LockType lockType, boolean allowUncontended)
        throws LockConflictException {

        final LockStanding standing = new LockStanding();
        standing.lsn = bin.getLsn(index);
        standing.deleted = bin.isEntryKnownDeleted(index) ||
                           bin.isEntryPendingDeleted(index);

        /* Ensure that a known-deleted null LSN is not present. */
        if (standing.lsn == DbLsn.NULL_LSN) {
            assert bin.isEntryKnownDeleted(index);
            standing.nullLsn = true;
            return standing;
        }

        /*
         * We can avoid taking a lock if uncontended.  However, we must
         * call preLogWithoutLock to prevent logging on a replica, and as
         * good measure to prepare for undo.
         */
        if (allowUncontended &&
            databaseImpl.getDbEnvironment().
                         getTxnManager().
                         getLockManager().
                         isLockUncontended(standing.lsn)) {
            locker.preLogWithoutLock(databaseImpl);
            standing.uncontended = true;
            assert verifyPendingDeleted(lockType);
            return standing;
        }

        /*
         * Try a non-blocking lock first, to avoid unlatching.  If the default
         * is no-wait, use the standard lock method so
         * LockNotAvailableException is thrown; there is no need to try a
         * non-blocking lock twice.
         *
         * Even for dirty-read (LockType.NONE) we must call Locker.lock() since
         * it checks the locker state and may throw LockPreemptedException.
         */
        if (locker.getDefaultNoWait()) {
            try {
                standing.lockResult = locker.lock
                    (standing.lsn, lockType, true /*noWait*/, databaseImpl);
            } catch (LockConflictException e) {

                /*
                 * Release all latches.  Note that we catch
                 * LockConflictException for simplicity but we expect either
                 * LockNotAvailableException or LockNotGrantedException.
                 */
                releaseBIN();
                throw e;
            }
        } else {
            standing.lockResult = locker.nonBlockingLock
                (standing.lsn, lockType, false /*jumpAheadOfWaiters*/,
                 databaseImpl);
        }
        if (standing.lockResult.getLockGrant() != LockGrantType.DENIED) {
            /* Lock was granted whiled latched, no need to check LSN. */
            assert verifyPendingDeleted(lockType);
            return standing;
        }

        /*
         * Unlatch, get a blocking lock, latch, and get the current LSN from
         * the slot.  If the LSN changes while unlatched, revert the lock and
         * repeat.
         */
        while (true) {

            /* Request a blocking lock. */
            releaseBIN();
            standing.lockResult = locker.lock
                (standing.lsn, lockType, false /*noWait*/, databaseImpl);

            /* Check current LSN after locking. */
            latchBIN();
            standing.deleted = bin.isEntryKnownDeleted(index) ||
                               bin.isEntryPendingDeleted(index);
            final long newLsn = bin.getLsn(index);
            if (standing.lsn != newLsn) {
                /* The LSN changed, revert the lock and try again. */
                revertLock(standing);
                standing.lsn = newLsn;
                /* Ensure that a known-deleted null LSN is not present. */
                if (newLsn == DbLsn.NULL_LSN) {
                    assert bin.isEntryKnownDeleted(index);
                    standing.nullLsn = true;
                    return standing;
                }
                continue;
            } else {
                /* If locked correctly, return the result. */
                assert verifyPendingDeleted(lockType);
                return standing;
            }
        }
    }

    public boolean lockCurrent(LockType lockType) {
        if (bin == null || index < 0) {
            return false;
        }
        latchBIN();
        try {
            lockLN(lockType);
            return true;
        } finally {
            releaseBIN();
        }
    }

    /**
     * After logging a deferred-write LN during eviction/checkpoint or a
     * migrated LN during cleaning, for every existing lock on the old LSN held
     * by another locker, we must lock the new LSN on behalf of that locker.
     *
     * This is done while holding the BIN latch so that the new LSN does not
     * change during the locking process.  The BIN must be latched on entry and
     * is left latched by this method.
     *
     * We release the lock on the oldLsn to prevent locks from accumulating
     * over time on a HandleLocker, as the cleaner migrates LNs, because
     * Database handle locks are legitmately very long-lived.  It is important
     * to first acquire all newLsn locks and then release the oldLsn locks.
     * Releasing an oldLsn lock might allow another locker to acquire it, and
     * then acquiring another newLsn lock may encounter a conflict. [#20617]
     *
     * @see HandleLocker
     * @see #lockLN
     */
    public static void lockAfterLsnChange(DatabaseImpl dbImpl,
                                          long oldLsn,
                                          long newLsn,
                                          Locker excludeLocker) {
        final LockManager lockManager =
            dbImpl.getDbEnvironment().getTxnManager().getLockManager();
        final Set<LockInfo> owners = lockManager.getOwners(oldLsn);
        if (owners == null) {
            return;
        }
        /* Acquire newLsn locks. */
        for (LockInfo lockInfo : owners) {
            final Locker locker = lockInfo.getLocker();
            if (locker != excludeLocker) {
                locker.lockAfterLsnChange(oldLsn, newLsn, dbImpl);
            }
        }
        /* Release oldLsn locks. */
        for (LockInfo lockInfo : owners) {
            final Locker locker = lockInfo.getLocker();
            if (locker != excludeLocker &&
                locker.allowReleaseLockAfterLsnChange()) {
                locker.releaseLock(oldLsn);
            }
        }
    }

    /**
     * For debugging. Verify that a BINs cursor set refers to the BIN.
     */
    private void verifyCursor(BIN bin)
        throws DatabaseException {

        if (!bin.getCursorSet().contains(this)) {
            throw new EnvironmentFailureException
                (databaseImpl.getDbEnvironment(),
                 EnvironmentFailureReason.UNEXPECTED_STATE,
                 "BIN cursorSet is inconsistent");
        }
    }

    /**
     * Calls checkCursorState and returns false is an exception is thrown.
     */
    private boolean assertCursorState(boolean mustBeInitialized) {
        try {
            checkCursorState(mustBeInitialized);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Check that the cursor is open and optionally if it is initialized.
     *
     * @throws IllegalStateException via all Cursor methods that call
     * Cursor.checkState (all get and put methods, plus more).
     */
    public void checkCursorState(boolean mustBeInitialized) {
        switch (status) {
        case CURSOR_NOT_INITIALIZED:
            if (mustBeInitialized) {
                throw new IllegalStateException("Cursor not initialized.");
            }
            break;
        case CURSOR_INITIALIZED:
            if (DEBUG) {
                if (bin != null) {
                    verifyCursor(bin);
                }
            }
            break;
        case CURSOR_CLOSED:
            throw new IllegalStateException("Cursor has been closed.");
        default:
            throw EnvironmentFailureException.unexpectedState
                ("Unknown cursor status: " + status);
        }
    }

    /**
     * Checks that LN deletedness matches KD/PD flag state, at least when the
     * LN is resident.  Should only be called under an assertion.
     */
    private boolean verifyPendingDeleted(LockType lockType) {

        /* Cannot verify deletedness if LN is not locked. */
        if (lockType == LockType.NONE) {
            return true;
        }

        /* Cannot verify deletedness if cursor is not intialized. */
        if (bin == null || index < 0) {
            return true;
        }

        /* Cannot verify deletedness if LN is not resident. */
        final LN ln = (LN) bin.getTarget(index);
        if (ln == null) {
            return true;
        }

        /*
         * If the LN is deleted then KD or PD must be set.  If the LN is not
         * deleted then PD must not be set, but KD may or may not be set since
         * it used for various purposes (see IN.java).
         */
        final boolean kd = bin.isEntryKnownDeleted(index);
        final boolean pd = bin.isEntryPendingDeleted(index);
        final boolean lnDeleted = ln.isDeleted();
        assert ((lnDeleted && (kd || pd)) || (!lnDeleted && !pd)) :
               "Deleted state mismatch LNDeleted = " + lnDeleted +
               " PD = " + pd + " KD = " + kd;
        return true;
    }

    private void revertLock(LockStanding standing)
        throws DatabaseException {

        if (standing.lockResult != null) {
            revertLock(standing.lsn, standing.lockResult);
            standing.lockResult = null;
        }
    }

    /**
     * Return this lock to its prior status. If the lock was just obtained,
     * release it. If it was promoted, demote it.
     */
    private void revertLock(long lsn, LockResult lockResult)
        throws DatabaseException {

        LockGrantType lockStatus = lockResult.getLockGrant();
        if ((lockStatus == LockGrantType.NEW) ||
            (lockStatus == LockGrantType.WAIT_NEW)) {
            locker.releaseLock(lsn);
        } else if ((lockStatus == LockGrantType.PROMOTION) ||
                   (lockStatus == LockGrantType.WAIT_PROMOTION)){
            locker.demoteLock(lsn);
        }
    }

    /**
     * Locks the logical EOF node for the databaseImpl.
     */
    public void lockEof(LockType lockType)
        throws DatabaseException {

        locker.lock(databaseImpl.getEofLsn(), lockType,
                    false /*noWait*/, databaseImpl);
    }

    /**
     * @throws EnvironmentFailureException if the underlying environment is
     * invalid.
     */
    public void checkEnv() {
        databaseImpl.getDbEnvironment().checkIfInvalid();
    }

    /**
     * Callback object for traverseDbWithCursor.
     */
    public interface WithCursor {

        /**
         * Called for each record in the databaseImpl.
         * @return true to continue or false to stop the enumeration.
         */
        boolean withCursor(CursorImpl cursor,
                           DatabaseEntry key,
                           DatabaseEntry data)
            throws DatabaseException;
    }

    /**
     * Enumerates all records in a databaseImpl non-transactionally and calls
     * the withCursor method for each record.  Stops the enumeration if the
     * callback returns false.
     *
     * @param db DatabaseImpl to traverse.
     *
     * @param lockType non-null LockType for reading records.
     *
     * @param allowEviction should normally be true to evict when performing
     * multiple operations, but may be false if eviction is disallowed in a
     * particular context.
     *
     * @param withCursor callback object.
     */
    public static void traverseDbWithCursor(DatabaseImpl db,
                                            LockType lockType,
                                            boolean allowEviction,
                                            WithCursor withCursor)
        throws DatabaseException {

        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Locker locker = null;
        CursorImpl cursor = null;
        try {
            EnvironmentImpl envImpl = db.getDbEnvironment();
            locker = LockerFactory.getInternalReadOperationLocker(envImpl);
            cursor = new CursorImpl(db, locker);
            cursor.setAllowEviction(allowEviction);
            if (cursor.positionFirstOrLast(true /*first*/)) {
                OperationStatus status =
                    cursor.getCurrentAlreadyLatched(key, data, lockType);
                boolean done = false;
                while (!done) {

                    /*
                     * getCurrentAlreadyLatched may have returned non-SUCCESS
                     * if the first record is deleted, but we can call getNext
                     * below to move forward.
                     */
                    if (status == OperationStatus.SUCCESS) {
                        if (!withCursor.withCursor(cursor, key, data)) {
                            done = true;
                        }
                    }
                    if (!done) {
                        status = cursor.getNext(key, data, lockType,
                                                true /*forward*/,
                                                false /*alreadyLatched*/,
                                                null /*rangeConstraint*/);
                        if (status != OperationStatus.SUCCESS) {
                            done = true;
                        }
                    }
                }
            }
        } finally {
            if (cursor != null) {
                cursor.releaseBIN();
                cursor.close();
            }
            if (locker != null) {
                locker.operationEnd();
            }
        }
    }

    /**
     * Dump the cursor for debugging purposes.  Dump the bin that the cursor
     * refers to if verbose is true.
     */
    public void dump(boolean verbose) {
        System.out.println(dumpToString(verbose));
    }

    /**
     * dump the cursor for debugging purposes.
     */
    public void dump() {
        System.out.println(dumpToString(true));
    }

    /*
     * dumper
     */
    private String statusToString(byte status) {
        switch(status) {
        case CURSOR_NOT_INITIALIZED:
            return "CURSOR_NOT_INITIALIZED";
        case CURSOR_INITIALIZED:
            return "CURSOR_INITIALIZED";
        case CURSOR_CLOSED:
            return "CURSOR_CLOSED";
        default:
            return "UNKNOWN (" + Byte.toString(status) + ")";
        }
    }

    /*
     * dumper
     */
    public String dumpToString(boolean verbose) {
        StringBuilder sb = new StringBuilder();

        sb.append("<Cursor idx=\"").append(index).append("\"");
        sb.append(" status=\"").append(statusToString(status)).append("\"");
        sb.append(">\n");
        if (verbose) {
            sb.append((bin == null) ? "" : bin.dumpString(2, true));
        }
        sb.append("\n</Cursor>");

        return sb.toString();
    }

    /*
     * For unit tests
     */
    public StatGroup getLockStats()
        throws DatabaseException {

        return locker.collectStats();
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void trace(Level level,
                       String changeType,
                       BIN theBin,
                       int lnIndex,
                       long oldLsn,
                       long newLsn) {
        EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();
        if (envImpl.getLogger().isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(changeType);
            sb.append(" bin=");
            sb.append(theBin.getNodeId());
            sb.append(" lnIdx=");
            sb.append(lnIndex);
            sb.append(" oldLnLsn=");
            sb.append(DbLsn.getNoFormatString(oldLsn));
            sb.append(" newLnLsn=");
            sb.append(DbLsn.getNoFormatString(newLsn));

            LoggerUtils.logMsg
                (envImpl.getLogger(), envImpl, level, sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceInsert(Level level,
                             BIN insertingBin,
                             long lnLsn,
                             int index) {
        EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();
        if (envImpl.getLogger().isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(TRACE_INSERT);
            sb.append(" bin=");
            sb.append(insertingBin.getNodeId());
            sb.append(" lnLsn=");
            sb.append(DbLsn.getNoFormatString(lnLsn));
            sb.append(" index=");
            sb.append(index);

            LoggerUtils.logMsg(envImpl.getLogger(), envImpl, level,
                               sb.toString());
        }
    }

    /* For unit testing only. */
    public void setTestHook(TestHook hook) {
        testHook = hook;
    }

    /* Check that the target bin is latched. For use in assertions. */
    private boolean checkAlreadyLatched(boolean alreadyLatched) {
        if (alreadyLatched) {
            if (bin != null) {
                return bin.isLatchOwnerForWrite();
            }
        }
        return true;
    }
}
