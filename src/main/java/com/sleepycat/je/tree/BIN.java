/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.RelatchRequiredException;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.TinyHashSet;
import com.sleepycat.je.utilint.VLSN;

/**
 * A BIN represents a Bottom Internal Node in the JE tree.
 */
public class BIN extends IN implements Loggable {

    private static final String BEGIN_TAG = "<bin>";
    private static final String END_TAG = "</bin>";

    /*
     * The set of cursors that are currently referring to this BIN.
     */
    private final TinyHashSet<CursorImpl> cursorSet =
        new TinyHashSet<CursorImpl>();

    /*
     * Support for logging BIN deltas. (Partial BIN logging)
     */

    /* Location of last delta, for cleaning. */
    private long lastDeltaVersion = DbLsn.NULL_LSN;
    private int numDeltasSinceLastFull; // num deltas logged
    private boolean prohibitNextDelta;  // disallow delta on next log

    /* See VLSNCache.  */
    private VLSNCache vlsnCache = VLSNCache.EMPTY_CACHE;

    public BIN() {
        numDeltasSinceLastFull = 0;
        prohibitNextDelta = false;
    }

    public BIN(DatabaseImpl db,
               byte[] identifierKey,
               int maxEntriesPerNode,
               int level) {
        super(db, identifierKey, maxEntriesPerNode, level);

        numDeltasSinceLastFull = 0;
        prohibitNextDelta = false;
    }

    /**
     * For Sizeof, set all array fields to null, since they are not part of the
     * fixed overhead.
     */
    public BIN(SizeofMarker marker) {
        super(marker);
        vlsnCache = null;
    }

    /**
     * Returns the VLSN.  VLSN.NULL_VLSN.getSequence() (-1) is returned in two
     * cases:
     * 1) This is a standalone environment.
     * 2) The VLSN is not cached (perhaps VLSN caching is not configured), and
     *    the allowFetch param is false.
     *
     * WARNING: Because the VLSNCache is only updated when an LN is evicted, it
     * is critical that getVLSN returns the VLSN for a resident LN before
     * getting the VLSN from the cache.
     */
    public long getVLSN(int idx, boolean allowFetch) {
            
        /* Must return the VLSN from the LN, if it is resident. */
        LN ln = (LN) getTarget(idx);
        if (ln != null) {
            return ln.getVLSNSequence();
        }

        /* Next try the VLSNCache. */
        final long vlsn = vlsnCache.get(idx);
        if (!VLSN.isNull(vlsn)) {
            return vlsn;
        }

        /* As the last resort, fetch the LN if fetching is allowed. */
        if (!allowFetch) {
            return vlsn;
        }
        ln = (LN) fetchTarget(idx);
        return ln.getVLSNSequence();
    }

    /** For unit testing. */
    public VLSNCache getVLSNCache() {
        return vlsnCache;
    }

    /**
     * Updates the VLSN cache when an LN target is evicted.  See VLSNCache.
     */
    @Override
    void setTarget(int idx, Node target) {
        if (target == null) {
            final Node oldTarget = getTarget(idx);
            if (oldTarget instanceof LN) {
                final long val = ((LN) oldTarget).getVLSNSequence();
                vlsnCache = vlsnCache.set(idx, val, this);
            }
        }
        super.setTarget(idx, target);
    }

    /**
     * Overridden to account for VLSNCache entries.
     */
    @Override
    void copyEntry(int idx, IN from, int fromIdx) {
        super.copyEntry(idx, from, fromIdx);
        final long vlsn = ((BIN) from).vlsnCache.get(fromIdx);
        vlsnCache = vlsnCache.set(idx, vlsn, this);
    }

    /**
     * Overridden to account for VLSNCache entries.
     */
    @Override
    void copyEntries(int from, int to, int n) {
        super.copyEntries(from, to, n);
        vlsnCache = vlsnCache.copy(from, to, n);
    }

    /**
     * Overridden to account for VLSNCache entries.
     */
    @Override
    void clearEntry(int idx) {
        super.clearEntry(idx);
        vlsnCache = vlsnCache.set(idx, VLSN.NULL_VLSN.getSequence(), this);
    }

    /**
     * Adds VLSNCache size to computed memory size.
     */
    @Override
    public long computeMemorySize() {

        /* 
         * The vlsnCache field is null only when this method is called by the
         * superclass constructor, i.e., before this class constructor has
         * run.  Luckily the initial VLSNCache.EMPTY_CACHE has a zero memory
         * size and we can ignore it in this case.
         */
        if (vlsnCache == null) {
            return super.computeMemorySize();
        }

        return super.computeMemorySize() + vlsnCache.getMemorySize();
    }

    /**
     * Create a holder object that encapsulates information about this BIN for
     * the INCompressor.
     */
    public BINReference createReference() {
      return new BINReference(getNodeId(), getDatabase().getId(),
                              getIdentifierKey());
    }

    /**
     * Create a new BIN.  Need this because we can't call newInstance()
     * without getting a 0 for nodeId.
     */
    @Override
    protected IN createNewInstance(byte[] identifierKey,
                                   int maxEntries,
                                   int level) {
        return new BIN(getDatabase(), identifierKey, maxEntries, level);
    }

    /*
     * Return whether the shared latch for this kind of node should be of the
     * "always exclusive" variety.  Presently, only IN's are actually latched
     * shared.  BINs are latched exclusive only.
     */
    @Override
    boolean isAlwaysLatchedExclusively() {
        return true;
    }

    @Override
    public boolean isBIN() {
        return true;
    }

    /**
     * Overrides the IN method to account for deltas.
     *
     * This method relies on setLastFullVersion being called first when a delta
     * is fetched, which is done by BINDelta.reconstituteBIN.
     */
    @Override
    void setLastLoggedLsn(long lsn) {
        if (getLastFullVersion() == DbLsn.NULL_LSN) {
            setLastFullLsn(lsn);
        } else {
            lastDeltaVersion = lsn;
        }
    }

    /**
     * Overrides the IN method to account for deltas.
     */
    @Override
    public long getLastLoggedVersion() {
        return (lastDeltaVersion != DbLsn.NULL_LSN) ?
               lastDeltaVersion :
               getLastFullVersion();
    }

    /**
     * Overrides the IN method to account for deltas.
     * Public for unit testing.
     */
    @Override
    public long getLastDeltaVersion() {
        return lastDeltaVersion;
    }

    /**
     * If cleaned or compressed, must log full version.
     */
    @Override
    public void setProhibitNextDelta() {
        prohibitNextDelta = true;
    }

    /**
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    @Override
    protected boolean isEvictionProhibited() {
        return (nCursors() > 0);
    }

    /**
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    @Override
    boolean hasPinnedChildren() {

        DatabaseImpl db = getDatabase();

        /*
         * For the mapping DB, if any MapLN is resident we cannot evict this
         * BIN.  If a MapLN was not previously stripped, then the DB may be
         * open.  [#13415]
         */
        if (db.getId().equals(DbTree.ID_DB_ID)) {
            return hasResidentChildren();
        }

        /*
         * We can always evict this BIN because its children are limited to
         * LNs.  When logging the BIN, any dirty LNs will be logged and
         * non-dirty LNs can be discarded.
         */
        return false;
    }

    /**
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    @Override
    int getChildEvictionType() {

        Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();

        for (int i = 0; i < getNEntries(); i++) {
            Node node = getTarget(i);
            if (node != null) {
                if (node instanceof LN) {
                    LN ln = (LN) node;

                    /*
                     * If the LN is not evictable, we may neither strip the LN
                     * nor evict the node.  isEvictableInexact is used here as
                     * a fast check, to avoid the overhead of acquiring a
                     * handle lock while selecting an IN for eviction.   See
                     * evictInternal which will call LN.isEvictable to acquire
                     * an handle lock and guarantee that another thread cannot
                     * open the MapLN.  [#13415]
                     */
                    if (!ln.isEvictableInexact()) {
                        return MAY_NOT_EVICT;
                    }

                    /*
                     * If the cleaner allows eviction, then this LN may be
                     * stripped.
                     */
                    if (cleaner.isEvictable(this, i, false /*latched*/)) {
                        return MAY_EVICT_LNS;
                    }
                } else {
                    return MAY_NOT_EVICT;
                }
            }
        }
        return MAY_EVICT_NODE;
    }

    /**
     * Indicates whether entry 0's key is "special" in that it always compares
     * less than any other key.  BIN's don't have the special key, but IN's do.
     */
    @Override
    boolean entryZeroKeyComparesLow() {
        return false;
    }

    /**
     * Mark this entry as deleted, using the delete flag. Only BINS may do
     * this.
     *
     * @param index indicates target entry
     */
    @Override
    public void setKnownDeleted(int index) {

        /*
         * The target is cleared to save memory, since a known deleted entry
         * will never be fetched.  The migrate flag is also cleared since
         * migration is never needed for known deleted entries either.
         */
        super.setKnownDeleted(index);

        /*
         * We know it's an LN because we never call setKnownDeleted for
         * an IN.
         */
        LN oldLN = (LN) getTarget(index);
        updateMemorySize(oldLN, null /* newNode */);
        if (oldLN != null) {
            oldLN.releaseMemoryBudget();
        }
        setMigrate(index, false);
        setTarget(index, null);
        setDirty(true);
    }

    public void setKnownDeletedClearAll(int index) {
        setKnownDeleted(index);
        setLsnElement(index, DbLsn.NULL_LSN);
    }

    /**
     * Clear the known deleted flag. Only BINS may do this.
     * @param index indicates target entry
     */
    @Override
    public void clearKnownDeleted(int index) {
        super.clearKnownDeleted(index);
        setDirty(true);
    }

    @Override
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.BIN_FIXED_OVERHEAD;
    }

    /**
     * Returns the treeAdmin memory in objects referenced by this BIN.
     * Specifically, this refers to the DbFileSummaryMap held by
     * MapLNs
     */
    @Override
    public long getTreeAdminMemorySize() {

        if (getDatabase().getId().equals(DbTree.ID_DB_ID)) {
            long treeAdminMem = 0;
            for (int i = 0; i < getMaxEntries(); i++) {
                Node n = getTarget(i);
                if (n != null) {
                    MapLN mapLN = (MapLN) n;
                    treeAdminMem += mapLN.getDatabase().getTreeAdminMemory();
                }
            }
            return treeAdminMem;
        } else {
            return 0;
        }
    }

    /*
     * Cursors
     */

    /* public for the test suite. */
    public Set<CursorImpl> getCursorSet() {
        return cursorSet.copy();
    }

    /**
     * Register a cursor with this BIN.  Caller has this BIN already latched.
     * @param cursor Cursor to register.
     */
    public void addCursor(CursorImpl cursor) {
        assert isLatchOwnerForWrite();
        cursorSet.add(cursor);
    }

    /**
     * Unregister a cursor with this bin.  Caller has this BIN already
     * latched.
     *
     * @param cursor Cursor to unregister.
     */
    public void removeCursor(CursorImpl cursor) {
        assert isLatchOwnerForWrite();
        cursorSet.remove(cursor);
    }

    /**
     * @return the number of cursors currently referring to this BIN.
     */
    public int nCursors() {
        return cursorSet.size();
    }

    /**
     * Called when we know we are about to split on behalf of a key that is the
     * minimum (leftSide) or maximum (!leftSide) of this node.  This is
     * achieved by just forcing the split to occur either one element in from
     * the left or the right (i.e. splitIndex is 1 or nEntries - 1).
     */
    @Override
    void splitSpecial(IN parent,
                      int parentIndex,
                      int maxEntriesPerNode,
                      byte[] key,
                      boolean leftSide,
                      CacheMode cacheMode)
        throws DatabaseException {

        int index = findEntry(key, true, false);
        int nEntries = getNEntries();
        boolean exact = (index & IN.EXACT_MATCH) != 0;
        index &= ~IN.EXACT_MATCH;
        if (leftSide &&
            index < 0) {
            splitInternal(parent, parentIndex, maxEntriesPerNode,
                          1, cacheMode);
        } else if (!leftSide &&
                   !exact &&
                   index == (nEntries - 1)) {
            splitInternal(parent, parentIndex, maxEntriesPerNode,
                          nEntries - 1, cacheMode);
        } else {
            split(parent, parentIndex, maxEntriesPerNode, cacheMode);
        }
    }

    /**
     * Adjust any cursors that are referring to this BIN.  This method is
     * called during a split operation.  "this" is the BIN being split.
     * newSibling is the new BIN into which the entries from "this" between
     * newSiblingLow and newSiblingHigh have been copied.
     *
     * @param newSibling - the newSibling into which "this" has been split.
     * @param newSiblingLow
     * @param newSiblingHigh - the low and high entry of
     * "this" that were moved into newSibling.
     */
    @Override
    void adjustCursors(IN newSibling,
                       int newSiblingLow,
                       int newSiblingHigh) {
        assert newSibling.isLatchOwnerForWrite();
        assert this.isLatchOwnerForWrite();
        int adjustmentDelta = (newSiblingHigh - newSiblingLow);
        Iterator<CursorImpl> iter = cursorSet.iterator();
        while (iter.hasNext()) {
            CursorImpl cursor = iter.next();
            if (cursor.getBINToBeRemoved() == this) {

                /*
                 * This BIN will be removed from the cursor by CursorImpl
                 * following advance to next BIN; ignore it.
                 */
                continue;
            }
            int cIdx = cursor.getIndex();
            BIN cBin = cursor.getBIN();
            assert cBin == this :
                "nodeId=" + getNodeId() +
                " cursor=" + cursor.dumpToString(true);
            assert newSibling instanceof BIN;

            /*
             * There are four cases to consider for cursor adjustments,
             * depending on (1) how the existing node gets split, and (2) where
             * the cursor points to currently.  In cases 1 and 2, the id key of
             * the node being split is to the right of the splitindex so the
             * new sibling gets the node entries to the left of that index.
             * This is indicated by "new sibling" to the left of the vertical
             * split line below.  The right side of the node contains entries
             * that will remain in the existing node (although they've been
             * shifted to the left).  The vertical bar (^) indicates where the
             * cursor currently points.
             *
             * case 1:
             *
             *   We need to set the cursor's "bin" reference to point at the
             *   new sibling, but we don't need to adjust its index since that
             *   continues to be correct post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *         cursor ^
             *
             * case 2:
             *
             *   We only need to adjust the cursor's index since it continues
             *   to point to the current BIN post-split.
             *
             *   +=======================================+
             *   |  new sibling        |  existing node  |
             *   +=======================================+
             *                              cursor ^
             *
             * case 3:
             *
             *   Do nothing.  The cursor continues to point at the correct BIN
             *   and index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *         cursor ^
             *
             * case 4:
             *
             *   Adjust the "bin" pointer to point at the new sibling BIN and
             *   also adjust the index.
             *
             *   +=======================================+
             *   |  existing Node        |  new sibling  |
             *   +=======================================+
             *                                 cursor ^
             */
            BIN ns = (BIN) newSibling;
            if (newSiblingLow == 0) {
                if (cIdx < newSiblingHigh) {
                    /* case 1 */
                    iter.remove();
                    cursor.setBIN(ns);
                    ns.addCursor(cursor);
                } else {
                    /* case 2 */
                    cursor.setIndex(cIdx - adjustmentDelta);
                }
            } else {
                if (cIdx >= newSiblingLow) {
                    /* case 4 */
                    cursor.setIndex(cIdx - newSiblingLow);
                    iter.remove();
                    cursor.setBIN(ns);
                    ns.addCursor(cursor);
                }
            }
        }
    }

    /**
     * For each cursor in this BIN's cursor set, ensure that the cursor is
     * actually referring to this BIN.
     */
    public void verifyCursors() {
        if (cursorSet != null) {
            for (CursorImpl cursor : cursorSet) {
                if (cursor.getBINToBeRemoved() != this) {
                    BIN cBin = cursor.getBIN();
                    assert cBin == this;
                }
            }
        }
    }

    /**
     * Adjust cursors referring to this BIN following an insert.
     *
     * @param insertIndex - The index of the new entry.
     */
    @Override
    void adjustCursorsForInsert(int insertIndex) {
        assert this.isLatchOwnerForWrite();
        /* cursorSet may be null if this is being created through
           createFromLog() */
        if (cursorSet != null) {
            for (CursorImpl cursor : cursorSet) {
                if (cursor.getBINToBeRemoved() != this) {
                    int cIdx = cursor.getIndex();
                    if (insertIndex <= cIdx) {
                        cursor.setIndex(cIdx + 1);
                    }
                }
            }
        }
    }

    /**
     * Compress this BIN by removing any entries that are deleted.  No cursors
     * may be present on the BIN.  Caller is responsible for latching and
     * unlatching this node.
     *
     * @param localTracker is used only for temporary DBs, and may be specified
     * to consolidate multiple tracking operations.  If null, the tracking is
     * performed immediately in this method.
     *
     * @return true if all deleted slots were compressed, or false if one or
     * more slots could not be compressed because we were unable to obtain a
     * lock.
     */
    public boolean compress(LocalUtilizationTracker localTracker)
        throws DatabaseException {

        /* 
         * If the environment is not yet recovered we can't rely on locks
         * being set up to safeguard active data and so we can't compress
         * safely.
         */
        if (!databaseImpl.getDbEnvironment().isValid()) {
            return false;
        }
        
        if (nCursors() > 0) {
            throw EnvironmentFailureException.unexpectedState();
        }

        boolean setNewIdKey = false;
        boolean anyLocksDenied = false;
        final DatabaseImpl db = getDatabase();
        final EnvironmentImpl envImpl = db.getDbEnvironment();

        for (int i = 0; i < getNEntries(); i++) {

            /* KD and PD determine deletedness. */
            if (!isEntryPendingDeleted(i) && !isEntryKnownDeleted(i)) {
                continue;
            }

            /*
             * We have to be able to lock the LN before we can compress the
             * entry.  If we can't, then skip over it.
             *
             * We must lock the LN even if isKnownDeleted is true, because
             * locks protect the aborts. (Aborts may execute multiple
             * operations, where each operation latches and unlatches. It's the
             * LN lock that protects the integrity of the whole multi-step
             * process.)
             *
             * For example, during abort, there may be cases where we have
             * deleted and then added an LN during the same txn.  This means
             * that to undo/abort it, we first delete the LN (leaving
             * knownDeleted set), and then add it back into the tree.  We want
             * to make sure the entry is in the BIN when we do the insert back
             * in.
             */
            final BasicLocker lockingTxn =
                BasicLocker.createBasicLocker(envImpl);
            /* Don't allow this short-lived lock to be preempted/stolen. */
            lockingTxn.setPreemptable(false);
            try {
                /* Lock LSN.  Can discard a NULL_LSN entry without locking. */
                final long lsn = getLsn(i);
                if (lsn != DbLsn.NULL_LSN) {
                    final LockResult lockRet = lockingTxn.nonBlockingLock
                        (lsn, LockType.READ, false /*jumpAheadOfWaiters*/, db);
                    if (lockRet.getLockGrant() == LockGrantType.DENIED) {
                        anyLocksDenied = true;
                        continue;
                    }
                }

                /* At this point, we know we can delete. */
                if (Key.compareKeys(getKey(i), getIdentifierKey(),
                                    getKeyComparator()) == 0) {

                    /*
                     * We're about to remove the entry with the idKey so the
                     * node will need a new idkey.
                     */
                    setNewIdKey = true;
                    
                    /*
                     * We think identifier keys are always in the first slot.
                     * However, this assertion fails in DatabaseTest.  Needs
                     * futher investigation.
                     */
                    //assert (i == 0) : i;
                }

                if (db.isDeferredWriteMode()) {
                    final LN ln = (LN) getTarget(i);
                    if (ln != null &&
                        ln.isDirty() &&
                        !DbLsn.isTransient(lsn)) {
                        if (db.isTemporary()) {

                            /*
                             * When a previously logged LN in a temporary DB is
                             * dirty, we can count the LSN of the last logged
                             * LN as obsolete without logging.  There is no
                             * requirement for the dirty deleted LN to be
                             * durable past recovery.  There is no danger of
                             * the last logged LN being accessed again (after
                             * log cleaning, for example), since temporary DBs
                             * do not survive recovery.
                             */
                            if (localTracker != null) {
                                localTracker.countObsoleteNode
                                    (lsn, ln.getGenericLogType(),
                                     ln.getLastLoggedSize(), db);
                            } else {
                                envImpl.getLogManager().countObsoleteNode
                                    (lsn, ln.getGenericLogType(),
                                     ln.getLastLoggedSize(), db,
                                     true /*countExact*/);
                            }
                        } else {

                            /*
                             * When a previously logged deferred-write LN is
                             * dirty, we log the dirty deleted LN to make the
                             * deletion durable.  The act of logging will also
                             * count the last logged LSN as obsolete.
                             */
                            logDirtyLN(i, ln, false /*ensureDurableLsn*/);
                        }
                    }
                }

                boolean deleteSuccess = deleteEntry(i, true);
                assert deleteSuccess;

                /*
                 * Since we're deleting the current entry, bump the current
                 * index back down one.
                 */
                i--;
            } finally {
                lockingTxn.operationEnd();
            }
        }

        if (getNEntries() != 0 && setNewIdKey) {
            setIdentifierKey(getKey(0));
        }

        /* This BIN is empty and expendable. */
        if (getNEntries() == 0) {
            setGeneration(0);
        }

        return !anyLocksDenied;
    }

    /**
     * This method is called whenever a deleted slot is observed (when the
     * slot's PendingDeleted or KnownDeleted flag is set), to ensure that the
     * slot is compressed away.  This is an attempt to process slots that were
     * not compressed during the mainstream record deletion process because of
     * cursors on the BIN during compress, or a crash prior to compression.
     */
    public void queueSlotDeletion() {

        /*
         * If we will log a delta (which includes the case where no slots are
         * dirty), set the BIN dirty to ensure we compress it later in the
         * beforeLog method.
         */
        if (shouldLogDelta()) {
            setDirty(true);
            return;
        }

        /* If we will next log a full version, add to the queue. */
        final EnvironmentImpl envImpl = getDatabase().getDbEnvironment();
        envImpl.addToCompressorQueue(this, false);
    }

    @Override
    public boolean isCompressible() {
        return true;
    }

    /**
     * Reduce memory consumption by evicting all LN targets. Note that this may
     * cause LNs to be logged, which would require marking this BIN dirty.
     *
     * The BIN should be latched by the caller.
     *
     * @return number of evicted bytes. Note that a 0 return does not
     * necessarily mean that the BIN had no evictable LNs. It's possible that
     * resident, dirty LNs were not lockable.
     */
    public long evictLNs()
        throws DatabaseException {

        assert isLatchOwnerForWrite() :
            "BIN must be latched before evicting LNs";

        Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();

        /*
         * We can't evict an LN which is pointed to by a cursor, in case that
         * cursor has a reference to the LN object. We'll take the cheap choice
         * and avoid evicting any LNs if there are cursors on this BIN. We
         * could do a more expensive, precise check to see entries have which
         * cursors. This is something we might move to later.
         */
        long removed = 0;
        if (nCursors() == 0) {
            for (int i = 0; i < getNEntries(); i++) {
                removed += evictInternal(i, cleaner);
            }
            updateMemorySize(removed, 0);
        }

        /* May decrease the memory footprint by changing the INTargetRep. */
        if (removed > 0) {
            compactMemory();
        }

        return removed;
    }

    /**
     * Evict a single LN if allowed and adjust the memory budget.
     */
    public void evictLN(int index)
        throws DatabaseException {

        Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();
        long removed = evictInternal(index, cleaner);
        updateMemorySize(removed, 0);

        /* May decrease the memory footprint by changing the INTargetRep. */
        if (removed > 0) {
            compactMemory();
        }
    }

    /**
     * Evict a single LN if allowed.  The amount of memory freed is returned
     * and must be subtracted from the memory budget by the caller.
     *
     * @return number of evicted bytes. Note that a 0 return does not
     * necessarily mean there was no eviction because the targetLN was not
     * resident. It's possible that resident, dirty LNs were not lockable.
     */
    private long evictInternal(int index, Cleaner cleaner)
        throws DatabaseException {

        final Node n = getTarget(index);

        if (n instanceof LN) {
            final LN ln = (LN) n;
            final long lsn = getLsn(index);

            /*
             * Don't evict MapLNs for open databases (LN.isEvictable) [#13415].
             * And don't strip LNs that the cleaner will be migrating
             * (Cleaner.isEvictable).
             */
            if (ln.isEvictable(lsn) &&
                cleaner.isEvictable(this, index, true /*latched*/)) {

                /* Log target if necessary. */
                logDirtyLN(index, ln, true /*ensureDurableLsn*/);

                /* Clear target. */
                setTarget(index, null);
                ln.releaseMemoryBudget();

                return n.getMemorySizeIncludedByParent();
            }
        }
        return 0;
    }

    /**
     * Logs the LN at the given index if it is dirty.
     */
    private void logDirtyLN(int index, LN ln, boolean ensureDurableLsn)
        throws DatabaseException {

        final long oldLsn = getLsn(index);
        final boolean force = ensureDurableLsn &&
                              getDatabase().isDeferredWriteMode() &&
                              DbLsn.isTransientOrNull(oldLsn);
        if (force || ln.isDirty()) {
            final DatabaseImpl dbImpl = getDatabase();
            final EnvironmentImpl envImpl = dbImpl.getDbEnvironment();

            /* Only deferred write databases should have dirty LNs. */
            assert dbImpl.isDeferredWriteMode();

            /*
             * Do not lock while logging.  Locking of new LSN is performed by
             * lockAfterLsnChange. This should never be part of the replication
             * stream, because this is a deferred-write DB.
             */
            final long newLsn = ln.log(envImpl, dbImpl, getKey(index), oldLsn,
                                       true /*backgroundIO*/,
                                       ReplicationContext.NO_REPLICATE);
            updateEntry(index, newLsn);
            /* Lock new LSN on behalf of existing lockers. */
            CursorImpl.lockAfterLsnChange(dbImpl, oldLsn, newLsn,
                                          null /*excludeLocker*/);

            /*
             * It is desirable to evict a non-dirty LN in a duplicates DB
             * because it will never be fetched again.  However, this causes
             * memory budgeting errors in tests and is currently disabled.
             */
            /*
            if (databaseImpl.getSortedDuplicates()) {
                evictLN(index);
            }
            */
        }
    }

    /* For debugging.  Overrides method in IN. */
    @Override
    boolean validateSubtreeBeforeDelete(int index) {
        return true;
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It may not have any LN children.
     *
     * We assume that this is only called under an assert.
     */
    @Override
    boolean isValidForDelete()
        throws DatabaseException {

        int validIndex = 0;
        int numValidEntries = 0;
        boolean needToLatch = !isLatchOwnerForWrite();
        try {
            if (needToLatch) {
                latch();
            }
            for (int i = 0; i < getNEntries(); i++) {
                if (!isEntryKnownDeleted(i)) {
                    numValidEntries++;
                    validIndex = i;
                }
            }

            if (numValidEntries > 0) { // any valid entries, not eligable
                return false;
            }
            if (nCursors() > 0) {      // cursors on BIN, not eligable
                return false;
            }
            return true;               // 0 entries, no cursors
        } finally {
            if (needToLatch &&
                isLatchOwnerForWrite()) {
                releaseLatch();
            }
        }
    }

    /*
     * DbStat support.
     */
    @Override
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
        acc.processBIN(this, Long.valueOf(getNodeId()), getLevel());
    }

    @Override
    public String beginTag() {
        return BEGIN_TAG;
    }

    @Override
    public String endTag() {
        return END_TAG;
    }

    /*
     * Logging support
     */

    /**
     * @see IN#logDirtyChildren
     */
    @Override
    public void logDirtyChildren()
        throws DatabaseException {

        /* Look for targets that are dirty. */
        EnvironmentImpl envImpl = getDatabase().getDbEnvironment();
        for (int i = 0; i < getNEntries(); i++) {
            Node node = getTarget(i);
            if (node != null) {
                logDirtyLN(i, (LN) node, true /*ensureDurableLsn*/);
            }
        }
    }

    /**
     * @see IN#incEvictStats
     */
    @Override
    public void incEvictStats(EvictionSource source) {
        databaseImpl.getDbEnvironment().getEvictor().incBINEvictStats(source);
    }

    /**
     * @see Node#incFetchStats
     */
    @Override
    public void incFetchStats(EnvironmentImpl envImpl, boolean isMiss) {
        envImpl.getEvictor().incBINFetchStats(isMiss);
    }

    /**
     * @see IN#getLogType
     */
    @Override
    public LogEntryType getLogType() {
        return LogEntryType.LOG_BIN;
    }

    @Override
    public String shortClassName() {
        return "BIN";
    }

    /**
     * Overrides the IN method to account for deltas.
     */
    @Override
    public void beforeLog(LogManager logManager,
                          INLogItem item,
                          INLogContext context) {

        final DatabaseImpl dbImpl = getDatabase();
        final EnvironmentImpl envImpl = dbImpl.getDbEnvironment();

        /* Allow the cleaner to migrate LNs before logging. */
        envImpl.getCleaner().lazyMigrateLNs(this, context.backgroundIO);

        /* Determine whether we log a delta rather than full version. */
        final boolean doDeltaLog;
        final BINDelta deltaInfo;
        if (context.allowDeltas && shouldLogDelta()) {
            doDeltaLog = true;
            deltaInfo = new BINDelta(this);
        } else {
            doDeltaLog = false;
            deltaInfo = null;
        }

        /* Perform lazy compression when logging a full BIN. */
        if (context.allowCompress && !doDeltaLog) {
            envImpl.lazyCompress(this);
        }

        /*
         * Write dirty LNs in deferred-write databases.  This is done after
         * compression to reduce total logging, at least for temp DBs.
         */
        if (dbImpl.isDeferredWriteMode()) {
            logDirtyLNs();
        }

        /*
         * In the Btree, the parent IN slot contains the latest full version
         * LSN or, if a delta was last logged, the delta LSN.  Somewhat
         * redundantly, the transient IN.lastFullVersion and
         * BIN.lastDeltaVersion fields contain the last logged full version and
         * delta version LSNs.
         *
         * For delta logging:
         *  + Count lastDeltaVersion obsolete, if non-null.
         *  + Set lastDeltaVersion to newly logged LSN.
         *  + Leave lastFullVersion unchanged.
         *
         * For full version logging:
         *  + Count lastFullVersion and lastDeltaVersion obsolete, if non-null.
         *  + Set lastFullVersion to newly logged LSN.
         *  + Set lastDeltaVersion to null.
         */
        item.isDelta = doDeltaLog;
        beforeLogCommon(item, context,
                        doDeltaLog ? DbLsn.NULL_LSN : getLastFullVersion(),
                        lastDeltaVersion);
        item.entry = doDeltaLog ?
            (new BINDeltaLogEntry(deltaInfo)) :
            (new INLogEntry(this));
    }

    /**
     * Overrides the IN method to account for deltas.  See beforeLog.
     */
    @Override
    public void afterLog(LogManager logManager,
                         INLogItem item,
                         INLogContext context) {

        afterLogCommon(logManager, item, context,
                       item.isDelta ? DbLsn.NULL_LSN : getLastFullVersion(),
                       lastDeltaVersion);

        if (item.isDelta) {
            lastDeltaVersion = item.newLsn;
            numDeltasSinceLastFull += 1;
        } else {
            setLastFullLsn(item.newLsn);
            lastDeltaVersion = DbLsn.NULL_LSN;
            numDeltasSinceLastFull = 0;

            /*
             * Before logging a full version BIN we attempted to compress it.
             * If we could not compress a slot because of the presence of
             * cursors, we must re-queue (or at least re-dirty) the BIN so
             * that we will compress it later.  The BIN is set non-dirty by
             * afterLogCommon above.
             */
            for (int i = 0; i < getNEntries(); i += 1) {
                if (isEntryKnownDeleted(i) || isEntryPendingDeleted(i)) {
                    queueSlotDeletion();
                    break;
                }
            }
        }

        prohibitNextDelta = false;
    }

    private void logDirtyLNs()
        throws DatabaseException {

        for (int i = 0; i < getNEntries(); i++) {
            final Node node = getTarget(i);
            if ((node != null) && (node instanceof LN)) {
                logDirtyLN(i, (LN) node, true /*ensureDurableLsn*/);
            }
        }
    }

    /**
     * Decide whether to log a full or partial BIN, depending on the ratio of
     * the delta size to full BIN size, and the number of deltas that have been
     * logged since the last full.
     *
     * Other factors are taken into account:
     * + a delta cannot be logged if the BIN has never been logged before
     * + deltas are not currently supported for DeferredWrite databases
     * + this particular delta may have been prohibited because the cleaner is
     *   migrating the BIN or a slot has been deleted
     * + if there are no dirty slots, we might as well log a full BIN
     *
     * @return true if we should log the deltas of this BIN
     */
    public boolean shouldLogDelta() {

        final DatabaseImpl dbImpl = getDatabase();

        /* Cheapest checks first. */
        if (prohibitNextDelta ||
            dbImpl.isDeferredWriteMode() ||
            (getLastFullVersion() == DbLsn.NULL_LSN) ||
            (numDeltasSinceLastFull >= dbImpl.getBinMaxDeltas())) {
            return false;
        }

        /* Must count deltas to check further. */
        final int numDeltas = BINDelta.getNumDeltas(this);
        
        /* A delta with zero items is not valid. */
        if (numDeltas <= 0) {
            return false;
        }

        /* Check the configured BinDeltaPercent. */
        final int maxDiffs =
            (getNEntries() * dbImpl.getBinDeltaPercent()) / 100;
        if (numDeltas > maxDiffs) {
            return false;
        }

        return true;
    }

    /**
     * We require exclusive latches on a BIN, so this method does not need to
     * declare that it throws RelatchRequiredException.
     */
    @Override
    public Node fetchTarget(int idx)
        throws DatabaseException {

        try {
            return super.fetchTarget(idx);
        } catch (RelatchRequiredException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }
}
