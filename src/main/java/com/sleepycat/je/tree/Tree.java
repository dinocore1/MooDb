/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_RELATCHES_REQUIRED;
import static com.sleepycat.je.dbi.BTreeStatDefinition.BTREE_ROOT_SPLITS;
import static com.sleepycat.je.dbi.BTreeStatDefinition.GROUP_DESC;
import static com.sleepycat.je.dbi.BTreeStatDefinition.GROUP_NAME;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.BtreeStats;
import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.latch.SharedLatch;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.recovery.RecoveryManager;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.IntStat;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.RelatchRequiredException;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Tree implements the JE B+Tree.
 *
 * A note on tree search patterns:
 * There's a set of Tree.search* methods. Some clients of the tree use
 * those search methods directly, whereas other clients of the tree
 * tend to use methods built on top of search.
 *
 * The semantics of search* are
 *   they leave you pointing at a BIN or IN
 *   they don't tell you where the reference of interest is.
 * The semantics of the get* methods are:
 *   they leave you pointing at a BIN or IN
 *   they return the index of the slot of interest
 *   they traverse down to whatever level is needed
 *   they are built on top of search* methods.
 * For the future:
 * Over time, we need to clarify which methods are to be used by clients
 * of the tree. Preferably clients that call the tree use get*, although
 * their are cases where they need visibility into the tree structure.
 *
 * Also, search* should return the location of the slot to save us a
 * second binary search.
 *
 * Search Method Call Hierarchy
 * ----------------------------
 * getFirst/LastNode
 *  search
 *  CALLED BY:
 *   CursorImpl.getFirstOrLast
 *
 * getNext/PrevBin
 *  getParentINForChildIN
 *  searchSubTree
 *  CALLED BY:
 *   DupConvert
 *   CursorImpl.getNext
 *
 * getParentINForChildIN
 *  IN.findParent
 *  does not use shared latching
 *  CALLED BY:
 *   Checkpointer.flushIN (doFetch=false, targetLevel=-1)
 *   FileProcessor.processIN (doFetch=true, targetLevel=LEVEL)
 *   Evictor.evictIN (doFetch=true, targetLevel=-1)
 *   RecoveryManager.replaceOrInsertChild (doFetch=true, targetLevel=-1)
 *   getNext/PrevBin (doFetch=true, targetLevel=-1)
 *
 * search
 *  searchSubTree
 *  CALLED BY:
 *   CursorImpl.searchAndPosition
 *   INCompressor to find BIN
 *  
 * searchSubTree 
 *  uses shared grandparent latching
 *
 * getParentBINForChildLN
 *  searchSplitsAllowed
 *   CALLED BY:
 *    RecoveryManager.redo
 *    RecoveryManager.recoveryUndo
 *  search
 *   CALLED BY:
 *    RecoveryManager.abortUndo
 *    RecoveryManager.rollbackUndo
 *    FileProcessor.processLN
 *    Cleaner.processPendingLN
 *    UtilizationProfile.verifyLsnIsObsolete (utility)
 *
 * findBinForInsert
 *  searchSplitsAllowed
 *  CALLED BY:
 *   CursorImpl.putInternal
 *
 * searchSplitsAllowed
 *  uses shared non-grandparent latching
 *  CALLED BY:
 *   DupConvert (instead of findBinForInsert, which needs a cursor)
 *
 * Possible Shared Latching Improvements
 * -------------------------------------
 * By implementing grandparent latching in searchSplitsAllowed we would
 * increase performance slightly in these cases:
 *  Insertions
 *
 * By implementing shared latching in getParentINForChildIN we would get better
 * concurrency in these cases:
 *  Cursor scan, when moving between BINs
 *  Eviction
 *  Checkpoints
 *  Cleaning INs
 *  
 * By implementing shared latching for BINs we would get better concurrency in
 * these cases:
 *  Reads when LN is in cache, or LN is not needed (key-only op, e.g., dups)
 */
public final class Tree implements Loggable {

    /* For debug tracing */
    private static final String TRACE_ROOT_SPLIT = "RootSplit:";

    private DatabaseImpl database;
    private ChildReference root;
    private int maxTreeEntriesPerNode;

    /* Stats */
    private StatGroup stats;

    /* The number of tree root splited. */
    private IntStat rootSplits;
    /* The number of latch upgrades from shared to exclusive required. */
    private LongStat relatchesRequired;

    /*
     * Latch that must be held when using/accessing the root node.  Protects
     * against the root being changed out from underneath us by splitRoot.
     */
    private SharedLatch rootLatch;

    private ThreadLocal<TreeWalkerStatsAccumulator> treeStatsAccumulatorTL =
        new ThreadLocal<TreeWalkerStatsAccumulator>();

    /*
     * We don't need the stack trace on this so always throw a static and
     * avoid the cost of Throwable.fillInStack() every time it's thrown.
     * [#13354].
     */
    private static SplitRequiredException splitRequiredException =
        new SplitRequiredException();

    /**
     * Embodies an enum for the type of search being performed.  NORMAL means
     * do a regular search down the tree.  LEFT/RIGHT means search down the
     * left/right side to find the first/last node in the tree.
     */
    public static class SearchType {
        /* Search types */
        public static final SearchType NORMAL = new SearchType();
        public static final SearchType LEFT   = new SearchType();
        public static final SearchType RIGHT  = new SearchType();

        /* No lock types can be defined outside this class. */
        private SearchType() {
        }
    }

    /* For unit tests */
    private TestHook waitHook; // used for generating race conditions
    private TestHook searchHook; // [#12736]
    private TestHook ckptHook; // [#13897]

    /**
     * Create a new tree.
     */
    public Tree(DatabaseImpl database) {
        init(database);
        setDatabase(database);
    }

    /**
     * Create a tree that's being read in from the log.
     */
    public Tree() {
        init(null);
        maxTreeEntriesPerNode = 0;
    }

    /**
     * constructor helper
     */
    private void init(DatabaseImpl database) {
        rootLatch = new SharedLatch("RootLatch");
        this.root = null;
        this.database = database;

        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        relatchesRequired = new LongStat(stats, BTREE_RELATCHES_REQUIRED);
        rootSplits = new IntStat(stats, BTREE_ROOT_SPLITS);
    }

    /**
     * Set the database for this tree. Used by recovery when recreating an
     * existing tree.
     */
    public void setDatabase(DatabaseImpl database) {
        this.database = database;

        maxTreeEntriesPerNode = database.getNodeMaxTreeEntries();
    }

    /**
     * @return the database for this Tree.
     */
    public DatabaseImpl getDatabase() {
        return database;
    }

    /**
     * Set the root for the tree. Should only be called within the root latch.
     */
    public void setRoot(ChildReference newRoot, boolean notLatched) {
        assert (notLatched || rootLatch.isWriteLockedByCurrentThread());
        root = newRoot;
    }

    public ChildReference makeRootChildReference(Node target,
                                                 byte[] key,
                                                 long lsn) {
        return new RootChildReference(target, key, lsn);
    }

    private ChildReference makeRootChildReference() {
        return new RootChildReference();
    }

    /*
     * A tree doesn't have a root if (a) the root field is null, or (b) the
     * root is non-null, but has neither a valid target nor a valid LSN. Case
     * (b) can happen if the database is or was previously opened in deferred
     * write mode.
     *
     * @return false if there is no real root.
     */
    public boolean rootExists() {
        if (root == null) {
            return false;
        }

        if ((root.getTarget() == null) &&
            (root.getLsn() == DbLsn.NULL_LSN)) {
            return false;
        }

        return true;
    }

    /**
     * Perform a fast check to see if the root IN is resident.  No latching is
     * performed.  To ensure that the root IN is not loaded by another thread,
     * this method should be called while holding a write lock on the MapLN.
     * That will prevent opening the DB in another thread, and potentially
     * loading the root IN. [#13415]
     */
    public boolean isRootResident() {
        return root != null && root.getTarget() != null;
    }

    /*
     * Class that overrides fetchTarget() so that if the rootLatch is not
     * held exclusively when the root is fetched, we upgrade it to exclusive.
     */
    private class RootChildReference extends ChildReference {

        private RootChildReference() {
            super();
        }

        private RootChildReference(Node target, byte[] key, long lsn) {
            super(target, key, lsn);
        }

        /* Caller is responsible for releasing rootLatch. */
        @Override
        public Node fetchTarget(DatabaseImpl database, IN in)
            throws DatabaseException {

            if (getTarget() == null &&
                !rootLatch.isWriteLockedByCurrentThread()) {
                rootLatch.release();
                rootLatch.acquireExclusive();
            }

            return super.fetchTarget(database, in);
        }

        @Override
        public void setTarget(Node target) {
            assert rootLatch.isWriteLockedByCurrentThread();
            super.setTarget(target);
        }

        @Override
        public void clearTarget() {
            assert rootLatch.isWriteLockedByCurrentThread();
            super.clearTarget();
        }

        @Override
        public void setLsn(long lsn) {
            assert rootLatch.isWriteLockedByCurrentThread();
            super.setLsn(lsn);
        }

        @Override
        void updateLsnAfterOptionalLog(DatabaseImpl dbImpl, long lsn) {
            assert rootLatch.isWriteLockedByCurrentThread();
            super.updateLsnAfterOptionalLog(dbImpl, lsn);
        }
    }

    /**
     * Get LSN of the rootIN. Obtained without latching, should only be
     * accessed while quiescent.
     */
    public long getRootLsn() {
        if (root == null) {
            return DbLsn.NULL_LSN;
        } else {
            return root.getLsn();
        }
    }

    /**
     * Cheaply calculates and returns the maximum possible number of LNs in the
     * btree.
     */
    public long getMaxLNs() {
        final int levels;
        final int topLevelSlots;
        rootLatch.acquireShared();
        try {
            IN rootIN = (IN) root.fetchTarget(database, null);
            levels = rootIN.getLevel() & IN.LEVEL_MASK;
            topLevelSlots = rootIN.getNEntries();
        } finally {
            rootLatch.release();
        }
        return (long) (topLevelSlots *
                       Math.pow(database.getNodeMaxTreeEntries(), levels - 1));
    }

    /**
     * @return the TreeStats for this tree.
     */
    int getTreeStats() {
        return rootSplits.get();
    }

    private TreeWalkerStatsAccumulator getTreeStatsAccumulator() {
        if (EnvironmentImpl.getThreadLocalReferenceCount() > 0) {
            return treeStatsAccumulatorTL.get();
        } else {
            return null;
        }
    }

    public void setTreeStatsAccumulator(TreeWalkerStatsAccumulator tSA) {
        treeStatsAccumulatorTL.set(tSA);
    }

    public IN withRootLatchedExclusive(WithRootLatched wrl)
        throws DatabaseException {

        try {
            rootLatch.acquireExclusive();
            return wrl.doWork(root);
        } finally {
            rootLatch.release();
        }
    }

    public IN withRootLatchedShared(WithRootLatched wrl)
        throws DatabaseException {

        try {
            rootLatch.acquireShared();
            return wrl.doWork(root);
        } finally {
            rootLatch.release();
        }
    }

    public void latchRootLatchExclusive()
        throws DatabaseException {

        rootLatch.acquireExclusive();
    }

    public void releaseRootLatch()
        throws DatabaseException {

        rootLatch.releaseIfOwner();
    }

    /**
     * Deletes a BIN specified by key from the tree. If the BIN resides in a
     * subtree that can be pruned away, prune as much as possible, so we
     * don't leave a branch that has no BINs.
     *
     * It's possible that the targeted BIN will now have entries, or will
     * have resident cursors. Either will prevent deletion.
     *
     * @param idKey - the identifier key of the node to delete.
     * @param localTracker is used for tracking obsolete node info.
     */
    public void delete(byte[] idKey,
                       LocalUtilizationTracker localTracker)
        throws DatabaseException,
               NodeNotEmptyException,
               CursorsExistException {

        final EnvironmentImpl envImpl = database.getDbEnvironment();
        final INList inList = envImpl.getInMemoryINs();
        IN subtreeRootIN = null;

        /*
         * A delete is a reverse split that must be propagated up to the root.
         * [#13501] Keep all nodes from the rootIN to the parent of the
         * deletable subtree latched as we descend so we can log the
         * IN deletion and cascade the logging up the tree. The latched
         * nodes are kept in order in the nodeLadder.
         */
        final ArrayList<SplitInfo> nodeLadder = new ArrayList<SplitInfo>();

        IN rootIN = null;
        boolean rootNeedsUpdating = false;
        rootLatch.acquireExclusive();
        try {
            if (!rootExists()) {
                /* no action, tree is deleted or was never persisted. */
                return;
            }

            rootIN = (IN) root.fetchTarget(database, null);
            rootIN.latch(CacheMode.UNCHANGED);

            searchDeletableSubTree(rootIN, idKey, nodeLadder);
            if (nodeLadder.size() == 0) {

                /*
                 * The tree is empty, so do nothing.  Root compression is no
                 * longer supported.  Root compression has no impact on memory
                 * usage now that we evict the root IN.  It reduces log space
                 * taken by INs for empty (but not removed) databases, yet
                 * requires logging a INDelete and MapLN; this provides very
                 * little benefit, if any.  Because it requires extensive
                 * testing (which has not been done), this minor benefit is not
                 * worth the cost.  And by removing it we no longer log
                 * INDelete, which reduces complexity going forward. [#17546]
                 */
            } else {
                /* Detach this subtree. */
                SplitInfo detachPoint =
                    nodeLadder.get(nodeLadder.size() - 1);
                boolean deleteOk =
                    detachPoint.parent.deleteEntry(detachPoint.index,
                                                   true);
                assert deleteOk;

                subtreeRootIN = detachPoint.child;

                /*
                 * For a deferred-write DB, account for deleted INs.  For a
                 * deferred-write DB, they can't be actually counted obsolete
                 * until the parent is logged at a later time.  [#21348]
                 *
                 * We do this before calling cascadeUpdates, because in the
                 * future it may also apply to a normal DB, in which case they
                 * will be actually counted obsolete when the parent is logged
                 * by cascadeUpdates.
                 */
                if (database.isDeferredWriteMode()) {
                    subtreeRootIN.accountForDeferredWriteSubtreeRemoval
                        (inList, detachPoint.parent);
                }

                /* Cascade updates upward, including writing the root IN. */
                rootNeedsUpdating = cascadeUpdates(nodeLadder, -1);
            }
        } finally {
            releaseNodeLadderLatches(nodeLadder);

            if (rootIN != null) {
                rootIN.releaseLatch();
            }

            rootLatch.release();
        }

        if (subtreeRootIN != null) {

            if (rootNeedsUpdating) {

                /*
                 * modifyDbRoot will grab locks and we can't have the root
                 * latch held while it tries to acquire locks.
                 */
                envImpl.getDbTree().optionalModifyDbRoot(database);
                /* TODO: change trace, not actually a root deletion. */
                RecoveryManager.traceRootDeletion
                    (envImpl.getLogger(), database);
            }

            if (!database.isDeferredWriteMode()) {

                /*
                 * Count obsolete nodes after logging the delete. We can do
                 * this without having the parent of the subtree latched
                 * because the subtree has been detached from the tree.
                 */
                subtreeRootIN.accountForSubtreeRemoval(inList, localTracker);
            }

            LoggerUtils.envLogMsg(Level.FINE, envImpl,
                                  "SubtreeRemoval: subtreeRoot = " +
                                  subtreeRootIN.getNodeId());
        }
    }

    private void releaseNodeLadderLatches(ArrayList<SplitInfo> nodeLadder)
        throws DatabaseException {

        /*
         * Clear any latches left in the node ladder. Release from the
         * bottom up.
         */
        ListIterator<SplitInfo> iter =
            nodeLadder.listIterator(nodeLadder.size());
        while (iter.hasPrevious()) {
            SplitInfo info = iter.previous();
            info.child.releaseLatch();
        }
    }

    /**
     * Update nodes for a delete, going upwards. For example, suppose a
     * node ladder holds:
     * INa, INb, index for INb in INa
     * INb, INc, index for INc in INb
     * INc, BINd, index for BINd in INc
     *
     * When we enter this method, BINd has already been removed from INc. We
     * need to
     *  - log INc
     *  - update INb, log INb
     *  - update INa, log INa
     *
     * @param nodeLadder List of SplitInfos describing each node pair on the
     * downward path
     * @param index slot occupied by this din tree.
     * @return whether the DB root needs updating.
     */
    private boolean cascadeUpdates(ArrayList<SplitInfo> nodeLadder, int index)
        throws DatabaseException {

        ListIterator<SplitInfo> iter =
            nodeLadder.listIterator(nodeLadder.size());
        EnvironmentImpl envImpl = database.getDbEnvironment();
        LogManager logManager = envImpl.getLogManager();

        long newLsn = DbLsn.NULL_LSN;
        SplitInfo info = null;
        while (iter.hasPrevious()) {
            info = iter.previous();

            if (newLsn != DbLsn.NULL_LSN) {
                info.parent.updateEntry(info.index, newLsn);
            }
            newLsn = info.parent.optionalLog(logManager);
        }

        boolean rootNeedsUpdating = false;
        if (info != null) {
            /* We've logged the top of this subtree, record it properly. */
            assert info.parent.isDbRoot();
            /* We updated the rootIN of the database. */
            assert rootLatch.isWriteLockedByCurrentThread();
            root.updateLsnAfterOptionalLog(database, newLsn);
            rootNeedsUpdating = true;
        }
        return rootNeedsUpdating;
    }

    /**
     * Find the leftmost node (IN or BIN) in the tree.
     *
     * @return the leftmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    public IN getFirstNode(CacheMode cacheMode)
        throws DatabaseException {

        return search(null, SearchType.LEFT, null, cacheMode,
                      null /*searchComparator*/);
    }

    /**
     * Find the rightmost node (IN or BIN) in the tree.
     *
     * @return the rightmost node in the tree, null if the tree is empty.  The
     * returned node is latched and the caller must release it.
     */
    public IN getLastNode(CacheMode cacheMode)
        throws DatabaseException {

        return search(null, SearchType.RIGHT, null, cacheMode,
                      null /*searchComparator*/);
    }

    /**
     * GetParentNode without optional tracking.
     */
    public SearchResult getParentINForChildIN(IN child,
                                              boolean requireExactMatch,
                                              CacheMode cacheMode)
        throws DatabaseException {

        return getParentINForChildIN
            (child, requireExactMatch, cacheMode, -1 /*targetLevel*/, null);
    }

    /**
     * Return a reference to the parent or possible parent of the child.  Used
     * by objects that need to take a standalone node and find it in the tree,
     * like the evictor, checkpointer, and recovery.
     *
     * @param child The child node for which to find the parent.  This node is
     * latched by the caller and is released by this function before returning
     * to the caller.
     *
     * @param requireExactMatch if true, we must find the exact parent, not a
     * potential parent.
     *
     * @param cacheMode The CacheMode for affecting the hotness of the tree.
     *
     * @param trackingList if not null, add the LSNs of the parents visited
     * along the way, as a debug tracing mechanism. This is meant to stay in
     * production, to add information to the log.
     *
     * @return a SearchResult object. If the parent has been found,
     * result.foundExactMatch is true. If any parent, exact or potential has
     * been found, result.parent refers to that node.
     */
    public SearchResult getParentINForChildIN(IN child,
                                              boolean requireExactMatch,
                                              CacheMode cacheMode,
                                              int targetLevel,
                                              List<TrackingInfo> trackingList)
        throws DatabaseException {

        /* Sanity checks */
        if (child == null) {
            throw EnvironmentFailureException.unexpectedState
                ("getParentNode passed null");
        }

        assert child.isLatchOwnerForWrite();

        /*
         * Get information from child before releasing latch.
         */
        byte[] treeKey = child.getIdentifierKey();
        boolean isRoot = child.isRoot();
        child.releaseLatch();

        return getParentINForChildIN(child.getNodeId(),
                                     isRoot,
                                     treeKey,
                                     requireExactMatch,
                                     cacheMode,
                                     targetLevel,
                                     trackingList,
                                     true);
    }

    /**
     * Return a reference to the parent or possible parent of the child.  Used
     * by objects that need to take a node ID and find it in the tree,
     * like the evictor, checkpointer, and recovery.
     *
     * @param requireExactMatch if true, we must find the exact parent, not a
     * potential parent.
     *
     * @param cacheMode The CacheMode for affecting the hotness of the tree.
     *
     * @param trackingList if not null, add the LSNs of the parents visited
     * along the way, as a debug tracing mechanism. This is meant to stay in
     * production, to add information to the log.
     *
     * @param doFetch if false, stop the search if we run into a non-resident
     * child. Used by the checkpointer to avoid conflicting with work done
     * by the evictor.
     *
     * @return a SearchResult object. If the parent has been found,
     * result.foundExactMatch is true. If any parent, exact or potential has
     * been found, result.parent refers to that node.
     */
    public SearchResult getParentINForChildIN(long targetNodeId,
                                              boolean targetIsRoot,
                                              byte[] targetTreeKey,
                                              boolean requireExactMatch,
                                              CacheMode cacheMode,
                                              int targetLevel,
                                              List<TrackingInfo> trackingList,
                                              boolean doFetch)
        throws DatabaseException {

        /*
         * Use exclusive latching. Since the caller will be logging the child
         * IN, the parent IN must be latched exclusively. [#18567]
         */
        IN rootIN = getRootINLatchedExclusive(cacheMode);

        SearchResult result = new SearchResult();
        if (rootIN != null) {
            /* The tracking list is a permanent tracing aid. */
            if (trackingList != null) {
                trackingList.add(new TrackingInfo(root.getLsn(),
                                                  rootIN.getNodeId(),
                                                  rootIN.getNEntries()));
            }

            IN potentialParent = rootIN;
            boolean success = false;

            try {
                while (result.keepSearching) {

                    /*
                     * [12736] Prune away oldBin.  Assert has intentional
                     * side effect.
                     */
                    assert TestHookExecute.doHookIfSet(searchHook);

                    potentialParent.findParent(SearchType.NORMAL,
                                               targetNodeId,
                                               targetIsRoot,
                                               targetTreeKey,
                                               result,
                                               requireExactMatch,
                                               cacheMode,
                                               targetLevel,
                                               doFetch);

                    /* Update tracking list. */
                    if (trackingList != null) {
                        trackingList.get(trackingList.size() - 1).
                            setIndex(result.index);
                        if (result.keepSearching) {
                            trackingList.add(new TrackingInfo
                                (potentialParent.getLsn(result.index),
                                 result.parent.getNodeId(),
                                 result.parent.getNEntries()));
                        }
                    }

                    /* Move to next potential parent. */
                    potentialParent = result.parent;
                }
                success = true;

            } catch (RelatchRequiredException e) {
                /* Should never happen because we use exclusive latches. */
                throw EnvironmentFailureException.unexpectedException(e);
            } finally {

                /*
                 * The only thing that can be latched at this point is
                 * potentialParent.
                 */
                if (!success) {
                    potentialParent.releaseLatch();
                }
            }
        }
        return result;
    }

    /**
     * Return a reference to the parent of this LN. This searches through the
     * tree and allows splits. Set the tree location to the proper BIN parent
     * whether or not the LN child is found. That's because if the LN is not
     * found, recovery or abort will need to place it within the tree, and so
     * we must point at the appropriate position.
     *
     * <p>When this method returns with location.bin non-null, the BIN is
     * latched and must be unlatched by the caller.  Note that location.bin may
     * be non-null even if this method returns false.</p>
     *
     * @param location a holder class to hold state about the location
     * of our search. Sort of an internal cursor.
     *
     * @param key key to navigate through main key
     *
     * @param splitsAllowed true if this method is allowed to cause tree splits
     * as a side effect. In practice, recovery can cause splits, but abort
     * can't.
     *
     * @param cacheMode The CacheMode for affecting the hotness of the tree.
     *
     * @return true if node found in tree.
     * If false is returned and there is the possibility that we can insert
     * the record into a plausible parent we must also set
     * - location.bin (may be null if no possible parent found)
     * - location.lnKey (don't need to set if no possible parent).
     */
    public boolean getParentBINForChildLN(TreeLocation location,
                                          byte[] key,
                                          boolean splitsAllowed,
                                          boolean findDeletedEntries,
                                          CacheMode cacheMode)
        throws DatabaseException {

        /*
         * Find the BIN that either points to this LN or could be its
         * ancestor.
         */
        location.reset();
        IN searchResult;
        if (splitsAllowed) {
            searchResult = searchSplitsAllowed(key, cacheMode,
                                               null /*searchComparator*/);
        } else {
            searchResult = search(key, SearchType.NORMAL, null, cacheMode,
                                  null /*searchComparator*/);
        }

        if (searchResult == null) {
            return false;
        }

        try {
            location.bin = (BIN) searchResult;

            /*
             * If caller wants us to consider knownDeleted entries then do an
             * inexact search in findEntry since that will find knownDeleted
             * entries.  If caller doesn't want us to consider knownDeleted
             * entries then do an exact search in findEntry since that will not
             * return knownDeleted entries.
             */
            boolean exactSearch = false;
            boolean indicateIfExact = true;
            if (!findDeletedEntries) {
                exactSearch = true;
                indicateIfExact = false;
            }
            location.index =
                location.bin.findEntry(key, indicateIfExact, exactSearch);

            boolean match = false;
            if (findDeletedEntries) {
                match = (location.index >= 0 &&
                         (location.index & IN.EXACT_MATCH) != 0);
                location.index &= ~IN.EXACT_MATCH;
            } else {
                match = (location.index >= 0);
            }

            if (match) {
                location.childLsn = location.bin.getLsn(location.index);
                return true;
            } else {
                location.lnKey = key;
                return false;
            }
        } catch (RuntimeException e) {
            searchResult.releaseLatch();
            throw e;
        }
    }

    /**
     * Return a reference to the adjacent BIN.
     *
     * @param bin The BIN to find the next BIN for.  This BIN is latched.
     *
     * @return The next BIN, or null if there are no more.  The returned node
     * is latched and the caller must release it.  If null is returned, the
     * argument BIN remains latched.
     */
    public BIN getNextBin(BIN bin,
                          CacheMode cacheMode)
        throws DatabaseException {

        return getNextBinInternal(bin, true, cacheMode);
    }

    /**
     * Return a reference to the previous BIN.
     *
     * @param bin The BIN to find the next BIN for.  This BIN is latched.
     *
     * @return The previous BIN, or null if there are no more.  The returned
     * node is latched and the caller must release it.  If null is returned,
     * the argument bin remains latched.
     */
    public BIN getPrevBin(BIN bin,
                          CacheMode cacheMode)
        throws DatabaseException {

        return getNextBinInternal(bin, false, cacheMode);
    }

    /**
     * Helper routine for above two routines to iterate through BIN's.
     */
    private BIN getNextBinInternal(BIN bin,
                                   boolean forward,
                                   CacheMode cacheMode)
        throws DatabaseException {

        /*
         * Use the right most key (for a forward progressing cursor) or the
         * left most key (for a backward progressing cursor) as the idkey.  The
         * reason is that the BIN may get split while finding the next BIN so
         * it's not safe to take the BIN's identifierKey entry.  If the BIN
         * gets splits, then the right (left) most key will still be on the
         * resultant node.  The exception to this is that if there are no
         * entries, we just use the identifier key.
         */
        byte[] idKey = null;

        if (bin.getNEntries() == 0) {
            idKey = bin.getIdentifierKey();
        } else if (forward) {
            idKey = bin.getKey(bin.getNEntries() - 1);
        } else {
            idKey = bin.getKey(0);
        }

        IN next = bin;
        boolean nextIsLatched = false;

        assert LatchSupport.countLatchesHeld() == 1:
            LatchSupport.latchesHeldToString();

        /*
         * Ascend the tree until we find a level that still has nodes to the
         * right (or left if !forward) of the path that we're on.  If we reach
         * the root level, we're done.
         */
        IN parent = null;
        IN nextIN = null;
        boolean nextINIsLatched = false;
        try {
            while (true) {

                /*
                 * Move up a level from where we are now and check to see if we
                 * reached the top of the tree.
                 */
                SearchResult result = null;
                nextIsLatched = false;
                result = getParentINForChildIN
                    (next, true /*requireExactMatch*/, cacheMode);
                if (result.exactParentFound) {
                    parent = result.parent;
                } else {
                    /* We've reached the root of the tree. */
                    assert (LatchSupport.countLatchesHeld() == 0):
                        LatchSupport.latchesHeldToString();
                    return null;
                }

                assert (LatchSupport.countLatchesHeld() == 1) :
                    LatchSupport.latchesHeldToString();

                /*
                 * Figure out which entry we are in the parent.  Add (subtract)
                 * 1 to move to the next (previous) one and check that we're
                 * still pointing to a valid child.  Don't just use the result
                 * of the parent.findEntry call in getParentNode, because we
                 * want to use our explicitly chosen idKey.
                 */
                int index = parent.findEntry(idKey, false, false);
                boolean moreEntriesThisBin = false;
                if (forward) {
                    index++;
                    if (index < parent.getNEntries()) {
                        moreEntriesThisBin = true;
                    }
                } else {
                    if (index > 0) {
                        moreEntriesThisBin = true;
                    }
                    index--;
                }

                if (moreEntriesThisBin) {

                    /*
                     * There are more entries to the right of the current path
                     * in parent.  Get the entry, and then descend down the
                     * left most path to a BIN.
                     */
                    nextIN = (IN) parent.fetchTargetWithExclusiveLatch(index);
                    nextIN.latch(cacheMode);
                    nextINIsLatched = true;

                    assert (LatchSupport.countLatchesHeld() == 2):
                        LatchSupport.latchesHeldToString();

                    if (nextIN.isBIN()) {
                        /* We landed at a leaf (i.e. a BIN). */
                        parent.releaseLatch();
                        parent = null; // to avoid falsely unlatching parent
                        TreeWalkerStatsAccumulator treeStatsAccumulator =
                            getTreeStatsAccumulator();
                        if (treeStatsAccumulator != null) {
                            nextIN.accumulateStats(treeStatsAccumulator);
                        }

                        return (BIN) nextIN;
                    } else {

                        /*
                         * We landed at an IN.  Descend down to the appropriate
                         * leaf (i.e. BIN) node.
                         */
                        IN ret = searchSubTree(nextIN, null,
                                               (forward ?
                                                SearchType.LEFT :
                                                SearchType.RIGHT),
                                               null,
                                               cacheMode,
                                               null /*searchComparator*/);
                        nextINIsLatched = false;
                        parent.releaseLatch();
                        parent = null; // to avoid falsely unlatching parent

                        assert LatchSupport.countLatchesHeld() == 1:
                            LatchSupport.latchesHeldToString();

                        if (ret.isBIN()) {
                            return (BIN) ret;
                        } else {
                            throw EnvironmentFailureException.unexpectedState
                                ("subtree did not have a BIN for leaf");
                        }
                    }
                }

                /* Nothing at this level.  Ascend to a higher level. */
                next = parent;
                nextIsLatched = true;
                parent = null; // to avoid falsely unlatching parent below
            }
        } catch (DatabaseException e) {

            if (next != null &&
                nextIsLatched) {
                next.releaseLatch();
            }

            if (parent != null) {
                parent.releaseLatch();
            }

            if (nextIN != null &&
                nextINIsLatched) {
                nextIN.releaseLatch();
            }

            throw e;
        }
    }

    /**
     * Split the root of the tree.
     */
    private void splitRoot(CacheMode cacheMode)
        throws DatabaseException {

        /*
         * Create a new root IN, insert the current root IN into it, and then
         * call split.
         */
        EnvironmentImpl env = database.getDbEnvironment();
        LogManager logManager = env.getLogManager();
        INList inMemoryINs = env.getInMemoryINs();

        IN curRoot = null;
        curRoot = (IN) root.fetchTarget(database, null);
        curRoot.latch(cacheMode);
        long curRootLsn = 0;
        long logLsn = 0;
        IN newRoot = null;
        try {

            /*
             * Make a new root IN, giving it an id key from the previous root.
             */
            byte[] rootIdKey = curRoot.getKey(0);
            newRoot = new IN(database, rootIdKey, maxTreeEntriesPerNode,
                             curRoot.getLevel() + 1);
            newRoot.latch(cacheMode);
            newRoot.setIsRoot(true);
            curRoot.setIsRoot(false);

            /*
             * Make the new root IN point to the old root IN. Log the old root
             * provisionally, because we modified it so it's not the root
             * anymore, then log the new root. We are guaranteed to be able to
             * insert entries, since we just made this root.
             */
            try {
                curRootLsn =
                    curRoot.optionalLogProvisional(logManager, newRoot);
                boolean insertOk = newRoot.insertEntry
                    (new ChildReference(curRoot, rootIdKey, curRootLsn));
                assert insertOk;

                logLsn = newRoot.optionalLog(logManager);
            } catch (DatabaseException e) {
                /* Something went wrong when we tried to log. */
                curRoot.setIsRoot(true);
                throw e;
            }
            inMemoryINs.add(newRoot);

            /*
             * Make the tree's root reference point to this new node. Now the
             * MapLN is logically dirty, but the change hasn't been logged.  Be
             * sure to flush the MapLN if we ever evict the root.
             */
            root.setTarget(newRoot);
            root.updateLsnAfterOptionalLog(database, logLsn);
            curRoot.split(newRoot, 0, maxTreeEntriesPerNode, cacheMode);
            root.setLsn(newRoot.getLastLoggedVersion());

        } finally {
            /* FindBugs ignore possible null pointer dereference of newRoot. */
            newRoot.releaseLatch();
            curRoot.releaseLatch();
        }
        rootSplits.increment();
        traceSplitRoot(Level.FINE, TRACE_ROOT_SPLIT, newRoot, logLsn,
                       curRoot, curRootLsn);
    }

    /**
     * Search the tree, starting at the root.  Depending on search type either
     * search using key, or search all the way down the right or left sides.
     * Stop the search either when the bottom of the tree is reached, or a node
     * matching nid is found (see below) in which case that node's parent is
     * returned.
     *
     * Preemptive splitting is not done during the search.
     *
     * @param key - the key to search for, or null if searchType is LEFT or
     * RIGHT.
     *
     * @param searchType - The type of tree search to perform.  NORMAL means
     * we're searching for key in the tree.  LEFT/RIGHT means we're descending
     * down the left or right side, resp.  DELETE means we're descending the
     * tree and will return the lowest node in the path that has > 1 entries.
     *
     * @param binBoundary - If non-null, information is returned about whether
     * the BIN found is the first or last BIN in the database.
     *
     * @return - the Node that matches the criteria, if any.  This is the node
     * that is farthest down the tree with a match.  Returns null if the root
     * is null.  Node is latched (unless it's null) and must be unlatched by
     * the caller.  Only IN's and BIN's are returned, not LN's.  In a NORMAL
     * search, It is the caller's responsibility to do the findEntry() call on
     * the key and BIN to locate the entry that matches key.  The return value
     * node is latched upon return and it is the caller's responsibility to
     * unlatch it.
     */
    public IN search(byte[] key,
                     SearchType searchType,
                     BINBoundary binBoundary,
                     CacheMode cacheMode,
                     Comparator<byte[]> searchComparator) {

        IN rootIN = getRootIN(cacheMode);

        if (rootIN != null) {
            return searchSubTree(rootIN, key, searchType, binBoundary,
                                 cacheMode, searchComparator);
        } else {
            return null;
        }
    }

    /**
     * Do a key based search, permitting pre-emptive splits. Returns the
     * target node's parent.
     */
    public IN searchSplitsAllowed(byte[] key,
                                  CacheMode cacheMode,
                                  Comparator<byte[]> searchComparator) {
        IN insertTarget = null;
        while (insertTarget == null) {
            rootLatch.acquireShared();
            boolean rootLatched = true;
            boolean rootLatchedExclusive = false;
            boolean rootINLatched = false;
            boolean success = false;
            IN rootIN = null;
            try {
                while (true) {
                    if (rootExists()) {
                        rootIN = (IN) root.fetchTarget(database, null);

                        /* Check if root needs splitting. */
                        if (rootIN.needsSplitting()) {
                            if (!rootLatchedExclusive) {
                                rootIN = null;
                                rootLatch.release();
                                rootLatch.acquireExclusive();
                                rootLatchedExclusive = true;
                                continue;
                            }
                            splitRoot(cacheMode);

                            /*
                             * We can't hold any latches while we lock.  If the
                             * root splits again between latch release and
                             * DbTree.db lock, no problem.  The latest root
                             * will still get written out.
                             */
                            rootLatch.release();
                            rootLatched = false;
                            EnvironmentImpl env = database.getDbEnvironment();
                            env.getDbTree().optionalModifyDbRoot(database);
                            rootLatched = true;
                            rootLatch.acquireExclusive();
                            rootIN = (IN) root.fetchTarget(database, null);
                            rootIN.latch(cacheMode);
                        } else {
                            rootIN.latchShared(cacheMode);
                        }
                        rootINLatched = true;
                    }
                    break;
                }
                success = true;
            } finally {
                if (!success && rootINLatched) {
                    rootIN.releaseLatch();
                }
                if (rootLatched) {
                    rootLatch.release();
                }
            }

            /* Don't loop forever if the root is null. [#13897] */
            if (rootIN == null) {
                break;
            }

            try {
                assert rootINLatched;
                while (true) {
                    try {
                        insertTarget = searchSubTreeSplitsAllowed
                            (rootIN, key, cacheMode, searchComparator);
                        break;
                    } catch (RelatchRequiredException RRE) {
                        relatchesRequired.increment();
                        database.getDbEnvironment().incRelatchesRequired();
                        rootLatch.acquireExclusive();
                        rootIN = (IN) root.fetchTarget(database, null);
                        rootIN.latch(cacheMode);
                        rootLatch.release();
                        continue;
                    }
                }
            } catch (SplitRequiredException e) {

                /*
                 * The last slot in the root was used at the point when this
                 * thread released the rootIN latch in order to force splits.
                 * Retry. SR [#11147].
                 */
                continue;
            }
        }

        return insertTarget;
    }

    public void loadStats(StatsConfig config, BtreeStats btreeStats) {
        /* Add the tree statistics to BtreeStats. */
        btreeStats.setTreeStats(stats.cloneGroup(false));

        if (config.getClear()) {
            relatchesRequired.clear();
        }
    }

    /**
     * Wrapper for searchSubTreeInternal that does a restart if a
     * RelatchRequiredException is thrown (i.e. a relatch of the root is
     * needed).
     */
    private IN searchSubTree(IN parent,
                             byte[] key,
                             SearchType searchType,
                             BINBoundary binBoundary,
                             CacheMode cacheMode,
                             Comparator<byte[]> searchComparator) {

        /*
         * If a an intermediate IN (e.g., from getNextBinInternal) was
         * originally passed, it was latched exclusively.
         */
        assert parent == null ||
               parent.isRoot() ||
               parent.isLatchOwnerForWrite();

        /*
         * Max of two iterations required.  First is root latched shared, and
         * second is root latched exclusive.
         */
        for (int i = 0; i < 2; i++) {
            try {
                return searchSubTreeInternal
                    (parent, key, searchType, binBoundary, cacheMode,
                     searchComparator);
            } catch (RelatchRequiredException RRE) {

                /*
                 * The original parent param was the DB root IN if this
                 * exception occurs, so latch it exclusively and retry.  If an
                 * intermediate IN was originally passed, it was latched
                 * exclusively and this can't happen.  See assertion at top of
                 * method.
                 */
                parent = getRootINLatchedExclusive(cacheMode);
            }
        }

        throw EnvironmentFailureException.unexpectedState
            ("searchSubTreeInternal should have completed in two tries");
    }

    /**
     * Searches a portion of the tree starting at parent using key.  If
     * searchType is NORMAL, then key must be supplied to guide the search.  If
     * searchType is LEFT (or RIGHT), then the tree is searched down the left
     * (or right) side to find the first (or last) leaf, respectively.
     * <p>
     * Enters with parent latched, assuming it's not null.  Exits with the
     * return value latched, assuming it's not null.
     * 
     * @param parent - the root of the subtree to start the search at.  This
     * node should be latched by the caller and will be unlatched prior to
     * return.
     *
     * @param key - the key to search for, unless searchType is LEFT or RIGHT
     *
     * @param searchType - NORMAL means search using key
     *                     LEFT means find the first (leftmost) leaf
     *                     RIGHT means find the last (rightmost) leaf
     *
     * @return - the node matching the argument criteria, or null.  The node is
     * latched and must be unlatched by the caller.  The parent argument and
     * any other nodes that are latched during the search are unlatched prior
     * to return.
     *
     * @throws RelatchRequiredException if the root node (parent) must be
     * relatched exclusively because a null target was encountered (i.e. a
     * fetch must be performed on parent's child and the parent is latched
     * shared.
     */
    private IN searchSubTreeInternal(IN parent,
                                     byte[] key,
                                     SearchType searchType,
                                     BINBoundary binBoundary,
                                     CacheMode cacheMode,
                                     Comparator<byte[]> searchComparator)
        throws RelatchRequiredException {

        /* Return null if we're passed a null arg. */
        if (parent == null) {
            return null;
        }

        if ((searchType == SearchType.LEFT ||
             searchType == SearchType.RIGHT) &&
            key != null) {

            /*
             * If caller is asking for a right or left search, they shouldn't
             * be passing us a key.
             */
            throw EnvironmentFailureException.unexpectedState
                ("searchSubTree passed key and left/right search");
        }

        assert parent.isLatchOwnerForRead();

        if (binBoundary != null) {
            binBoundary.isLastBin = true;
            binBoundary.isFirstBin = true;
        }

        int index;
        IN child = null;
        IN grandParent = null;
        boolean childIsLatched = false;
        boolean grandParentIsLatched = false;
        boolean maintainGrandParentLatches = !parent.isLatchOwnerForWrite();

        TreeWalkerStatsAccumulator treeStatsAccumulator =
            getTreeStatsAccumulator();

        boolean success = false;
        try {
            do {
                if (treeStatsAccumulator != null) {
                    parent.accumulateStats(treeStatsAccumulator);
                }

                if (parent.getNEntries() == 0) {
                    /* No more children, can't descend anymore. */
                    success = true;
                    return parent;
                } else if (searchType == SearchType.NORMAL) {
                    /* Look for the entry matching key in the current node. */
                    index = parent.findEntry(key, false, false,
                                             searchComparator);
                } else if (searchType == SearchType.LEFT) {
                    /* Left search, always take the 0th entry. */
                    index = 0;
                } else if (searchType == SearchType.RIGHT) {
                    /* Right search, always take the highest entry. */
                    index = parent.getNEntries() - 1;
                } else {
                    throw EnvironmentFailureException.unexpectedState
                        ("Invalid value of searchType: " + searchType);
                }

                assert index >= 0;

                if (binBoundary != null) {
                    if (index != parent.getNEntries() - 1) {
                        binBoundary.isLastBin = false;
                    }
                    if (index != 0) {
                        binBoundary.isFirstBin = false;
                    }
                }

                /*
                 * Get the child node.  If target is null, and we don't have
                 * parent latched exclusively, then we need to relatch this
                 * parent so that we can fill in the target.  Fetching a target
                 * is a write to a node so it must be exclusively latched.
                 * Once we have the parent relatched exclusively, then we can
                 * release the grand parent.
                 */
                if (maintainGrandParentLatches &&
                    parent.getTarget(index) == null &&
                    !parent.isAlwaysLatchedExclusively()) {

                    if (grandParent == null) {

                        /*
                         * grandParent is null which implies parent is the root
                         * so throw RelatchRequiredException.
                         */
                        throw
                            RelatchRequiredException.relatchRequiredException;
                    }

                    /* Release parent shared and relatch exclusive. */
                    parent.releaseLatch();
                    parent.latch(cacheMode);

                    /*
                     * Once parent has been re-latched exclusive we can release
                     * grandParent so it not latched during fetchTarget.
                     */
                    grandParent.releaseLatch();
                    grandParentIsLatched = false;
                    grandParent = null;
                }
                child = (IN) parent.fetchTarget(index);

                /*
                 * We know we're done with grandParent for sure, so release
                 * now.
                 */
                if (grandParent != null) {
                    grandParent.releaseLatch();
                    grandParentIsLatched = false;
                }

                /* See if we're even using shared latches. */
                if (maintainGrandParentLatches) {
                    /* Note that BINs are always latched exclusive. */
                    child.latchShared(cacheMode);
                } else {
                    child.latch(cacheMode);
                }
                childIsLatched = true;

                if (treeStatsAccumulator != null) {
                    child.accumulateStats(treeStatsAccumulator);
                }

                /* Continue down a level */
                if (maintainGrandParentLatches) {
                    grandParent = parent;
                    grandParentIsLatched = true;
                } else {
                    parent.releaseLatch();
                }
                parent = child;
            } while (!parent.isBIN());

            success = true;
            return child;
        } finally {
            if (!success) {

                /*
                 * In [#14903] we encountered a latch exception below and the
                 * original exception was lost.  Print the stack trace and
                 * allow the original exception to be thrown if this happens
                 * again, to get more information about the problem.
                 */
                try {
                    if (child != null &&
                        childIsLatched) {
                        child.releaseLatch();
                    }

                    if (parent != child) {
                        parent.releaseLatch();
                    }
                } catch (Exception e) {
                    LoggerUtils.traceAndLogException
                        (database.getDbEnvironment(), "Tree",
                         "searchSubTreeInternal", "", e);
                }
            }

            if (grandParent != null &&
                grandParentIsLatched) {
                grandParent.releaseLatch();
                grandParentIsLatched = false;
            }
        }
    }

    /**
     * Search down the tree using a key, but instead of returning the BIN that
     * houses that key, find the point where we can detach a deletable
     * subtree. A deletable subtree is a branch where each IN has one child,
     * and the bottom BIN has no entries and no resident cursors. That point
     * can be found by saving a pointer to the lowest node in the path with
     * more than one entry.
     *
     *              INa
     *             /   \
     *          INb    INc
     *          |       |
     *         INd     ..
     *         / \
     *      INe  ..
     *       |
     *     BINx (suspected of being empty)
     *
     * In this case, we'd like to prune off the subtree headed by INe. INd
     * is the parent of this deletable subtree. As we descend, we must keep
     * latches for all the nodes that will be logged. In this case, we
     * will need to keep INa, INb and INd latched when we return from this
     * method.
     *
     * The method returns a list of parent/child/index structures. In this
     * example, the list will hold:
     *  INa/INb/index
     *  INb/INd/index
     *  INd/INe/index
     * Every node is latched, and every node except for the bottom most child
     * (INe) must be logged.
     */
    public void searchDeletableSubTree(IN parent,
                                       byte[] key,
                                       ArrayList<SplitInfo> nodeLadder)
        throws DatabaseException,
               NodeNotEmptyException,
               CursorsExistException {

        assert (parent!=null);
        assert (key!= null);
        assert parent.isLatchOwnerForWrite();

        int index;
        IN child = null;

        /* Save the lowest IN in the path that has multiple entries. */
        IN lowestMultipleEntryIN = null;

        do {
            if (parent.getNEntries() == 0) {
                break;
            }

            /* Remember if this is the lowest multiple point. */
            if (parent.getNEntries() > 1) {
                lowestMultipleEntryIN = parent;
            }

            index = parent.findEntry(key, false, false);
            assert index >= 0;

            /* Get the child node that matches. */
            child = (IN) parent.fetchTargetWithExclusiveLatch(index);
            child.latch(CacheMode.UNCHANGED);
            nodeLadder.add(new SplitInfo(parent, child, index));

            /* Continue down a level */
            parent = child;
        } while (!parent.isBIN());

        /*
         * See if there is a reason we can't delete this BIN -- i.e.
         * new items have been inserted, or a cursor exists on it.
         */
        if ((child != null) && child.isBIN()) {
            if (child.getNEntries() != 0) {
                throw NodeNotEmptyException.NODE_NOT_EMPTY;
            }

            /*
             * This case can happen if we are keeping a BIN on an empty
             * cursor as we traverse.
             */
            if (((BIN) child).nCursors() > 0) {
                throw CursorsExistException.CURSORS_EXIST;
            }
        }

        if (lowestMultipleEntryIN != null) {

            /*
             * Release all nodes up to the pair that holds the detach
             * point. We won't be needing those nodes, since they'll be
             * pruned and won't need to be updated.
             */
            ListIterator<SplitInfo> iter =
                nodeLadder.listIterator(nodeLadder.size());
            while (iter.hasPrevious()) {
                SplitInfo info = iter.previous();
                if (info.parent == lowestMultipleEntryIN) {
                    break;
                } else {
                    info.child.releaseLatch();
                    iter.remove();
                }
            }
        } else {

            /*
             * We actually have to prune off the entire tree. Release
             * all latches, and clear the node ladder.
             */
            releaseNodeLadderLatches(nodeLadder);
            nodeLadder.clear();
        }
    }

    /**
     * Search the portion of the tree starting at the parent, permitting
     * preemptive splits.
     *
     * When this returns, parent will be unlatched unless parent is the
     * returned IN.
     */
    private IN searchSubTreeSplitsAllowed(IN parent,
                                          byte[] key,
                                          CacheMode cacheMode,
                                          Comparator<byte[]> searchComparator)
        throws RelatchRequiredException,
               SplitRequiredException {

        if (parent != null) {

            /*
             * Search downward until we hit a node that needs a split. In that
             * case, retreat to the top of the tree and force splits downward.
             */
            while (true) {
                try {
                    return searchSubTreeUntilSplit(parent, key, cacheMode,
                                                   searchComparator);
                } catch (SplitRequiredException e) {
                    /* SR [#11144]*/
                    assert TestHookExecute.doHookIfSet(waitHook);

                    /*
                     * ForceSplit may itself throw SplitRequiredException if it
                     * finds that the parent doesn't have room to hold an extra
                     * entry. Allow the exception to propagate up to a place
                     * where it's safe to split the parent. We do this rather
                     * than
                     */
                    parent = forceSplit(parent, key, cacheMode);
                }
            }
        } else {
            return null;
        }
    }

    /**
     * Search the subtree, but throw an exception when we see a node
     * that has to be split.
     *
     * When this returns, parent will be unlatched unless parent is the
     * returned IN.
     */
    private IN searchSubTreeUntilSplit(IN parent,
                                       byte[] key,
                                       CacheMode cacheMode,
                                       Comparator<byte[]> searchComparator)
        throws RelatchRequiredException,
               SplitRequiredException {

        boolean latchingIsExclusive = parent.isLatchOwnerForWrite();

        int index;
        IN child = null;
        boolean childIsLatched = false;
        boolean success = false;

        try {
            do {
                if (parent.getNEntries() == 0) {
                    /* No more children, can't descend anymore. */
                    success = true;
                    return parent;
                } else {
                    /* Look for the entry matching key in the current node. */
                    index = parent.findEntry(key, false, false,
                                             searchComparator);
                }

                assert index >= 0;

                /* Get the child node that matches. */
                child = (IN) parent.fetchTarget(index);
                if (latchingIsExclusive) {
                    child.latch(cacheMode);
                } else {
                    /* Note that BINs are always latched exclusive. */
                    child.latchShared(cacheMode);
                }
                childIsLatched = true;

                /* Throw if we need to split. */
                if (child.needsSplitting()) {
                    /* Try compressing and check again. */
                    database.getDbEnvironment().lazyCompress(child);
                    if (child.needsSplitting()) {
                        /* Let the finally release child and parent latches. */
                        throw splitRequiredException;
                    }
                }

                /* Continue down a level */
                parent.releaseLatch();
                parent = child;
            } while (!parent.isBIN());
            success = true;
            return parent;
        } finally {
            if (!success) {
                if (child != null &&
                    childIsLatched) {
                    child.releaseLatch();
                }
                if (parent != child) {
                    parent.releaseLatch();
                }
            }
        }
    }

    /**
     * Do pre-emptive splitting in the subtree topped by the "parent" node.
     * Search down the tree until we get to the BIN level, and split any nodes
     * that fit the splittable requirement.
     *
     * Note that more than one node in the path may be splittable. For example,
     * a tree might have a level2 IN and a BIN that are both splittable, and
     * would be encountered by the same insert operation.
     *
     * @return the parent to use for retrying the search, which may be
     * different than the parent parameter passed if the root IN has been
     * evicted.
     */
    private IN forceSplit(IN parent, byte[] key, CacheMode cacheMode)
        throws DatabaseException, SplitRequiredException {

        ArrayList<SplitInfo> nodeLadder = new ArrayList<SplitInfo>();

        boolean allLeftSideDescent = true;
        boolean allRightSideDescent = true;
        int index;
        IN child = null;
        IN originalParent = parent;
        ListIterator<SplitInfo> iter = null;

        boolean isRootLatched = false;
        boolean success = false;
        try {

            /*
             * Latch the root in order to update the root LSN when we're done.
             * Latch order must be: root, root IN.  We'll leave this method
             * with the original parent latched.
             *
             * Although we are checking isDbRoot without latching, if it
             * changes (if the root is split) we'll detect this below and throw
             * splitRequiredException.  Note that this property can change from
             * true to false, but never from false to true.
             */
            if (originalParent.isDbRoot()) {
                rootLatch.acquireExclusive();
                isRootLatched = true;
                /* The root IN may have been evicted. [#16173] */
                parent = (IN) root.fetchTarget(database, null);
                originalParent = parent;
            }
            originalParent.latch(cacheMode);

            /*
             * Another thread may have crept in and
             *  - used the last free slot in the parent, making it impossible
             *    to correctly progagate the split.
             *  - actually split the root, in which case we may be looking at
             *    the wrong subtree for this search.
             * If so, throw and retry from above. SR [#11144]
             */
            if (originalParent.needsSplitting() || !originalParent.isRoot()) {
                throw splitRequiredException;
            }

            /*
             * Search downward to the BIN level, saving the information
             * needed to do a split if necessary.
             */
            do {
                if (parent.getNEntries() == 0) {
                    /* No more children, can't descend anymore. */
                    break;
                } else {
                    /* Look for the entry matching key in the current node. */
                    index = parent.findEntry(key, false, false);
                    if (index != 0) {
                        allLeftSideDescent = false;
                    }
                    if (index != (parent.getNEntries() - 1)) {
                        allRightSideDescent = false;
                    }
                }

                assert index >= 0;

                /*
                 * Get the child node that matches. We only need to work on
                 * nodes in residence.
                 */
                child = (IN) parent.getTarget(index);
                if (child == null) {
                    break;
                } else {
                    child.latch(cacheMode);
                    nodeLadder.add(new SplitInfo(parent, child, index));
                }

                /* Continue down a level */
                parent = child;
            } while (!parent.isBIN());

            boolean startedSplits = false;
            LogManager logManager =
                database.getDbEnvironment().getLogManager();

            /*
             * Process the accumulated nodes from the bottom up. Split each
             * node if required. If the node should not split, we check if
             * there have been any splits on the ladder yet. If there are none,
             * we merely release the node, since there is no update.  If splits
             * have started, we need to propagate new LSNs upward, so we log
             * the node and update its parent.
             *
             * Start this iterator at the end of the list.
             */
            iter = nodeLadder.listIterator(nodeLadder.size());
            long lastParentForSplit = Node.NULL_NODE_ID;
            while (iter.hasPrevious()) {
                SplitInfo info = iter.previous();

                /*
                 * Get rid of current entry on nodeLadder so it doesn't get
                 * unlatched in the finally clause.
                 */
                iter.remove();
                child = info.child;
                parent = info.parent;
                index = info.index;

                /* Opportunistically split the node if it is full. */
                if (child.needsSplitting()) {
                    if (allLeftSideDescent || allRightSideDescent) {
                        child.splitSpecial(parent,
                                           index,
                                           maxTreeEntriesPerNode,
                                           key,
                                           allLeftSideDescent,
                                           cacheMode);
                    } else {
                        child.split(parent, index, maxTreeEntriesPerNode,
                                    cacheMode);
                    }
                    lastParentForSplit = parent.getNodeId();
                    startedSplits = true;

                    /*
                     * If the DB root IN was logged, update the DB tree's child
                     * reference.  Now the MapLN is logically dirty, but the
                     * change hasn't been logged. Set the rootIN to be dirty
                     * again, to force flushing the rootIN and mapLN in the
                     * next checkpoint. Be sure to flush the MapLN
                     * if we ever evict the root.
                     */
                    if (parent.isDbRoot()) {
                        assert isRootLatched;
                        root.setLsn(parent.getLastLoggedVersion());
                        parent.setDirty(true);
                    }
                } else {
                    if (startedSplits) {
                        long newLsn = 0;

                        /*
                         * If this child was the parent of a split, it's
                         * already logged by the split call. We just need to
                         * propagate the logging upwards. If this child is just
                         * a link in the chain upwards, log it.
                         */
                        if (lastParentForSplit == child.getNodeId()) {
                            newLsn = child.getLastLoggedVersion();
                        } else {
                            newLsn = child.optionalLog(logManager);
                        }
                        parent.updateEntry(index, newLsn);
                    }
                }
                child.releaseLatch();
                child = null;
            }
            success = true;
        } finally {
            if (!success) {

                /*
                 * This will only happen if an exception is thrown and we leave
                 * things in an intermediate state.
                 */
                if (child != null) {
                    child.releaseLatch();
                }

                if (nodeLadder.size() > 0) {
                    iter = nodeLadder.listIterator(nodeLadder.size());
                    while (iter.hasPrevious()) {
                        SplitInfo info = iter.previous();
                        info.child.releaseLatch();
                    }
                }

                originalParent.releaseLatch();
            }

            if (isRootLatched) {
                rootLatch.release();
            }
        }
        return originalParent;
    }

    /**
     * Helper to obtain the root IN with shared root latching.  Optionally
     * updates the generation of the root when latching it.
     */
    public IN getRootIN(CacheMode cacheMode)
        throws DatabaseException {

        return getRootINInternal(cacheMode, false/*exclusive*/);
    }

    /**
     * Helper to obtain the root IN with exclusive root latching.  Optionally
     * updates the generation of the root when latching it.
     */
    public IN getRootINLatchedExclusive(CacheMode cacheMode)
        throws DatabaseException {

        return getRootINInternal(cacheMode, true/*exclusive*/);
    }

    private IN getRootINInternal(CacheMode cacheMode, boolean exclusive)
        throws DatabaseException {

        rootLatch.acquireShared();
        IN rootIN = null;
        try {
            if (rootExists()) {
                rootIN = (IN) root.fetchTarget(database, null);
                if (exclusive) {
                    rootIN.latch(cacheMode);
                } else {
                    rootIN.latchShared(cacheMode);
                }
            }
            return rootIN;
        } finally {
            rootLatch.release();
        }
    }

    public IN getResidentRootIN(boolean latched)
        throws DatabaseException {

        IN rootIN = null;
        if (rootExists()) {
            rootIN = (IN) root.getTarget();
            if (rootIN != null && latched) {
                rootIN.latchShared(CacheMode.UNCHANGED);
            }
        }
        return rootIN;
    }

    /**
     * Find the BIN that is relevant to the insert.  If the tree doesn't exist
     * yet, then create the first IN and BIN.  On return, the cursor is set to
     * the BIN that is found or created, and the BIN is latched.
     */
    public void findBinForInsert(final byte[] key, final CursorImpl cursor) {

        /*
         * First try using the BIN at the cursor position to avoid a search.
         *
         * Note that although the cursor has a BIN property that we can try to
         * leverage, the cursor is not added to that BIN.  This is important
         * because when we fall through and do a search, and the BIN needs
         * spliting, we compress to avoid splitting the BIN when it contains
         * slots that are deleted.  If the cursor were added to the BIN,
         * compression would not be possible.
         */
        BIN bin = cursor.latchBIN();
        if (bin != null) {
            if (!bin.needsSplitting() && bin.isKeyInBounds(key)) {
                return;
            } else {
                bin.releaseLatch();
            }
        }

        boolean rootLatchIsHeld = false;
        try {
            long logLsn;

            /*
             * We may have to try several times because of a small
             * timing window, explained below.
             */
            while (true) {
                rootLatchIsHeld = true;
                rootLatch.acquireShared();
                if (!rootExists()) {
                    rootLatch.release();
                    rootLatch.acquireExclusive();
                    if (rootExists()) {
                        rootLatch.release();
                        rootLatchIsHeld = false;
                        continue;
                    }

                    final CacheMode cacheMode = cursor.getCacheMode();
                    final EnvironmentImpl env = database.getDbEnvironment();
                    final LogManager logManager = env.getLogManager();
                    final INList inMemoryINs = env.getInMemoryINs();

                    /*
                     * This is an empty tree, either because it's brand new
                     * tree or because everything in it was deleted. Create an
                     * IN and a BIN.  We could latch the rootIN here, but
                     * there's no reason to since we're just creating the
                     * initial tree and we have the rootLatch held. Log the
                     * nodes as soon as they're created, but remember that
                     * referred-to children must come before any references to
                     * their LSNs.
                     */

                    /* First BIN in the tree, log provisionally right away. */
                    bin = new BIN(database, key, maxTreeEntriesPerNode, 1);
                    bin.latch(cacheMode);
                    logLsn = bin.optionalLogProvisional(logManager, null);

                    /*
                     * Log the root right away. Leave the root dirty, because
                     * the MapLN is not being updated, and we want to avoid
                     * this scenario from [#13897], where the LN has no
                     * possible parent.
                     *  provisional BIN
                     *  root IN
                     *  checkpoint start
                     *  LN is logged
                     *  checkpoint end
                     *  BIN is dirtied, but is not part of checkpoint
                     */

                    IN rootIN =
                        new IN(database, key, maxTreeEntriesPerNode, 2);

                    /*
                     * OK to latch the root after a child BIN because it's
                     * during creation.
                     */
                    rootIN.latch(cacheMode);
                    rootIN.setIsRoot(true);

                    boolean insertOk = rootIN.insertEntry
                        (new ChildReference(bin, key, logLsn));
                    assert insertOk;

                    logLsn = rootIN.optionalLog(logManager);
                    rootIN.setDirty(true);  /*force re-logging, see [#13897]*/

                    root = makeRootChildReference(rootIN,
                                                  new byte[0],
                                                  logLsn);

                    rootIN.releaseLatch();

                    /* Add the new nodes to the in memory list. */
                    inMemoryINs.add(bin);
                    inMemoryINs.add(rootIN);
                    rootLatch.release();
                    rootLatchIsHeld = false;

                    break;
                } else {
                    rootLatch.release();
                    rootLatchIsHeld = false;

                    /*
                     * There's a tree here, so search for where we should
                     * insert. However, note that a window exists after we
                     * release the root latch. We release the latch because the
                     * search method expects to take the latch. After the
                     * release and before search, the INCompressor may come in
                     * and delete the entire tree, so search may return with a
                     * null.
                     */
                    IN in = searchSplitsAllowed(key, cursor.getCacheMode(),
                                                null /*searchComparator*/);
                    if (in == null) {
                        /* The tree was deleted by the INCompressor. */
                        continue;
                    } else {
                        /* search() found a BIN where this key belongs. */
                        bin = (BIN) in;
                        break;
                    }
                }
            }
        } finally {
            if (rootLatchIsHeld) {
                rootLatch.release();
            }
        }

        /* testing hook to insert item into log. */
        assert TestHookExecute.doHookIfSet(ckptHook);

        cursor.setBIN(bin);
    }

    /*
     * Logging support
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        int size = 1;                          // rootExists
        if (root != null) {
            size += root.getLogSize();
        }
        return size;
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        byte booleans = (byte) ((root != null) ? 1 : 0);
        logBuffer.put(booleans);
        if (root != null) {
            root.writeToLog(logBuffer);
        }
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean rootExists = false;
        byte booleans = itemBuffer.get();
        rootExists = (booleans & 1) != 0;
        if (rootExists) {
            root = makeRootChildReference();
            root.readFromLog(itemBuffer, entryVersion);
        }
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<root>");
        if (root != null) {
            root.dumpLog(sb, verbose);
        }
        sb.append("</root>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * rebuildINList is used by recovery to add all the resident nodes to the
     * IN list.
     */
    public void rebuildINList()
        throws DatabaseException {

        INList inMemoryList = database.getDbEnvironment().getInMemoryINs();

        if (root != null) {
            rootLatch.acquireShared();
            try {
                Node rootIN = root.getTarget();
                if (rootIN != null) {
                    rootIN.rebuildINList(inMemoryList);
                }
            } finally {
                rootLatch.release();
            }
        }
    }

    /*
     * Debugging stuff.
     */
    public void dump() {
        System.out.println(dumpString(0));
    }

    public String dumpString(int nSpaces) {
        StringBuilder sb = new StringBuilder();
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("<tree>");
        sb.append('\n');
        if (root != null) {
            sb.append(DbLsn.dumpString(root.getLsn(), nSpaces));
            sb.append('\n');
            IN rootIN = (IN) root.getTarget();
            if (rootIN == null) {
                sb.append("<in/>");
            } else {
                sb.append(rootIN.toString());
            }
            sb.append('\n');
        }
        sb.append(TreeUtils.indent(nSpaces));
        sb.append("</tree>");
        return sb.toString();
    }

    /**
     * Unit test support to validate subtree pruning. Didn't want to make root
     * access public.
     */
    boolean validateDelete(int index)
        throws DatabaseException {

        rootLatch.acquireShared();
        try {
            IN rootIN = (IN) root.fetchTarget(database, null);
            return rootIN.validateSubtreeBeforeDelete(index);
        } finally {
            rootLatch.release();
        }
    }

    /**
     * Debugging check that all resident nodes are on the INList and no stray
     * nodes are present in the unused portion of the IN arrays.
     */
    public void validateINList(IN parent)
        throws DatabaseException {

        if (parent == null) {
            parent = (IN) root.getTarget();
        }
        if (parent != null) {
            INList inList = database.getDbEnvironment().getInMemoryINs();
            if (!inList.contains(parent)) {
                throw EnvironmentFailureException.unexpectedState
                    ("IN " + parent.getNodeId() + " missing from INList");
            }
            for (int i = 0;; i += 1) {
                try {
                    Node node = parent.getTarget(i);
                    if (i >= parent.getNEntries()) {
                        if (node != null) {
                            throw EnvironmentFailureException.unexpectedState
                                ("IN " + parent.getNodeId() +
                                 " has stray node " + node +
                                 " at index " + i);
                        }
                        byte[] key = parent.getKey(i);
                        if (key != null) {
                            throw EnvironmentFailureException.unexpectedState
                                ("IN " + parent.getNodeId() +
                                 " has stray key " + key +
                                 " at index " + i);
                        }
                    }
                    if (node instanceof IN) {
                        validateINList((IN) node);
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    break;
                }
            }
        }
    }

    /* For unit testing only. */
    public void setWaitHook(TestHook hook) {
        waitHook = hook;
    }

    /* For unit testing only. */
    public void setSearchHook(TestHook hook) {
        searchHook = hook;
    }

    /* For unit testing only. */
    public void setCkptHook(TestHook hook) {
        ckptHook = hook;
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceSplitRoot(Level level,
                                String splitType,
                                IN newRoot,
                                long newRootLsn,
                                IN oldRoot,
                                long oldRootLsn) {
        Logger logger = database.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(splitType);
            sb.append(" newRoot=").append(newRoot.getNodeId());
            sb.append(" newRootLsn=").
                append(DbLsn.getNoFormatString(newRootLsn));
            sb.append(" oldRoot=").append(oldRoot.getNodeId());
            sb.append(" oldRootLsn=").
                append(DbLsn.getNoFormatString(oldRootLsn));
            LoggerUtils.logMsg
                (logger, database.getDbEnvironment(), level, sb.toString());
        }
    }

    private static class SplitInfo {
        IN parent;
        IN child;
        int index;

        SplitInfo(IN parent, IN child, int index) {
            this.parent = parent;
            this.child = child;
            this.index = index;
        }
    }
}
