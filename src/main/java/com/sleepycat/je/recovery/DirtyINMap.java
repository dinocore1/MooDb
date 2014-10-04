/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.recovery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.recovery.Checkpointer.CheckpointReference;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.utilint.IdentityHashMap;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Manages the by-level map of checkpoint references that are to be flushed by
 * a checkpoint or Database.sync, the MapLNs to be flushed, the highest level
 * by database to be flushed, and the state of the checkpoint.
 *
 * An single instance of this class is used for checkpoints and has the same
 * lifetime as the checkpointer and environment.  An instance per Database.sync
 * is created as needed.  Only one checkpoint can occur at a time, but multiple
 * syncs may occur concurrently with each other and with the checkpoint.
 *
 * The methods in this class are synchronized to protect internal state from
 * concurrent access by the checkpointer and eviction, and to coordinate state
 * changes between the two.  Eviction must participate in the checkpoint so
 * that INs cascade up properly; see coordinateEvictionWithCheckpoint.
 *
 * When INs are latched along with synchronization on a DirtyINMap, the order
 * must be: 1) IN latches and 2) synchronize on DirtyINMap.  For example,
 * the evictor latches the parent and child IN before calling the synchronized
 * method coordinateEvictionWithCheckpoint, and selectDirtyINsForCheckpoint
 * latches the IN before calling the synchronized method selectForCheckpoint.
 */
class DirtyINMap {

    private final EnvironmentImpl envImpl;
    private final SortedMap<Integer, Map<Long, CheckpointReference>> levelMap;
    private int numEntries;
    private final Set<DatabaseId> mapLNsToFlush;
    private final Map<DatabaseImpl, Integer> highestFlushLevels;

    enum CkptState {
        /** No checkpoint in progress, or is used for Database.sync. */
        NONE,
        /** Checkpoint started but dirty map is not yet complete. */
        DIRTY_MAP_INCOMPLETE,
        /** Checkpoint in progress and dirty map is complete. */
        DIRTY_MAP_COMPLETE,
    };

    private CkptState ckptState;
    private boolean ckptFlushAll;
    private boolean ckptFlushExtraLevel;

    DirtyINMap(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
        levelMap = new TreeMap<Integer, Map<Long, CheckpointReference>>();
        numEntries = 0;
        mapLNsToFlush = new HashSet<DatabaseId>();
        highestFlushLevels = new IdentityHashMap<DatabaseImpl, Integer>();
        ckptState = CkptState.NONE;
    }

    /**
     * Coordinates an eviction with an in-progress checkpoint and returns
     * whether or not provisional logging is needed.
     *
     * @return true if the target must be logged provisionally.
     */
    synchronized boolean coordinateEvictionWithCheckpoint(IN target,
                                                          IN parent) {
        final DatabaseImpl db = target.getDatabase();

        /*
         * If the checkpoint is in-progress and has not finished dirty map
         * construction, we must add the parent to the dirty map.  That way the
         * dirtiness and logging will cascade up in the same way as if the
         * target were not evicted, and instead were encountered during dirty
         * map construction.  We don't want the evictor's actions to introduce
         * an IN in the log that has not cascaded up properly.
         *
         * Note that we add the parent even if it is not dirty here.  It will
         * become dirty after the target child is logged, but that hasn't
         * happened yet.
         *
         * We do not add the parent if it is null, which is the case when the
         * root is being evicted.
         */
        if (ckptState == CkptState.DIRTY_MAP_INCOMPLETE &&
            parent != null) {
            /* Add latched parent IN to dirty map. */
            selectForCheckpoint(parent);
            /* Save dirty/temp DBs for later. */
            saveMapLNsToFlush(parent);
        }

        /*
         * The evictor has to log provisionally in three cases:
         *
         * 1 - The eviction target is part of a deferred write database.
         */
        if (db.isDeferredWriteMode()) {
            return true;
        }

        /*
         * 2 - The checkpoint is in-progress and has not finished dirty map
         *     construction, and the target is not the root. The parent IN has
         *     been added to the dirty map, so we know the child IN is at a
         *     level below the max flush level.
         */
        if (ckptState == CkptState.DIRTY_MAP_INCOMPLETE &&
            parent != null) {
            return true;
        }

        /*
         * 3 - The checkpoint is in-progress and has finished dirty map
         *     construction, and is at a level above the eviction target.
         */
        if (ckptState == CkptState.DIRTY_MAP_COMPLETE &&
            target.getLevel() < getHighestFlushLevel(db)) {
            return true;
        }

        /* Otherwise, log non-provisionally. */
        return false;
    }

    /**
     * Must be called before starting a checkpoint, and must not be called for
     * Database.sync.  Updates memory budget and sets checkpoint state.
     */
    synchronized void beginCheckpoint(boolean flushAll,
                                      boolean flushExtraLevel) {
        assert levelMap.isEmpty();
        assert mapLNsToFlush.isEmpty();
        assert highestFlushLevels.isEmpty();
        assert numEntries == 0;
        assert ckptState == CkptState.NONE;
        ckptState = CkptState.DIRTY_MAP_INCOMPLETE;
        ckptFlushAll = flushAll;
        ckptFlushExtraLevel = flushExtraLevel;
    }

    /**
     * Must be called after a checkpoint or Database.sync is complete.  Updates
     * memory budget and clears checkpoint state.
     */
    synchronized void reset() {
        removeCostFromMemoryBudget();
        levelMap.clear();
        mapLNsToFlush.clear();
        highestFlushLevels.clear();
        numEntries = 0;
        ckptState = CkptState.NONE;
    }

    /**
     * Scan the INList for all dirty INs, excluding temp DB INs.  Save them in
     * a tree-level ordered map for level ordered flushing.
     *
     * Take this opportunity to recalculate the memory budget tree usage.
     *
     * This method itself is not synchronized to allow concurrent eviction.
     * Synchronization is performed on a per-IN basis to protect the data
     * structures here, and eviction can occur in between INs.
     */
    void selectDirtyINsForCheckpoint()
        throws DatabaseException {

        assert ckptState == CkptState.DIRTY_MAP_INCOMPLETE;

        /*
         * Opportunistically recalculate the INList memory budget while
         * traversing the entire INList.
         */
        final INList inMemINs = envImpl.getInMemoryINs();
        inMemINs.memRecalcBegin();
        boolean completed = false;
        try {
            for (IN in : inMemINs) {
                in.latchShared(CacheMode.UNCHANGED);
                try {
                    inMemINs.memRecalcIterate(in);
                    /* Add latched IN to dirty map. */
                    if (in.getDirty()) {
                        selectForCheckpoint(in);
                    }
                    /* Save dirty/temp DBs for later. */
                    saveMapLNsToFlush(in);
                } finally {
                    in.releaseLatch();
                }

                /* Call test hook after releasing latch. */
                TestHookExecute.doHookIfSet
                    (Checkpointer.examineINForCheckpointHook, in);
            }
            completed = true;
        } finally {
            inMemINs.memRecalcEnd(completed);
        }

        /*
         * Finish filling out the highestFlushLevels map. For each entry in
         * highestFlushLevels that has a null level Integer value (set by
         * selectForCheckpoint), we call DbTree.getHighestLevel and replace the
         * null level. We must call DbTree.getHighestLevel, which latches the
         * root, only when not synchronized, to avoid breaking the
         * synchronization rules described in the class comment.  This must be
         * done in several steps to follow the sychronization rules, yet
         * protect the highestFlushLevels using synchronization.
         */
        final Map<DatabaseImpl, Integer> maxFlushDbs =
            new HashMap<DatabaseImpl, Integer>();

        /* Copy entries with a null level. */
        synchronized (this) {
            for (DatabaseImpl db : highestFlushLevels.keySet()) {
                if (highestFlushLevels.get(db) == null) {
                    maxFlushDbs.put(db, null);
                }
            }
        }

        /* Call getHighestLevel without synchronization. */
        final DbTree dbTree = envImpl.getDbTree();
        for (Map.Entry<DatabaseImpl, Integer> entry : maxFlushDbs.entrySet()) {
            entry.setValue(dbTree.getHighestLevel(entry.getKey()));
        }

        /* Fill in levels in highestFlushLevels. */
        synchronized (this) {
            for (Map.Entry<DatabaseImpl, Integer> entry :
                 maxFlushDbs.entrySet()) {
                highestFlushLevels.put(entry.getKey(), entry.getValue());
            }
        }

        /* Complete this phase of the checkpoint. */
        synchronized (this) {
            addCostToMemoryBudget();
            ckptState = CkptState.DIRTY_MAP_COMPLETE;
        }
    }

    /**
     * Adds the IN to the dirty map unless it belongs to a temp DB, whether or
     * not the IN is dirty.  Also updates the highest flush level map.
     */
    private synchronized void selectForCheckpoint(IN in) {
        /* Do not checkpoint temporary databases. */
        final DatabaseImpl db = in.getDatabase();
        if (db.isTemporary()) {
            return;
        }

        Integer level = addIN(in, false /*updateMemoryBudget*/);

        /*
         * IN was added to the dirty map.  Update the highest level seen
         * for the database.  Use one level higher when ckptFlushExtraLevel
         * is set.  When ckptFlushAll is set, use the maximum level for the
         * database.  Durable deferred-write databases must be synced, so
         * also use the maximum level.
         *
         * Always flush at least one level above the bottom-most BIN level so
         * that the BIN level is logged provisionally and the expense of
         * processing BINs during recovery is avoided.
         */
        if (ckptFlushAll || db.isDurableDeferredWrite()) {
            if (!highestFlushLevels.containsKey(db)) {

                /*
                 * Null is used as an indicator that getHighestLevel should be
                 * called in selectDirtyINsForCheckpoint, when not
                 * synchronized.
                 */
                highestFlushLevels.put(db, null);
            }
        } else {
            int levelVal = level.intValue();
            if (ckptFlushExtraLevel || in.isBIN()) {
                if (in.isRoot()) {
                    /* No reason to go above DB root. */
                } else {
                    /* Next level up in the same tree. */
                    levelVal += 1;
                    level = levelVal;
                }
            }
            final Integer highestLevelSeen = highestFlushLevels.get(db);
            if (highestLevelSeen == null ||
                levelVal > highestLevelSeen.intValue()) {
                highestFlushLevels.put(db, level);
            }
        }
    }

    /**
     * Scan the INList for all dirty INs for a given database.  Arrange them in
     * level sorted map for level ordered flushing.
     *
     * This method is not synchronized to allow concurrent eviction.
     * Coordination between eviction and Database.sync is not required.
     */
    void selectDirtyINsForDbSync(DatabaseImpl dbImpl)
        throws DatabaseException {

        assert ckptState == CkptState.NONE;

        final DatabaseId dbId = dbImpl.getId();

        for (IN in : envImpl.getInMemoryINs()) {
            if (in.getDatabaseId().equals(dbId)) {
                in.latch(CacheMode.UNCHANGED);
                try {
                    if (in.getDirty()) {
                        addIN(in, false /*updateMemoryBudget*/);
                    }
                } finally {
                    in.releaseLatch();
                }
            }
        }

        /*
         * Create a single entry map that forces all levels of this DB to
         * be flushed.
         */
        highestFlushLevels.put
            (dbImpl,
             Integer.valueOf(envImpl.getDbTree().getHighestLevel(dbImpl)));

        /* Add the dirty map to the memory budget.  */
        addCostToMemoryBudget();
    }

    synchronized int getHighestFlushLevel(DatabaseImpl db) {

        assert ckptState != CkptState.DIRTY_MAP_INCOMPLETE;

        /*
         * This method is only called while flushing dirty nodes for a
         * checkpoint or Database.sync, not for an eviction, so an entry for
         * this database should normally exist.  However, if the DB root (and
         * DatabaseImpl) have been evicted since the highestFlushLevels was
         * constructed, the new DatabaseImpl instance will not be present in
         * the map.  In this case, we do not need to checkpoint the IN and
         * eviction should be non-provisional.
         */
        Integer val = highestFlushLevels.get(db);
        return (val != null) ? val.intValue() : IN.MIN_LEVEL;
    }

    synchronized int getNumLevels() {
        return levelMap.size();
    }

    private synchronized void addCostToMemoryBudget() {
        final MemoryBudget mb = envImpl.getMemoryBudget();
        final int cost = numEntries * MemoryBudget.CHECKPOINT_REFERENCE_SIZE;
        mb.updateAdminMemoryUsage(cost);
    }

    private synchronized void removeCostFromMemoryBudget() {
        final MemoryBudget mb = envImpl.getMemoryBudget();
        final int cost = numEntries * MemoryBudget.CHECKPOINT_REFERENCE_SIZE;
        mb.updateAdminMemoryUsage(0 - cost);
    }

    /**
     * Add a node unconditionally to the dirty map. The dirty map is keyed by
     * level (Integers) and holds sets of IN references.
     *
     * @param updateMemoryBudget if true then update the memory budget as the
     * map is changed; if false then addCostToMemoryBudget must be called
     * later.
     *
     * @return level of IN added to the dirty map.  The level is returned
     * rather than a boolean simply to avoid allocating another Integer in the
     * caller.
     */
    synchronized Integer addIN(IN in, boolean updateMemoryBudget) {

        final Integer level = Integer.valueOf(in.getLevel());
        final Map<Long, CheckpointReference> nodeMap;
        if (levelMap.containsKey(level)) {
            nodeMap = levelMap.get(level);
        } else {
            nodeMap = new TreeMap<Long, CheckpointReference>();
            levelMap.put(level, nodeMap);
        }

        nodeMap.put(in.getNodeId(),
                    new CheckpointReference(in.getDatabase().getId(),
                                            in.getNodeId(),
                                            in.isDbRoot(),
                                            in.getIdentifierKey()));
        numEntries++;

        if (updateMemoryBudget) {
            final MemoryBudget mb = envImpl.getMemoryBudget();
            mb.updateAdminMemoryUsage
                (MemoryBudget.CHECKPOINT_REFERENCE_SIZE);
        }

        return level;
    }

    /**
     * Get the lowest level currently stored in the map.
     */
    synchronized Integer getLowestLevelSet() {
        return levelMap.firstKey();
    }

    /**
     * Removes the set corresponding to the given level.
     */
    synchronized void removeLevel(Integer level) {
        levelMap.remove(level);
    }

    synchronized boolean containsNode(Integer level, Long nodeId) {
        final Map<Long, CheckpointReference> nodeMap = levelMap.get(level);
        if (nodeMap != null) {
            return nodeMap.containsKey(nodeId);
        }
        return false;
    }

    synchronized CheckpointReference removeNode(Integer level, Long nodeId) {
        final Map<Long, CheckpointReference> nodeMap = levelMap.get(level);
        if (nodeMap != null) {
            return nodeMap.remove(nodeId);
        }
        return null;
    }

    synchronized CheckpointReference removeNextNode(Integer level) {
        final Map<Long, CheckpointReference> nodeMap = levelMap.get(level);
        if (nodeMap != null) {
            final Iterator<Map.Entry<Long, CheckpointReference>> iter =
                nodeMap.entrySet().iterator();
            if (iter.hasNext()) {
                final CheckpointReference ref = iter.next().getValue();
                iter.remove();
                return ref;
            }
        }
        return null;
    }

    /**
     * If the given IN is a BIN for the ID mapping database, saves all
     * dirty/temp MapLNs contained in it.
     */
    private synchronized void saveMapLNsToFlush(IN in) {
        if (in instanceof BIN &&
            in.getDatabase().getId().equals(DbTree.ID_DB_ID)) {
            for (int i = 0; i < in.getNEntries(); i += 1) {
                final MapLN ln = (MapLN) in.getTarget(i);
                if (ln != null && ln.getDatabase().isCheckpointNeeded()) {
                    mapLNsToFlush.add(ln.getDatabase().getId());
                }
            }
        }
    }

    /**
     * Flushes all saved dirty/temp MapLNs and clears the saved set.
     *
     * <p>If dirty, a MapLN must be flushed at each checkpoint to record
     * updated utilization info in the checkpoint interval.  If it is a
     * temporary DB, the MapLN must be flushed because all temp DBs must be
     * encountered by recovery so they can be removed if they were not closed
     * (and removed) by the user.</p>
     *
     * This method is not synchronized because it takes the Btree root latch,
     * and we must never latch something in the Btree after synchronizing on
     * DirtyINMap; see class comments.  Special synchronization is performed
     * for accessing internal state; see below.
     *
     * @param checkpointStart start LSN of the checkpoint in progress.  To
     * reduce unnecessary logging, the MapLN is only flushed if it has not been
     * written since that LSN.
     */
    void flushMapLNs(long checkpointStart)
        throws DatabaseException {

        /*
         * This method is called only while flushing dirty nodes for a
         * checkpoint or Database.sync, not for an eviction, and mapLNsToFlush
         * is not changed during the flushing phase.  So we don't strictly need
         * to synchronize while accessing mapLNsToFlush.  However, for
         * consistency and extra safety we always synchronize while accessing
         * internal state.
         */
        final Set<DatabaseId> mapLNsCopy;
        synchronized (this) {
            assert ckptState != CkptState.DIRTY_MAP_INCOMPLETE;
            if (mapLNsToFlush.isEmpty()) {
                mapLNsCopy = null;
            } else {
                mapLNsCopy = new HashSet<DatabaseId>(mapLNsToFlush);
                mapLNsToFlush.clear();
            }
        }
        if (mapLNsCopy != null) {
            final DbTree dbTree = envImpl.getDbTree();
            for (DatabaseId dbId : mapLNsCopy) {
                final DatabaseImpl db = dbTree.getDb(dbId);
                try {
                    if (db != null &&
                        !db.isDeleted() &&
                        db.isCheckpointNeeded()) {
                        dbTree.modifyDbRoot
                            (db, checkpointStart /*ifBeforeLsn*/,
                             true /*mustExist*/);
                    }
                } finally {
                    dbTree.releaseDb(db);
                }
            }
        }
    }

    /**
     * Flushes the DB mapping tree root at the end of the checkpoint, if either
     * mapping DB is dirty and the root was not flushed previously during the
     * checkpoint.
     *
     * This method is not synchronized because it does not access internal
     * state.  Also, it takes the DbTree root latch and although this latch
     * should never be held by eviction, for consistency we should not latch
     * something related to the Btree after synchronizing on DirtyINMap; see
     * class comments.
     *
     * @param checkpointStart start LSN of the checkpoint in progress.  To
     * reduce unnecessary logging, the Root is only flushed if it has not been
     * written since that LSN.
     */
    void flushRoot(long checkpointStart)
        throws DatabaseException {

        final DbTree dbTree = envImpl.getDbTree();
        if (dbTree.getDb(DbTree.ID_DB_ID).isCheckpointNeeded() ||
            dbTree.getDb(DbTree.NAME_DB_ID).isCheckpointNeeded()) {
            envImpl.logMapTreeRoot(checkpointStart);
        }
    }

    synchronized int getNumEntries() {
        return numEntries;
    }
}
