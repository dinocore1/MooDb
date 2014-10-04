/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.incomp;

import static com.sleepycat.je.incomp.INCompStatDefinition.GROUP_DESC;
import static com.sleepycat.je.incomp.INCompStatDefinition.GROUP_NAME;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_CURSORS_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_DBCLOSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_NON_EMPTY_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_PROCESSED_BINS;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_QUEUE_SIZE;
import static com.sleepycat.je.incomp.INCompStatDefinition.INCOMP_SPLIT_BINS;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.tree.BIN;
import com.sleepycat.je.tree.BINReference;
import com.sleepycat.je.tree.CursorsExistException;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.NodeNotEmptyException;
import com.sleepycat.je.tree.Tree;
import com.sleepycat.je.tree.Tree.SearchType;
import com.sleepycat.je.utilint.DaemonThread;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.LongStat;
import com.sleepycat.je.utilint.StatGroup;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * JE compression consists of removing deleted entries from BINs, and pruning
 * empty IN/BINs from the tree which is also called a reverse split.
 *
 * One of the reasons compression is treated specially is that slot compression
 * cannot be performed inline as part of a delete operation.  When we delete an
 * LN, a cursor is always present on the LN.  The API dictates that the cursor
 * will remain positioned on the deleted record.  In addition, if the deleting
 * transaction aborts we must restore the slot and the possibility of a split
 * during an abort is something we wish to avoid; for this reason, compression
 * will not occur if the slot's LSN is locked.  In principle, slot compression
 * could be performed during transaction commit, but that would be expensive
 * because a Btree lookup would be required, and this would negatively impact
 * operation latency.  For all these reasons, slot compression is performed
 * after the delete operation is complete and committed, and not in the thread
 * performing the operation or transaction commit.
 *
 * Compression is of two types:
 *
 * + "Queued compression" is carried out by the INCompressor daemon thread.
 *    Both slot compression and pruning are performed.
 *
 * + "Lazy compression" is carried out as part of logging a BIN by certain
 *   operations (namely checkpointing and eviction).  Only slot compression is
 *   performed by lazy compression, not pruning.
 *
 * The use of BINDeltas has a big impact on slot compression because slots
 * cannot be compressed until we know that a full BIN will next be logged.  If
 * a slot were compressed prior to logging a BINDelta, the record of the
 * compression would be lost and the slot would "reappear" when the BIN is
 * reconstituted; therefore, this is not permitted.
 *
 * Queued compression prior to logging a BINDelta is also wasteful because the
 * dequeued entry cannot be processed.  Therefore, lazy compression is used
 * when a BINDelta will next be logged, and queued compression is used only
 * when a full BIN will next be logged.  Because, in general, BINDeltas are
 * logged more often than BINs, lazy compression is used for slot compression
 * more often than queued compression.
 *
 * You may wonder, since lazy compression is used most of the time for slot
 * compression, why use queued compression for slot compression at all? Queued
 * compression is useful for slot compression for the following reasons:
 *
 * + When a cursor is on a BIN, queuing has an advantage over lazy compression.
 *   If we can't compress during logging because of a cursor, we have to log
 *   anyway, and we must delay compression and log again later.  If we can't
 *   compress when processing a queue entry, we requeue it and retry later,
 *   which increases the chances that we will be able to compress before
 *   logging.
 *
 * + The code to process a queue entry must do slot compression anyway, even if
 *   we only want to prune the BIN.  We have to account for the case where all
 *   slots are deleted but not yet compressed.  So the code to process the
 *   queue entry could not be simplified even if we were to decide not to queue
 *   entries for slot compression. 
 *
 * + Because BINDeltas are not used for DeferredWrite mode, queued compression
 *   is much more appropriate and efficient in this mode.
 *
 * The mainstream algorithm for compression is as follows.
 * 
 * 1. When a delete operation occurs (CursorImpl.delete) we call
 *    Locker.addDeleteInfo, which determines whether a BINDelta will next be
 *    logged (BIN.shouldLogDelta).  If so, it does nothing, meaning that lazy
 *    compression will be used.  If not (a full BIN will next be logged), it
 *    adds a BINReference to the Locker.deleteInfo map.
 *
 * 2. When the operation is successful and the Locker releases its locks, it
 *    copies the BINReferences from the deleteInfo map to the compressor queue.
 *    For a transaction this happens at commit (Txn.commit).  For a
 *    non-transaction locker this happens when the cursor moves or is closed
 *    (BasicLocker.releaseNonTxnLocks).
 *
 * 3. The INCompressor thread processes its queue entries periodically, based
 *    on EnvironmentConfig.COMPRESSOR_WAKEUP_INTERVAL.  For each BINReference
 *    that was queued, it tries to compress all deleted slots (BIN.compress).
 *    If the BIN is then empty, it prunes the Btree (Tree.delete).  If a slot
 *    cannot be compressed or an empty BIN cannot be pruned because a slot's
 *    LSN is locked or a cursor is present, the entry is requeued for retry.
 *
 * 4. Lazy compression occurs via the checkpointer and evictor.  When logging
 *    an IN, these components pass true for the allowCompress parameter.  If a
 *    full BIN is logged (BIN.shouldLogDelta returns false), the lazyCompress
 *    method is called by BIN.beforeLog.  lazyCompress will attempt to compress
 *    all deleted slots (BIN.compress).  If the BIN is then empty, it will
 *    queue a BINReference so that pruning will occur later.  If a slot cannot
 *    be compressed (because the LSN is locked or a cursor is present), the
 *    BIN.afterLog method will queue a BINReference.  In this last case, two
 *    full BINs will be logged consecutively.
 *
 * Special cases are as follows.
 *
 * A. Before performing a split, we call lazyCompress in order to avoid the
 *    split if possible (Tree.searchSubTreeUntilSplit).  It is important to
 *    avoid splitting when compression is deferred due to BINDeltas.
 *
 * B. When we undo an LN insertion (via abort, rollback or recovery undo in
 *    RecoveryManager.undo), or redo an LN deletion during recovery
 *    (RecoveryManager.redo), we queue a BINReference if a full BIN will next
 *    be logged (BIN.queueSlotDeletion).  This mimics what happens during a
 *    mainstream delete operation.
 */
public class INCompressor extends DaemonThread {
    private static final boolean DEBUG = false;

    private EnvironmentImpl env;
    private final long lockTimeout;

    /* stats */
    private StatGroup stats;
    private LongStat splitBins;
    private LongStat dbClosedBins;
    private LongStat cursorsBins;
    private LongStat nonEmptyBins;
    private LongStat processedBins;
    private LongStat compQueueSize;

    /* per-run stats */
    private int splitBinsThisRun = 0;
    private int dbClosedBinsThisRun = 0;
    private int cursorsBinsThisRun = 0;
    private int nonEmptyBinsThisRun = 0;
    private int processedBinsThisRun = 0;

    /*
     * The following stats are not kept per run, because they're set by
     * multiple threads doing lazy compression. They are debugging aids; it
     * didn't seem like a good idea to add synchronization to the general path.
     */
    private int lazyProcessed = 0;
    private int wokenUp = 0;

    /*
     * Store logical references to BINs that have deleted entries and are
     * candidates for compaction.
     */
    private Map<Long, BINReference> binRefQueue;
    private final Object binRefQueueSync;

    /* For unit tests */
    private TestHook beforeFlushTrackerHook; // [#15528]

    public INCompressor(EnvironmentImpl env, long waitTime, String name) {
        super(waitTime, name, env);
        this.env = env;
        lockTimeout = env.getConfigManager().getDuration
            (EnvironmentParams.COMPRESSOR_LOCK_TIMEOUT);
        binRefQueue = new HashMap<Long, BINReference>();
        binRefQueueSync = new Object();
 
        /* Do the stats definitions. */
        stats = new StatGroup(GROUP_NAME, GROUP_DESC);
        splitBins = new LongStat(stats, INCOMP_SPLIT_BINS);
        dbClosedBins = new LongStat(stats, INCOMP_DBCLOSED_BINS);
        cursorsBins = new LongStat(stats, INCOMP_CURSORS_BINS);
        nonEmptyBins = new LongStat(stats, INCOMP_NON_EMPTY_BINS);
        processedBins = new LongStat(stats, INCOMP_PROCESSED_BINS);
        compQueueSize = new LongStat(stats, INCOMP_QUEUE_SIZE);
    }

    synchronized public void clearEnv() {
        env = null;
    }

    /* For unit testing only. */
    public void setBeforeFlushTrackerHook(TestHook hook) {
        beforeFlushTrackerHook = hook;
    }

    public synchronized void verifyCursors()
        throws DatabaseException {

        /*
         * Environment may have been closed.  If so, then our job here is done.
         */
        if (env.isClosed()) {
            return;
        }

        /*
         * Use a snapshot to verify the cursors.  This way we don't have to
         * hold a latch while verify takes locks.
         */
        List<BINReference> queueSnapshot = null;
        synchronized (binRefQueueSync) {
            queueSnapshot = new ArrayList<BINReference>(binRefQueue.values());
        }

        /*
         * Use local caching to reduce DbTree.getDb overhead.  Do not call
         * releaseDb after each getDb, since the entire dbCache will be
         * released at the end.
         */
        DbTree dbTree = env.getDbTree();
        Map<DatabaseId, DatabaseImpl> dbCache =
            new HashMap<DatabaseId, DatabaseImpl>();
        try {
            Iterator<BINReference> it = queueSnapshot.iterator();
            while (it.hasNext()) {
                BINReference binRef = it.next();
                DatabaseImpl db = dbTree.getDb
                    (binRef.getDatabaseId(), lockTimeout, dbCache);
                BIN bin = searchForBIN(db, binRef);
                if (bin != null) {
                    bin.verifyCursors();
                    bin.releaseLatch();
                }
            }
        } finally {
            dbTree.releaseDbs(dbCache);
        }
    }

    public int getBinRefQueueSize() {
        int size = 0;
        synchronized (binRefQueueSync) {
            size = binRefQueue.size();
        }

        return size;
    }

    /*
     * There are multiple flavors of the addBin*ToQueue methods. All allow
     * the caller to specify whether the daemon should be notified. Currently
     * no callers proactively notify, and we rely on lazy compression and
     * the daemon timebased wakeup to process the queue.
     */

    /**
     * Adds the BIN to the queue if the BIN is not already in the queue.
     */
    public void addBinToQueue(BIN bin, boolean doWakeup) {
        synchronized (binRefQueueSync) {
            addBinToQueueAlreadyLatched(bin);
        }
        if (doWakeup) {
            wakeup();
        }
    }

    /**
     * Adds the BINReference to the queue if the BIN is not already in the
     * queue, or adds the deleted keys to an existing entry if one exists.
     */
    public void addBinRefToQueue(BINReference binRef, boolean doWakeup) {
        synchronized (binRefQueueSync) {
            addBinRefToQueueAlreadyLatched(binRef);
        }

        if (doWakeup) {
            wakeup();
        }
    }

    /**
     * Adds an entire collection of BINReferences to the queue at once.  Use
     * this to avoid latching for each add.
     */
    public void addMultipleBinRefsToQueue(Collection<BINReference> binRefs,
                                          boolean doWakeup) {
        synchronized (binRefQueueSync) {
            Iterator<BINReference> it = binRefs.iterator();
            while (it.hasNext()) {
                BINReference binRef = it.next();
                addBinRefToQueueAlreadyLatched(binRef);
            }
        }

        if (doWakeup) {
            wakeup();
        }
    }

    /**
     * Adds the BINReference with the latch held.
     */
    private void addBinRefToQueueAlreadyLatched(BINReference binRef) {
        final Long node = Long.valueOf(binRef.getNodeId());
        if (binRefQueue.containsKey(node)) {
            return;
        }
        binRefQueue.put(node, binRef);
    }

    /**
     * Adds the BIN with the latch held.
     */
    private void addBinToQueueAlreadyLatched(BIN bin) {
        final Long node = Long.valueOf(bin.getNodeId());
        if (binRefQueue.containsKey(node)) {
            return;
        }
        binRefQueue.put(node, bin.createReference());
    }

    public boolean exists(long nodeId) {
        synchronized (binRefQueueSync) {
            return binRefQueue.containsKey(nodeId);
        }
    }

    /**
     * Return stats
     */
    public StatGroup loadStats(StatsConfig config) {
        compQueueSize.set((long) getBinRefQueueSize());

        if (DEBUG) {
            System.out.println("lazyProcessed = " + lazyProcessed);
            System.out.println("wokenUp=" + wokenUp);
        }

        if (config.getClear()) {
            lazyProcessed = 0;
            wokenUp = 0;
        }

        return stats.cloneGroup(config.getClear());
    }

    /**
     * Return the number of retries when a deadlock exception occurs.
     */
    @Override
    protected long nDeadlockRetries() {
        return env.getConfigManager().getInt
            (EnvironmentParams.COMPRESSOR_RETRY);
    }

    @Override
    public synchronized void onWakeup()
        throws DatabaseException {

        if (env.isClosed()) {
            return;
        }
        wokenUp++;
        doCompress();
    }

    /**
     * The real work to doing a compress. This may be called by the compressor
     * thread or programatically.
     */
    public synchronized void doCompress()
        throws DatabaseException {

        /*
         * Make a snapshot of the current work queue so the compressor thread
         * can safely iterate over the queue. Note that this impacts lazy
         * compression, because it lazy compressors will not see BINReferences
         * that have been moved to the snapshot.
         */
        Map<Long, BINReference> queueSnapshot = null;
        int binQueueSize = 0;
        synchronized (binRefQueueSync) {
            binQueueSize = binRefQueue.size();
            if (binQueueSize > 0) {
                queueSnapshot = binRefQueue;
                binRefQueue = new HashMap<Long, BINReference>();
            }
        }

        /* There is work to be done. */
        if (binQueueSize > 0) {
            resetPerRunCounters();
            LoggerUtils.fine(logger, envImpl, 
                             "InCompress.doCompress called, queue size: " +
                             binQueueSize);
            assert LatchSupport.countLatchesHeld() == 0;

            /*
             * Compressed entries must be counted as obsoleted.  A separate
             * tracker is used to accumulate tracked obsolete info so it can be
             * added in a single call under the log write latch.  We log the
             * info for deleted subtrees immediately because we don't process
             * deleted IN entries during recovery; this reduces the chance of
             * lost info.
             */
            LocalUtilizationTracker localTracker =
                new LocalUtilizationTracker(env);

            /* Use local caching to reduce DbTree.getDb overhead. */
            Map<DatabaseId, DatabaseImpl> dbCache =
                new HashMap<DatabaseId, DatabaseImpl>();

            DbTree dbTree = env.getDbTree();
            BINSearch binSearch = new BINSearch();
            try {
                Iterator<BINReference> it = queueSnapshot.values().iterator();
                while (it.hasNext()) {
                    if (env.isClosed()) {
                        return;
                    }

                    BINReference binRef = it.next();
                    if (!findDBAndBIN(binSearch, binRef, dbTree, dbCache)) {

                        /*
                         * Either the db is closed, or the BIN doesn't exist.
                         * Don't process this BINReference.
                         */
                        continue;
                    }

                    /* Compress deleted slots and prune if possible. */
                    compressBin(binSearch.db, binSearch.bin, binRef,
                                localTracker);
                }

                /* SR [#11144]*/
                assert TestHookExecute.doHookIfSet(beforeFlushTrackerHook);

                /*
                 * Count obsolete nodes and write out modified file summaries
                 * for recovery.  All latches must have been released.
                 */
                env.getUtilizationProfile().flushLocalTracker(localTracker);

            } finally {
                dbTree.releaseDbs(dbCache);
                assert LatchSupport.countLatchesHeld() == 0;
                accumulatePerRunCounters();
            }
        }
    }

    /**
     * Compresses a single BIN and then deletes the BIN if it is empty.
     *
     * @param bin is latched when this method is called, and unlatched when it
     * returns.
     */
    private void compressBin(DatabaseImpl db,
                             BIN bin,
                             BINReference binRef,
                             LocalUtilizationTracker localTracker) {

        /* Safe to get identifier keys; bin is latched. */
        final byte[] idKey = bin.getIdentifierKey();
        boolean empty = (bin.getNEntries() == 0);

        try {
            if (!empty) {

                /*
                 * If a delta will be logged, do not compress, check for
                 * emptiness or re-add the entry to the queue.
                 *
                 * We strive not to add a slot to the queue when we will log a
                 * delta.  However, it is possible that an entry is added, or
                 * that an entry is not cleared by lazy compression prior to
                 * logging a full BIN.  Clean-up for such queue entries is
                 * here.
                 */
                if (bin.shouldLogDelta()) {
                    return;
                }

                /* If there are cursors on the BIN, requeue and try later. */
                if (bin.nCursors() > 0) {
                    addBinRefToQueue(binRef, false);
                    cursorsBinsThisRun++;
                    return;
                }

                /* If compression is incomplete, requeue and try later. */
                if (!bin.compress(localTracker)) {
                    addBinRefToQueue(binRef, false);
                    return;
                }

                /* After compression the BIN may be empty. */
                empty = (bin.getNEntries() == 0);
            }
        } finally {
            bin.releaseLatch();
        }

        /* After releasing the latch, prune the BIN if it is empty. */
        if (empty) {
            pruneBIN(db, binRef, idKey, localTracker);
        }
    }

    /**
     * If the target BIN is empty, attempt to remove the empty branch of the
     * tree.
     */
    private void pruneBIN(DatabaseImpl dbImpl,
                          BINReference binRef,
                          byte[] idKey,
                          LocalUtilizationTracker localTracker) {

        try {
            Tree tree = dbImpl.getTree();
            tree.delete(idKey, localTracker);
            processedBinsThisRun++;
        } catch (NodeNotEmptyException NNEE) {

            /*
             * Something was added to the node since the point when the
             * deletion occurred; we can't prune, and we can throw away this
             * BINReference.
             */
             nonEmptyBinsThisRun++;
        } catch (CursorsExistException e) {
            /* If there are cursors in the way of the delete, retry later. */
            addBinRefToQueue(binRef, false);
            cursorsBinsThisRun++;
        }
    }

    /**
     * Search the tree for the BIN that corresponds to this BINReference.
     *
     * @param binRef the BINReference that indicates the bin we want.
     *
     * @return the BIN that corresponds to this BINReference. The
     * node is latched upon return. Returns null if the BIN can't be found.
     */
    public BIN searchForBIN(DatabaseImpl db, BINReference binRef) {
        return (BIN) db.getTree().search
            (binRef.getKey(), SearchType.NORMAL, null, CacheMode.UNCHANGED,
             null /*keyComparator*/);
    }

    /**
     * Reset per-run counters.
     */
    private void resetPerRunCounters() {
        splitBinsThisRun = 0;
        dbClosedBinsThisRun = 0;
        cursorsBinsThisRun = 0;
        nonEmptyBinsThisRun = 0;
        processedBinsThisRun = 0;
    }

    private void accumulatePerRunCounters() {
        splitBins.add(splitBinsThisRun);
        dbClosedBins.add(dbClosedBinsThisRun);
        cursorsBins.add(cursorsBinsThisRun);
        nonEmptyBins.add(nonEmptyBinsThisRun);
        processedBins.add(processedBinsThisRun);
    }

    /**
     * Lazily compress prior to logging a full version of a BIN; the caller 
     * is responsible for ensuring that a full version is likely to be logged
     * next. Do not do any pruning. The target IN should be latched when we
     * enter, and it will be remain latched.
     *
     * When an LN is deleted and a delta will be logged next (see
     * BIN.shouldLogDelta), we do not add the slot to the compressor queue
     * because compression must be deferred until the full version is logged.
     * Therefore we cannot rely on the compressor to delete all slots and we do
     * the final deferred compression here.
     *
     * Note that we do not bother to delete queue entries for the BIN if
     * compression succeeds.  Queue entries are normally removed quickly by the
     * compressor.  In the case where queue entries happen to exist when we do
     * the final compression below, we rely on the compressor to clean them up
     * later on when they are processed.
     */
    public void lazyCompress(IN in) {

        /* Only BINs are compressible. */
        if (!in.isCompressible()) {
            return;
        }
        final BIN bin = (BIN) in;
        assert bin.isLatchOwnerForWrite();

        /* Cursors prohibit compression. */
        if (bin.nCursors() > 0) {
            return;
        }

        /* Compress. Then if empty, queue for pruning. */
        if (bin.compress(null /*localTracker*/)) {
            if (bin.getNEntries() == 0) {
                addBinToQueue(bin, false);
            }
        }

        lazyProcessed++;
    }

    /*
     * Find the db and bin for a BINReference.
     * @return true if the db is open and the target bin is found.
     */
    private boolean findDBAndBIN(BINSearch binSearch,
                                 BINReference binRef,
                                 DbTree dbTree,
                                 Map<DatabaseId, DatabaseImpl> dbCache)
        throws DatabaseException {

        /*
         * Find the database.  Do not call releaseDb after this getDb, since
         * the entire dbCache will be released later.
         */
        binSearch.db = dbTree.getDb
            (binRef.getDatabaseId(), lockTimeout, dbCache);
        if ((binSearch.db == null) ||(binSearch.db.isDeleted())) {
          /* The db was deleted. Ignore this BIN Ref. */
            dbClosedBinsThisRun++;
            return false;
        }

        /* Perform eviction before each operation. */
        env.daemonEviction(true /*backgroundIO*/);

        /* Find the BIN. */
        binSearch.bin = searchForBIN(binSearch.db, binRef);
        if ((binSearch.bin == null) ||
            binSearch.bin.getNodeId() != binRef.getNodeId()) {
            /* The BIN may have been split. */
            if (binSearch.bin != null) {
                binSearch.bin.releaseLatch();
            }
            splitBinsThisRun++;
            return false;
        }

        return true;
    }

    /* Struct to return multiple values from findDBAndBIN. */
    private static class BINSearch {
        public DatabaseImpl db;
        public BIN bin;
    }
}
