/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.RecoveryUtilizationTracker;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.INContainingEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.recovery.VLSNRecoveryProxy;
import com.sleepycat.je.tree.FileSummaryLN;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.tree.MapLN;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.utilint.DbLsn;

/**
 * INFileReader supports recovery by scanning log files during the IN rebuild
 * pass. It looks for internal nodes (all types), segregated by whether they
 * belong to the main tree or the duplicate trees.
 *
 * <p>This file reader can also be run in tracking mode to keep track of the
 * maximum node ID, database ID and txn ID seen so those sequences can be
 * updated properly at recovery.  In this mode it also performs utilization
 * counting.  It is only run once in tracking mode per recovery, in the first
 * phase of recovery.</p>
 */
public class INFileReader extends FileReader {

    /* Information about the last entry seen. */
    private boolean lastEntryWasDelete;
    private boolean lastEntryWasDupDelete;
    private LogEntryType fromLogType;
    private boolean isProvisional;

    /*
     * targetEntryMap maps DbLogEntryTypes to log entries. We use this
     * collection to find the right LogEntry instance to read in the current
     * entry.
     */
    private Map<LogEntryType, LogEntry> targetEntryMap;
    private LogEntry targetLogEntry;

    /* Set of non-target log entry types for ID tracking. */
    private Set<LogEntryType> idTrackingSet;
    /* Cache of non-target log entries for ID tracking. */
    private Map<LogEntryType, LogEntry> idTrackingMap;

    private boolean trackIds;
    private long minReplicatedNodeId;
    private long maxNodeId;
    private long minReplicatedDbId;
    private long maxDbId;
    private long minReplicatedTxnId;
    private long maxTxnId;
    private boolean mapDbOnly;
    private long ckptEnd;

    /* Used for utilization tracking. */
    private long partialCkptStart;
    private RecoveryUtilizationTracker tracker;

    /* Used for replication. */
    private VLSNRecoveryProxy vlsnProxy;

    /** DBs that may violate the rule for upgrading to log version 8. */
    private Set<DatabaseId> logVersion8UpgradeDbs;
    private AtomicBoolean logVersion8UpgradeDeltas;

    /**
     * Create this reader to start at a given LSN.
     */
    public INFileReader(EnvironmentImpl env,
                        int readBufferSize,
                        long startLsn,
                        long finishLsn,
                        boolean trackIds,
                        boolean mapDbOnly,
                        long partialCkptStart,
                        long ckptEnd,
                        RecoveryUtilizationTracker tracker) {
        this(env, readBufferSize, startLsn, finishLsn, trackIds, mapDbOnly,
             partialCkptStart, ckptEnd, tracker,
             null /*logVersion8UpgradeDbs*/,
             null /*logVersion8UpgradeDeltas*/);
    }

    /**
     * Create with logVersion8UpgradeDbs and logVersion8UpgradeDeltas params.
     */
    public INFileReader(EnvironmentImpl env,
                        int readBufferSize,
                        long startLsn,
                        long finishLsn,
                        boolean trackIds,
                        boolean mapDbOnly,
                        long partialCkptStart,
                        long ckptEnd,
                        RecoveryUtilizationTracker tracker,
                        Set<DatabaseId> logVersion8UpgradeDbs,
                        AtomicBoolean logVersion8UpgradeDeltas)
        throws DatabaseException {

        super(env, readBufferSize, true, startLsn, null,
              DbLsn.NULL_LSN, finishLsn);

        this.trackIds = trackIds;
        this.mapDbOnly = mapDbOnly;
        this.ckptEnd = ckptEnd;
        targetEntryMap = new HashMap<LogEntryType, LogEntry>();

        if (trackIds) {
            maxNodeId = 0;
            maxDbId = 0;
            maxTxnId = 0;
            minReplicatedNodeId = 0;
            minReplicatedDbId = DbTree.NEG_DB_ID_START;
            minReplicatedTxnId = 0;
            this.tracker = tracker;
            this.partialCkptStart = partialCkptStart;

            idTrackingSet = new HashSet<LogEntryType>();
            idTrackingMap = new HashMap<LogEntryType, LogEntry>();

            /*
             * Need all nodes for tracking:
             * - Need all INs for node ID tracking.
             * - Need all LNs for obsolete tracking.
             * - Need txnal LNs for txn ID tracking.
             * - Need FileSummaryLN for obsolete tracking.
             * - Need MapLN for obsolete and DB ID tracking.
             * - Need BINDelta for obsolete tracking.
             */
            for (LogEntryType entryType : LogEntryType.getAllTypes()) {
                if (entryType.isNodeType()) {
                    idTrackingSet.add(entryType);
                }
            }
            idTrackingSet.add(LogEntryType.LOG_BIN_DELTA);

            /* For tracking VLSNs. */
            vlsnProxy = envImpl.getVLSNProxy();
            idTrackingSet.add(LogEntryType.LOG_ROLLBACK_START);

            /* For checking for log version 8 upgrade errors. */
            this.logVersion8UpgradeDbs = logVersion8UpgradeDbs;
            this.logVersion8UpgradeDeltas = logVersion8UpgradeDeltas;
        }
    }

    /**
     * Configure this reader to target this kind of entry.
     */
    public void addTargetType(LogEntryType entryType)
        throws DatabaseException {

        targetEntryMap.put(entryType, entryType.getNewLogEntry());
    }

    /*
     * Utilization Tracking
     * --------------------
     * This class counts all new log entries and obsolete INs.  Obsolete LNs,
     * on the other hand, are counted by RecoveryManager undo/redo.
     *
     * Utilization counting is done in the first recovery pass where IDs are
     * tracked (trackIds=true).  Processing is split between isTargetEntry
     * and processEntry as follows.
     *
     * isTargetEntry counts only new non-node entries; this can be done very
     * efficiently using only the LSN and entry type, without reading and
     * unmarshalling the entry.
     *
     * processEntry counts new node entries and obsolete INs.
     *
     * processEntry also resets (sets all counters to zero and clears obsolete
     * offsets) the tracked summary for a file or database when a FileSummaryLN
     * or MapLN is encountered.  This clears the totals that have accumulated
     * during this recovery pass for entries prior to that point.  We only want
     * to count utilization for entries after that point.
     *
     * In addition, when processEntry encounters a FileSummaryLN or MapLN, its
     * LSN is recorded in the tracker.  This information is used during IN and
     * LN utilization counting.  For each file, knowing the LSN of the last
     * logged FileSummaryLN for that file allows the undo/redo code to know
     * whether to perform obsolete countng.  If the LSN of the FileSummaryLN is
     * less than (to the left of) the LN's LSN, obsolete counting should be
     * performed.  If it is greater, obsolete counting is already included in
     * the logged FileSummaryLN and should not be repeated to prevent double
     * counting.  The same thing is true of counting per-database utilization
     * relative to the LSN of the last logged MapLN.
     */

    /**
     * If we're tracking node, database and txn IDs, we want to see all node
     * log entries. If not, we only want to see IN entries.
     */
    @Override
    protected boolean isTargetEntry()
        throws DatabaseException {

        lastEntryWasDelete = false;
        lastEntryWasDupDelete = false;
        targetLogEntry = null;
        isProvisional = currentEntryHeader.getProvisional().isProvisional
            (getLastLsn(), ckptEnd);

        /* Get the log entry type instance we need to read the entry. */
        fromLogType = LogEntryType.findType(currentEntryHeader.getType());
        LogEntry possibleTarget = targetEntryMap.get(fromLogType);

        /* Always select a non-provisional target entry. */
        if (!isProvisional) {
            targetLogEntry = possibleTarget;
        }

        /* Recognize IN deletion. */
        if (LogEntryType.LOG_IN_DELETE_INFO.equals(fromLogType)) {
            lastEntryWasDelete = true;
        }
        if (LogEntryType.LOG_IN_DUPDELETE_INFO.equals(fromLogType)) {
            lastEntryWasDupDelete = true;
        }

        /* If we're not tracking IDs, select only the targeted entry. */
        if (!trackIds) {
            return (targetLogEntry != null);
        }

        /*
         * Count all non-node non-delta entries except for the file header as
         * new.  UtilizationTracker does not count the file header.  Node/delta
         * entries will be counted in processEntry.  Null is passed for the
         * database ID; it is only needed for node entries.
         */
        if (!fromLogType.isNodeType() &&
            !fromLogType.equals(LogEntryType.LOG_BIN_DELTA) &&
            !LogEntryType.LOG_FILE_HEADER.equals(fromLogType)) {
            tracker.countNewLogEntry(getLastLsn(),
                                     fromLogType,
                                     currentEntryHeader.getSize() +
                                     currentEntryHeader.getItemSize(),
                                     null); // DatabaseId
        }

        /*
         * When we encouter a DbTree log entry, reset the tracked summary for
         * the ID and Name mapping DBs.  This clears what we accummulated
         * previously for these databases during this recovery pass. Save the
         * LSN for these databases for use by undo/redo.
         */
        if (LogEntryType.LOG_DBTREE.equals(fromLogType)) {
            tracker.saveLastLoggedMapLN(DbTree.ID_DB_ID, getLastLsn());
            tracker.saveLastLoggedMapLN(DbTree.NAME_DB_ID, getLastLsn());
            tracker.resetDbInfo(DbTree.ID_DB_ID);
            tracker.resetDbInfo(DbTree.NAME_DB_ID);
        }

        /*
         * TODO: For now we track the VLSN for all replicated entries in
         * processTarget, so we return true if getReplicated below.  Remove
         * this when FileReader supports a correct getLastLsn when called from
         * isTargetEntry.
         */

        /* Return true if this entry should be passed on to processEntry. */
        return (targetLogEntry != null) ||
               idTrackingSet.contains(fromLogType) ||
               currentEntryHeader.getReplicated();
    }

    /**
     * This reader returns non-provisional INs and IN delete entries.
     * In tracking mode, it may also scan log entries that aren't returned:
     *  -to set the sequences for txn, node, and database ID.
     *  -to update utilization and obsolete offset information.
     *  -for VLSN mappings for recovery
     */
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        boolean useEntry = false;

        /* Read targeted entry. */
        if (targetLogEntry != null) {
            targetLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
            /* Apply the mapDbOnly setting. */
            DatabaseId dbId = getDatabaseId();
            boolean isMapDb = dbId.equals(DbTree.ID_DB_ID);
            useEntry = (!mapDbOnly || isMapDb);
        }

        /* If we're not tracking IDs, we're done. */
        if (!trackIds) {
            return useEntry;
        }

        /*
         * TODO: For now we track the VLSN for all replicated entries here.
         * Move this into isTargetEntry when FileReader supports a correct
         * getLastLsn when called from isTargetEntry.
         */
        /* Track VLSNs in the log entry header of all replicated entries. */
        if (currentEntryHeader.getReplicated()) {
            vlsnProxy.trackMapping(getLastLsn(),
                                   currentEntryHeader,
                                   null /*targetLogEntry*/);
        }
        /* TODO Also remove this block. */
        if (targetLogEntry == null && !idTrackingSet.contains(fromLogType)) {
            int endPosition = threadSafeBufferPosition(entryBuffer) +
                currentEntryHeader.getItemSize();
            threadSafeBufferPosition(entryBuffer, endPosition);
            return useEntry;
        }

        /* Read non-target entry. */
        if (targetLogEntry == null) {
            assert idTrackingSet.contains(fromLogType);
            targetLogEntry = idTrackingMap.get(fromLogType);
            if (targetLogEntry == null) {
                targetLogEntry = fromLogType.getNewLogEntry();
                idTrackingMap.put(fromLogType, targetLogEntry);
            }
            targetLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
        }

        /*
         * Count node and delta entries as new.  Non-node/delta entries are
         * counted in isTargetEntry.
         */
        if (fromLogType.isNodeType() ||
            fromLogType.equals(LogEntryType.LOG_BIN_DELTA)) {
            tracker.countNewLogEntry(getLastLsn(), fromLogType,
                                     currentEntryHeader.getSize() +
                                     currentEntryHeader.getItemSize(),
                                     targetLogEntry.getDbId());
        }

        /* Track VLSNs in RollbackStart. */
        if (fromLogType.equals(LogEntryType.LOG_ROLLBACK_START)) {
            vlsnProxy.trackMapping(getLastLsn(),
                                   currentEntryHeader,
                                   targetLogEntry);
        }

        DatabaseId dbIdToReset = null;
        long fileNumToReset = -1;

        /* Process LN types. */
        if (fromLogType.isLNType()) {
            LNLogEntry lnEntry = (LNLogEntry) targetLogEntry;

            if (fromLogType.equals(LogEntryType.LOG_MAPLN) ||
                fromLogType.equals(LogEntryType.LOG_MAPLN_TRANSACTIONAL)) {
                MapLN mapLN = (MapLN) lnEntry.getMainItem();
                DatabaseId dbId = mapLN.getDatabase().getId();

                /* Track latest DB ID. */
                long dbIdVal = dbId.getId();
                maxDbId = (dbIdVal > maxDbId) ? dbIdVal : maxDbId;
                minReplicatedDbId = (dbIdVal < minReplicatedDbId) ?
                    dbIdVal : minReplicatedDbId;

                /*
                 * When a MapLN is encountered, reset the tracked information
                 * for that database.  This clears what we accummulated
                 * previously for the database during this recovery pass.
                 */
                dbIdToReset = dbId;

                /* Save the LSN of the MapLN for use by undo/redo. */
                tracker.saveLastLoggedMapLN(dbId, getLastLsn());
            }

            /* Track latest txn ID. */
            if (fromLogType.isTransactional()) {
                long txnId = lnEntry.getTxnId().longValue();
                maxTxnId = (txnId > maxTxnId) ? txnId : maxTxnId;
                minReplicatedTxnId = (txnId < minReplicatedTxnId) ?
                    txnId : minReplicatedTxnId;
            }

            /*
             * When a FileSummaryLN is encountered, reset the tracked summary
             * for that file.  This clears what we accummulated previously for
             * the file during this recovery pass.
             */
            if (LogEntryType.LOG_FILESUMMARYLN.equals(fromLogType)) {
                lnEntry.postFetchInit(false /*isDupDb*/);
                byte[] keyBytes = lnEntry.getKey();
                FileSummaryLN fsln = (FileSummaryLN) lnEntry.getMainItem();
                long fileNum = fsln.getFileNumber(keyBytes);
                fileNumToReset = fileNum;

                /* Save the LSN of the FileSummaryLN for use by undo/redo. */
                tracker.saveLastLoggedFileSummaryLN(fileNum, getLastLsn());

                /*
                 * Do not cache the file summary in the UtilizationProfile here,
                 * since it may be for a deleted log file. [#10395]
                 */
            }
        }

        /* Process IN types. */
        if (fromLogType.isINType()) {
            INLogEntry inEntry = (INLogEntry) targetLogEntry;

            /* Keep track of the largest node ID seen. */
            long nodeId = inEntry.getNodeId();
            assert (nodeId != Node.NULL_NODE_ID);
            maxNodeId = (nodeId > maxNodeId) ? nodeId : maxNodeId;
            minReplicatedNodeId = (nodeId < minReplicatedNodeId) ?
                nodeId : minReplicatedNodeId;
        }

        /* Process INContainingEntry types. */
        if (fromLogType.isINType() ||
            fromLogType.equals(LogEntryType.LOG_BIN_DELTA)) {

            INContainingEntry inEntry =
                (INContainingEntry) targetLogEntry;

            /*
             * Count the obsolete LSN of the previous version, if available and
             * if not already counted.  Use inexact counting for two reasons:
             * 1) we don't always have the full LSN because earlier log
             * versions only had the file number, and 2) we can't guarantee
             * obsoleteness for provisional INs.
             */
            long oldLsn = inEntry.getPrevFullLsn();
            if (oldLsn != DbLsn.NULL_LSN) {
                long newLsn = getLastLsn();
                tracker.countObsoleteIfUncounted
                    (oldLsn, newLsn, fromLogType, 0, inEntry.getDbId(),
                     false /*countExact*/);
            }
            oldLsn = inEntry.getPrevDeltaLsn();
            if (oldLsn != DbLsn.NULL_LSN) {
                long newLsn = getLastLsn();
                tracker.countObsoleteIfUncounted
                    (oldLsn, newLsn, fromLogType, 0, inEntry.getDbId(),
                     false /*countExact*/);
            }

            /*
             * Count a provisional IN as obsolete if it follows
             * partialCkptStart.  It cannot have been already counted, because
             * provisional INs are not normally counted as obsolete; they are
             * only considered obsolete when they are part of a partial
             * checkpoint.
             *
             * Depending on the exact point at which the checkpoint was
             * aborted, this technique is not always accurate; therefore
             * inexact counting must be used.
             */
            if (isProvisional && partialCkptStart != DbLsn.NULL_LSN) {
                oldLsn = getLastLsn();
                if (DbLsn.compareTo(partialCkptStart, oldLsn) < 0) {
                    tracker.countObsoleteUnconditional
                        (oldLsn, fromLogType, 0, inEntry.getDbId(),
                         false /*countExact*/);
                }
            }
        }

        /*
         * Reset file and database utilization info only after counting a new
         * or obsolete node.  The MapLN itself is a node and will be counted as
         * new above, and we must reset that count as well.
         */
        if (fileNumToReset != -1) {
            tracker.resetFileInfo(fileNumToReset);
        }
        if (dbIdToReset != null) {
            tracker.resetDbInfo(dbIdToReset);
        }

        /*
         * Add candidate DB IDs and note deltas for possible log version 8
         * upgrade violations.
         */
        if (currentEntryHeader.getVersion() < 8) {
            if (logVersion8UpgradeDbs != null &&
                fromLogType.isNodeType()) {
                logVersion8UpgradeDbs.add(targetLogEntry.getDbId());
            }
            if (logVersion8UpgradeDeltas != null &&
                (fromLogType.equals(LogEntryType.LOG_BIN_DELTA) ||
                 fromLogType.equals(LogEntryType.LOG_DUP_BIN_DELTA))) {
                logVersion8UpgradeDeltas.set(true);
            }
        }

        /* Return true if this is a targeted entry. */
        return useEntry;
    }

    /**
     * Get the last IN seen by the reader.
     */
    public IN getIN(DatabaseImpl dbImpl)
        throws DatabaseException {

        return ((INContainingEntry) targetLogEntry).getIN(dbImpl);
    }

    /**
     * Get the last databaseId seen by the reader.
     */
    public DatabaseId getDatabaseId() {
        return ((INContainingEntry) targetLogEntry).getDbId();
    }

    /**
     * Get the maximum node ID seen by the reader.
     */
    public long getMaxNodeId() {
        return maxNodeId;
    }

    public long getMinReplicatedNodeId() {
        return minReplicatedNodeId;
    }

    /**
     * Get the maximum DB ID seen by the reader.
     */
    public long getMaxDbId() {
        return maxDbId;
    }

    public long getMinReplicatedDbId() {
        return minReplicatedDbId;
    }

    /**
     * Get the maximum txn ID seen by the reader.
     */
    public long getMaxTxnId() {
        return maxTxnId;
    }

    public long getMinReplicatedTxnId() {
        return minReplicatedTxnId;
    }

    /**
     * @return true if the last entry was a delete info entry.
     */
    public boolean isDeleteInfo() {
        return lastEntryWasDelete;
    }

    /**
     * @return true if the last entry was a dup delete info entry.
     */
    public boolean isDupDeleteInfo() {
        return lastEntryWasDupDelete;
    }

    /**
     * @return true if the last entry was a BINDelta.
     */
    public boolean isBINDelta() {
        return targetLogEntry.getLogType().equals(LogEntryType.LOG_BIN_DELTA);
    }

    public VLSNRecoveryProxy getVLSNProxy() {
        return vlsnProxy;
    }
}
