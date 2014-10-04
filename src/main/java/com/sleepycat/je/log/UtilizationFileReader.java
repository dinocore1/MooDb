/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.FileSummary;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.BINDeltaLogEntry;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.SingleItemEntry;
import com.sleepycat.je.tree.Node;
import com.sleepycat.je.tree.dupConvert.INDeleteInfo;
import com.sleepycat.je.tree.dupConvert.INDupDeleteInfo;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.txn.TxnEnd;
import com.sleepycat.je.txn.TxnChain.CompareSlot;
import com.sleepycat.je.utilint.DbLsn;

/**
 * Summarizes the utilized and unutilized portion of each log file by examining
 * each log entry.  Does not use the Cleaner UtilizationProfile information in
 * order to provide a second measure against which to evaluation the
 * UtilizationProfile accuracy.
 *
 * Limitations
 * ===========
 * BINDeltas are all considered obsolete, because we would have to model the
 * Btree differently to count them accurately.  We would need the ability to
 * lookup an IN by LSN, since the delta does not contain the node ID.
 *
 * Holds memory for all active LNs, i.e., a duplication of what is in the
 * Btree.
 *
 * Holds memory for txns with no abort or commit entry until the end of the
 * run.  Could consider these txns to be aborted when a recovery is
 * encountered, if we can recognize a recovery.
 *
 * Relies on the live Btree to get Database information -- whether dups are
 * configured and whether the database is deleted -- rather than reading the
 * log, which would require a separate initial pass.  Because of cleaner
 * migration, we can't rely on a database's MapLN preceding the other log
 * entries in the database.
 */
public class UtilizationFileReader extends FileReader {

    private static final boolean DEBUG = true;

    /* Long file -> FileSummary */
    private final Map<Long, FileSummary> summaries;

    /* Long IN node ID -> NodeInfo */
    private final Map<Long, NodeInfo> activeINs;

    /* LN CompareSlot -> NodeInfo */
    private final Map<CompareSlot, NodeInfo> activeLNs;

    /*
     * Long txn ID -> List of pairs,  where each pair is
     * [ExtendedFileSummary, LNLogEntry]
     */
    private final Map<Long, List<Object>> txns;

    /* Holds one [ExtendedFileSummary, LNLogEntry] */
    private final List<Object> twoEntryList;

    /* Cache of DB ID -> DatabaseImpl for reading live databases. */
    private final Map<DatabaseId, DatabaseImpl> dbCache;
    private final DbTree dbTree;

    private UtilizationFileReader(EnvironmentImpl envImpl, int readBufferSize)
        throws DatabaseException {

        super(envImpl,
              readBufferSize,
              true,            // read forward
              DbLsn.NULL_LSN,  // start LSN
              null,            // single file number
              DbLsn.NULL_LSN,  // end of file LSN
              DbLsn.NULL_LSN); // finish LSN

        summaries = new HashMap<Long, FileSummary>();
        activeINs = new HashMap<Long, NodeInfo>();
        activeLNs = new TreeMap<CompareSlot, NodeInfo>();
        txns = new HashMap<Long, List<Object>>();
        dbCache = new HashMap<DatabaseId, DatabaseImpl>();
        dbTree = envImpl.getDbTree();

        twoEntryList = new ArrayList<Object>();
        twoEntryList.add(null);
        twoEntryList.add(null);
    }

    @Override
    protected boolean isTargetEntry() {

        /* 
         * UtilizationTracker is supposed to mimic the UtilizationProfile. 
         * Accordingly it does not count the file header or invisible log 
         * entries because those entries are not covered by the U.P.
         */
        return ((currentEntryHeader.getType() !=
                 LogEntryType.LOG_FILE_HEADER.getTypeNum()) &&
                !currentEntryHeader.isInvisible());
    }

    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        final LogEntryType lastEntryType =
            LogEntryType.findType(currentEntryHeader.getType());
        final LogEntry entry = lastEntryType.getNewLogEntry();
        entry.readEntry(envImpl, currentEntryHeader, entryBuffer);

        ExtendedFileSummary summary = 
                (ExtendedFileSummary) summaries.get(window.currentFileNum());
        if (summary == null) {
            summary = new ExtendedFileSummary();
            summaries.put(window.currentFileNum(), summary);
        }

        final int size = getLastEntrySize();

        summary.totalCount += 1;
        summary.totalSize += size;

        if (entry instanceof LNLogEntry) {
            final LNLogEntry lnEntry = (LNLogEntry) entry;
            if (DEBUG) {
                final int otherSize = lnEntry.getLastLoggedSize();
                if (size != otherSize) {
                    System.out.println
                        ("LogReader.getLastEntrySize=" + size +
                         " LNEntry.getLastLoggedSize=" + otherSize +
                         " " + lnEntry.getLogType());
                }
            }

            /* Save transactional entry, apply non-transactional entry. */
            if (lastEntryType.isTransactional()) {
                final Long txnId = Long.valueOf(lnEntry.getTransactionId());
                List<Object> txnEntries = txns.get(txnId);
                if (txnEntries == null) {
                    txnEntries = new ArrayList<Object>();
                    txns.put(txnId, txnEntries);
                }
                txnEntries.add(summary);
                txnEntries.add(lnEntry);
            } else {
                twoEntryList.set(0, summary);
                twoEntryList.set(1, lnEntry);
                applyTxn(twoEntryList, true);
            }
        } else if (entry instanceof INLogEntry) {

            /* Count IN. */
            final INLogEntry inEntry = (INLogEntry) entry;
            final Long nodeId = Long.valueOf(inEntry.getNodeId());
            summary.totalINCount += 1;
            summary.totalINSize += size;
            countObsoleteIN(nodeId);
            putActiveIN(nodeId, size, summary, inEntry.getDbId().getId());

        } else if (entry instanceof BINDeltaLogEntry) {

            /* Count Delta as IN. */
            summary.totalINCount += 1;
            summary.totalINSize += size;

            /* Most deltas are obsolete, so count them all obsolete. */
            summary.obsoleteINCount += 1;
            summary.recalcObsoleteINSize += size;

        } else if (entry instanceof SingleItemEntry) {

            /* Count deleted IN. */
            final Object item = ((SingleItemEntry) entry).getMainItem();
            final long deletedNodeId;
            if (item instanceof INDeleteInfo) {
                deletedNodeId = ((INDeleteInfo) item).getDeletedNodeId();
            } else if (item instanceof INDupDeleteInfo) {
                deletedNodeId = ((INDupDeleteInfo) item).getDeletedNodeId();
            } else {
                deletedNodeId = Node.NULL_NODE_ID;
            }
            if (deletedNodeId != Node.NULL_NODE_ID) {
                final Long nodeId = deletedNodeId;
                countObsoleteIN(nodeId);
                activeINs.remove(nodeId);
            }

            /* Apply transaction on commit or abort. */
            if (item instanceof TxnEnd) {
                final Long txnId = Long.valueOf(entry.getTransactionId());
                final List<Object> txnEntries = txns.remove(txnId);
                if (txnEntries != null) {
                    applyTxn(txnEntries, item instanceof TxnCommit);
                }
            }
        }

        return true;
    }

    private void applyTxn(List<Object> entries, boolean commit) {
        for (int i = 0; i < entries.size(); i += 2) {
            final ExtendedFileSummary summary =
                (ExtendedFileSummary) entries.get(i);
            final LNLogEntry lnEntry = (LNLogEntry) entries.get(i + 1);
            final DatabaseId dbId = lnEntry.getDbId();
            final DatabaseImpl dbImpl =
                dbTree.getDb(dbId, -1 /*timeout*/, dbCache);
            final int size = lnEntry.getLastLoggedSize();

            summary.totalLNCount += 1;
            summary.totalLNSize += size;

            if (!commit ||
                lnEntry.isDeleted() ||
                dbImpl == null ||
                dbImpl.isDeleteFinished()) {

                /* Count immediately obsolete LN. */
                summary.obsoleteLNCount += 1;
                summary.recalcObsoleteLNSize += size;
            }
            if (commit && dbImpl != null) {
                /* Count committed LN. */
                lnEntry.postFetchInit(dbImpl);
                final CompareSlot slot = new CompareSlot(dbImpl, lnEntry);
                countObsoleteLN(slot);
                if (dbImpl.isDeleteFinished() || lnEntry.isDeleted()) {
                    activeLNs.remove(slot);
                } else {
                    putActiveLN(slot, size, summary,
                                lnEntry.getDbId().getId());
                }
            }
        }
    }

    private void finishProcessing() {
        /* Apply uncomitted transactions. */
        for (List<Object> txnEntries : txns.values()) {
            applyTxn(txnEntries, false);
        }
    }

    private void cleanUp() {
        dbTree.releaseDbs(dbCache);
    }

    private void putActiveIN(Long nodeId,
                             int size,
                             ExtendedFileSummary summary,
                             long dbId) {
        NodeInfo info = activeINs.get(nodeId);
        if (info == null) {
            info = new NodeInfo();
            activeINs.put(nodeId, info);
        }
        info.size = size;
        info.summary = summary;
        info.dbId = dbId;
    }

    private void putActiveLN(CompareSlot slot,
                             int size,
                             ExtendedFileSummary summary,
                             long dbId) {
        NodeInfo info = activeLNs.get(slot);
        if (info == null) {
            info = new NodeInfo();
            activeLNs.put(slot, info);
        }
        info.size = size;
        info.summary = summary;
        info.dbId = dbId;
    }

    private void countObsoleteIN(Long nodeId) {
        final NodeInfo info = activeINs.get(nodeId);
        if (info != null) {
            final ExtendedFileSummary summary = info.summary;
            summary.obsoleteINCount += 1;
            summary.recalcObsoleteINSize += info.size;
        }
    }

    private void countObsoleteLN(CompareSlot slot) {
        final NodeInfo info = activeLNs.get(slot);
        if (info != null) {
            final ExtendedFileSummary summary = info.summary;
            summary.obsoleteLNCount += 1;
            summary.recalcObsoleteLNSize += info.size;
        }
    }

    /**
     * Creates a UtilizationReader, reads the log, and returns the resulting
     * Map of Long file number to FileSummary.
     */
    public static Map<Long, FileSummary>
        calcFileSummaryMap(EnvironmentImpl envImpl) {

        final int readBufferSize = envImpl.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);

        final UtilizationFileReader reader =
            new UtilizationFileReader(envImpl, readBufferSize);
        try {
            while (reader.readNextEntry()) {
                /* All the work is done in processEntry. */
            }
            reader.finishProcessing();
            return reader.summaries;
        } finally {
            reader.cleanUp();
        }
    }

    private static class ExtendedFileSummary extends FileSummary {
        private int recalcObsoleteINSize;
        private int recalcObsoleteLNSize;

        /**
         * Overrides the LN size calculation to return the recalculated number
         * of obsolete LN bytes.
         */
        @Override
        public int getObsoleteLNSize() {
            return recalcObsoleteLNSize;
        }

        /**
         * Overrides the IN size calculation to return the recalculated number
         * of obsolete IN bytes.
         */
        @Override
        public int getObsoleteINSize() {
            return recalcObsoleteINSize;
        }

        /**
         * Overrides to add the extended data fields.
         */
        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append(super.toString());
            buf.append("<extended-info recalcObsoleteINSize=\"");
            buf.append(recalcObsoleteINSize);
            buf.append("\" recalcObsoleteLNSize=\"");
            buf.append(recalcObsoleteLNSize);
            buf.append("\"/>");
            return buf.toString();
        }
    }

    private static class NodeInfo {
        ExtendedFileSummary summary;
        int size;
        long dbId;
    }
}
