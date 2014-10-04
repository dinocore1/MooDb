/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.txn;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.WholeEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.tree.Key;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/**
 * TxnChain supports Txn.rollback(), which undoes the write operations for a
 * given transaction to an arbitrary point. This is used by HA during the
 * syncup phase, and by recovery when it is processes a RollbackStart/End.
 *
 * In the JE log, the log entries that make up a transaction are chained, but
 * each entry refers only to its pre-transaction value, and doesn't know its
 * intra-txn previous value. For example, a log looks like this:
 *
 * lsn       key              abortlsn
 * 100       A/node 10        null_lsn (first instance of a record)
 *  .....  txn begins .....
 * 200       A'/node10        100
 * 300       A'delete/node10  100
 * 400       A new/node77     100
 * 500       B/node80         null_lsn
 *
 * When reading the log, we can find all the records in the transaction. This
 * chain exists:
 * 500->400->300->200->null_lsn
 *
 * To rollback to an arbitrary entry in the transaction, we need a chain of all
 * the records that occupied a given BIN slot during the transaction.
 * chain. The key, data, and comparators are used to determine which records
 * hash to the same slot, mimicking the btree itself.
 *
 * 500 -> null_lsn (reverts to no-record)
 * 400 -> 300      (reverts to previous deleted record in that slot)
 * 300 -> 200      (reverts to A', intermediate version of the record)
 * 200 -> 100      (reverts to original, pre-txn version)
 */
public class TxnChain {
    private final Map<DatabaseId, DatabaseImpl> undoDatabases;
    private final EnvironmentImpl envImpl;

    /* Write locks that are retained, from the preserved portion of the txn.*/
    private final Set<Long> remainingLockedNodes;

    /* List of log entries to be rolled back, from end to beginning of txn. */
    private final LinkedList<RevertInfo> revertList;

    /* The last applied VLSN in this txn, after rollback has occurred. */
    private VLSN lastValidVLSN;

    /*
     * Find the previous version for all entries in this transaction. Used by
     * recovery. This differs from the constructor used by syncup rollback
     * which is instigated by the txn. In this case, there is no cache of
     * DatabaseImpls.
     */
    public TxnChain(long lastLoggedLsn,
                    long txnId,
                    long matchpoint,
                    EnvironmentImpl envImpl)  {
        this(lastLoggedLsn, txnId, matchpoint, null, envImpl);
    }

    /*
     * Find the previous version for all entries in this transaction.
     * DatabaseImpls used during txn chain creation are taken from the
     * transaction's undoDatabases cache.
     */
    public TxnChain(long lastLoggedLsn,
                    long txnId,
                    long matchpoint,
                    Map<DatabaseId, DatabaseImpl> undoDatabases,
                    EnvironmentImpl envImpl)
        throws DatabaseException {

        this.envImpl = envImpl;
        this.undoDatabases = undoDatabases;

        LogManager logManager = envImpl.getLogManager();
        remainingLockedNodes = new HashSet<Long>();

        /*
         * keyToLsns holds lists of all intermediate versions, organized by
         * slot value. Using the header example, keyToLsns would hold this:
         *
         * (B)   -> 500
         * (A/A')-> 400, 300, 200
         */
        TreeMap<CompareSlot, LinkedList<Long>> keyToLsns =
            new TreeMap<CompareSlot, LinkedList<Long>>();

        /* The chain list will have an entry per entry in the transaction.*/
        LinkedList<VersionCalculator> chain =
            new LinkedList<VersionCalculator>();

        /*
         * Traverse this transaction's entire chain to record intermediate
         * versions.  Using the example in the header, traverse from lsn
         * 500->lsn 100.
         */
        long undoLsn = lastLoggedLsn;
        try {
            lastValidVLSN = VLSN.NULL_VLSN;
            while (undoLsn != DbLsn.NULL_LSN) {
                WholeEntry wholeEntry =
                    logManager.getLogEntryAllowInvisible(undoLsn);
                LNLogEntry undoEntry = (LNLogEntry) wholeEntry.getEntry();

                DatabaseImpl dbImpl = getDatabaseImpl(undoEntry.getDbId());
                undoEntry.postFetchInit(dbImpl);

                try {

                    /*
                     * Add the LSN of the entry we're now perusing to the
                     * keyToLsns map.
                     */
                    CompareSlot entrySlot = new CompareSlot(dbImpl, undoEntry);
                    LinkedList<Long> lsns = keyToLsns.get(entrySlot);

                    if (lsns == null) {
                        lsns = new LinkedList<Long>();
                        keyToLsns.put(entrySlot, lsns);
                    }
                    lsns.add(undoLsn);

                    /*
                     * If this is an entry that will be rolled back, save
                     * enough information to calculate the prev version.
                     */
                    if (DbLsn.compareTo(undoLsn, matchpoint) > 0) {
                        chain.add(new VersionCalculator
                                  (undoLsn,
                                   undoEntry.getAbortLsn(),
                                   undoEntry.getAbortKnownDeleted(),
                                   lsns));
                    } else {
                        remainingLockedNodes.add(undoLsn);
                        if (lastValidVLSN != null &&
                            lastValidVLSN.isNull() &&
                            wholeEntry.getHeader().getVLSN() != null &&
                            !wholeEntry.getHeader().getVLSN().isNull()) {
                            lastValidVLSN = wholeEntry.getHeader().getVLSN();
                        }
                    }

                    /* Move on to the previous log entry for this txn. */
                    undoLsn = undoEntry.getUserTxn().getLastLsn();
                } finally {
                    releaseDatabaseImpl(dbImpl);
                }
            }
        } catch (FileNotFoundException e) {
            throw EnvironmentFailureException.promote
                (envImpl, EnvironmentFailureReason.LOG_INTEGRITY,
                 "Problem finding intermediates for txn " + txnId +
                 " at lsn " + DbLsn.getNoFormatString(undoLsn), e);
        }

        /*
         * For all the entries to be rolled back, calculate the previous
         * version.
         */
        revertList = new LinkedList<RevertInfo>();
        for (VersionCalculator calculator : chain) {
            revertList.add(calculator.findPreviousVersion());
        }
    }

    /**
     * Hide the details of whether we are getting a databaseImpl from the txn's
     * cache, or whether we're fetching it from the dbMapTree at recovery.
     */
    private DatabaseImpl getDatabaseImpl(DatabaseId dbId) {
        if (undoDatabases != null) {
            return undoDatabases.get(dbId);
        }

        return envImpl.getDbTree().getDb(dbId);
    }

    /** Only needed if we are in recovery, and fetched the DatabaseImpl. */
    private void releaseDatabaseImpl(DatabaseImpl dbImpl) {
        if (undoDatabases == null) {
            envImpl.getDbTree().releaseDb(dbImpl);
        }
    }

    /**
     * Returns LSNs for all nodes that should remain locked by the txn.  Note
     * that when multiple versions of a record were locked by the txn, the LSNs
     * of all versions are returned.  Only the latest version will actually be
     * locked.
     */
    public Set<Long> getRemainingLockedNodes() {
        return remainingLockedNodes;
    }

    /**
     * Return information about the next item on the transaction chain and
     * remove it from the chain.
     */
    public RevertInfo pop() {
        return revertList.remove();
    }

    public VLSN getLastValidVLSN() {
        return lastValidVLSN;
    }

    @Override
    public String toString() {
        return revertList.toString();
    }

    public static class RevertInfo {
        public long revertLsn;
        public boolean revertKnownDeleted;

        RevertInfo(long revertLsn,
                   boolean revertKnownDeleted) {
            this.revertLsn = revertLsn;
            this.revertKnownDeleted = revertKnownDeleted;
        }

        @Override
        public String toString() {
            return "revertLsn=" + DbLsn.getNoFormatString(revertLsn) +
                " revertKD=" + revertKnownDeleted;
        }
    }

    /**
     * Finds the LSN and known deleted value for the previous version of a
     * given LN log entry. That previous version is whatever occupied its slot
     * in the tree before, which might or might not have the same logical node.
     * If there are multiple items in the list, each item reverts to its
     * successor on the list. If there is only one item, then the item needs to
     * revert to its pre-txn value. Using the example above:
     *
     * (B) -> 500
     * entry 500 is the only thing on its list, it reverts to the pre-txn
     * version.
     *
     * (A/A') -> 400, 300, 200
     * entry 400 reverts to 300, and pops itself off the list.
     * entry 300 reverts to 200, and pops itself off the list.
     * entry 200 is the last thing on its list, it reverts to the pre-txn
     * version.
     */
    private static  class VersionCalculator {
        final long entryLsn;
        final long entryAbortLsn;
        final boolean entryAbortKnownDeleted;
        final LinkedList<Long> lsns;

        VersionCalculator(long entryLsn,
                          long entryAbortLsn,
                          boolean entryAbortKnownDeleted,
                          LinkedList<Long> lsns) {
            this.entryLsn = entryLsn;
            this.entryAbortLsn = entryAbortLsn;
            this.entryAbortKnownDeleted = entryAbortKnownDeleted;
            this.lsns = lsns;
        }

        RevertInfo findPreviousVersion() {
            Long topVersion = lsns.remove();
            assert(entryLsn == topVersion) :
            "entryLsn= " + DbLsn.getNoFormatString(entryLsn) +
                "topLsn= " + DbLsn.getNoFormatString(topVersion);

            if (lsns.size() == 0) {
                return new RevertInfo(entryAbortLsn,
                                      entryAbortKnownDeleted);
            }

            /*
             * The previous version is an intermediate version created by the
             * same txn.
             */
            return new RevertInfo(lsns.getFirst(),
                                  false /* abortKnownDeleted */);

        }
    }

    /**
     * Compare two keys using the appropriate comparator. Keys from different
     * databases should never be equal.
     */
    public static class CompareSlot implements Comparable<CompareSlot> {
        private final DatabaseImpl dbImpl;
        private final byte[] key;

        public CompareSlot(DatabaseImpl dbImpl, LNLogEntry undoEntry) {
            this(dbImpl, undoEntry.getKey());
        }

        private CompareSlot(DatabaseImpl dbImpl, byte[] key) {
            this.dbImpl = dbImpl;
            this.key = key;
        }

        public int compareTo(CompareSlot other) {
            int dbCompare = dbImpl.getId().compareTo(other.dbImpl.getId());
            if (dbCompare != 0) {
                /* LNs are from different databases. */
                return dbCompare;
            }

            /* Compare keys. */
            return Key.compareKeys(key, other.key, dbImpl.getKeyComparator());
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CompareSlot)) {
                return false;
            }
            return compareTo((CompareSlot) other) == 0;
        }

        @Override
        public int hashCode() {

            /*
             * Disallow use of HashSet/HashMap/etc.  TreeSet/TreeMap/etc should
             * be used instead when a CompareSlot is used as a key.
             *
             * Because a comparator may be configured that compares only a part
             * of the key, a hash code cannot take into account the key or
             * data, because hashCode() must return the same value for two
             * objects whenever equals() returns true.  We could hash the DB ID
             * alone, but that would not produce an efficient hash table.
             */
            throw EnvironmentFailureException.unexpectedState
                ("Hashing not supported");
        }
    }
}
