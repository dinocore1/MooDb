/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * BINDelta contains the information needed to create a partial (delta) BIN log
 * entry. It also knows how to combine a full BIN log entry and a delta to
 * generate a new BIN.
 */
public class BINDelta implements Loggable {

    private final DatabaseId dbId;    // owning db for this bin.
    private long lastFullLsn;   // location of last full version
    private long prevDeltaLsn;  // location of previous delta version
    private final ArrayList<DeltaInfo> deltas;  // list of key/action changes

    /**
     * Read a BIN and create the deltas.
     */
    public BINDelta(BIN bin) {
        lastFullLsn = bin.getLastFullVersion();
        prevDeltaLsn = bin.getLastDeltaVersion();
        dbId = bin.getDatabaseId();
        deltas = new ArrayList<DeltaInfo>();

        /*
         * Save every entry that has been modified since the last full version.
         * Note that we must rely on the dirty bit, and we can't infer any
         * dirtiness by comparing the last full version LSN and the child LSN.
         * That's because the child LSN may be earlier than the full version
         * LSN because of aborts.
         */
        for (int i = 0; i < bin.getNEntries(); i++) {
            if (bin.isDirty(i)) {
                deltas.add(new DeltaInfo(bin.getKey(i),
                                         bin.getLsn(i),
                                         bin.getState(i)));
            }
        }

        /* Use minimum memory. */
        deltas.trimToSize();
    }

    /**
     * For instantiating from the log.
     */
    public BINDelta() {
        dbId = new DatabaseId();
        lastFullLsn = DbLsn.NULL_LSN;
        prevDeltaLsn = DbLsn.NULL_LSN;
        deltas = new ArrayList<DeltaInfo>();
    }

    /**
     * For Sizeof.
     */
    public BINDelta(@SuppressWarnings("unused") SizeofMarker marker) {
        dbId = new DatabaseId();
        lastFullLsn = DbLsn.NULL_LSN;
        prevDeltaLsn = DbLsn.NULL_LSN;
        deltas = null; /* Computed separately. */
    }

    /**
     * @return a count of deltas for this BIN.
     */
    int getNumDeltas() {
        return deltas.size();
    }

    /**
     * @return a count of deltas for the given BIN, if a BINDelta were created.
     */
    static int getNumDeltas(BIN bin) {
        int n = 0;
        for (int i = 0; i < bin.getNEntries(); i += 1) {
            if (bin.isDirty(i)) {
                n += 1;
            }
        }
        return n;
    }

    /**
     * @return the dbId for this BIN.
     */
    public DatabaseId getDbId() {
        return dbId;
    }

    /**
     * @return the last full version of this BIN
     */
    public long getLastFullLsn() {
        return lastFullLsn;
    }

    /**
     * @return the prior delta version of this BIN, or NULL_LSN if the prior
     * version is a full BIN.  The returned value is the LSN that is obsoleted
     * by this delta.
     */
    public long getPrevDeltaLsn() {
        return prevDeltaLsn;
    }

    /**
     * Returns a key that can be used to find the BIN associated with this
     * delta.  The key of any slot will do.
     */
    public byte[] getSearchKey() {
        assert (deltas.size() > 0);
        return deltas.get(0).getKey();
    }

    /**
     * Create a BIN by fetching the full version and applying the deltas.
     */
    public BIN reconstituteBIN(DatabaseImpl dbImpl) {

        final EnvironmentImpl envImpl = dbImpl.getDbEnvironment();

        final BIN fullBIN = (BIN)
            envImpl.getLogManager().getEntryHandleFileNotFound(lastFullLsn);

        reconstituteBIN(dbImpl, fullBIN);

        return fullBIN;
    }

    /**
     * Given a full version BIN, apply the deltas.
     */
    public void reconstituteBIN(DatabaseImpl dbImpl, BIN fullBIN) {

        fullBIN.latch();
        try {

            /*
             * After this method returns, postFetchInit or postRecoveryInit
             * will normally be called, and these will initialize the database.
             * However, we need to also initialize the database here before
             * calling IN.findEntry below, since that method requires a
             * database to get the key comparator.
             */
            fullBIN.setDatabase(dbImpl);

            /*
             * The BIN's lastFullLsn is set here, while its lastLoggedLsn is
             * set by postFetchInit or postRecoveryInit.
             */
            fullBIN.setLastFullLsn(lastFullLsn);

            /* Process each delta. */
            for (int i = 0; i < deltas.size(); i++) {
                final DeltaInfo info = deltas.get(i);

                /*
                 * The BINDelta holds the authoritative version of each entry.
                 * In all cases, its entry should supersede the entry in the
                 * full BIN.  This is true even if the BIN Delta's entry is
                 * knownDeleted or if the full BIN's version is knownDeleted.
                 * Therefore we use the flavor of findEntry that will return a
                 * knownDeleted entry if the entry key matches (i.e. true,
                 * false) but still indicates exact matches with the return
                 * index.  findEntry only returns deleted entries if third arg
                 * is false, but we still need to know if it's an exact match
                 * or not so indicateExact is true.
                 */
                int foundIndex = fullBIN.findEntry(info.getKey(), true, false);
                if (foundIndex >= 0 &&
                    (foundIndex & IN.EXACT_MATCH) != 0) {
                    foundIndex &= ~IN.EXACT_MATCH;

                    /*
                     * The entry exists in the full version, update it with the
                     * delta info.
                     */
                    if (info.isKnownDeleted()) {
                        fullBIN.setKnownDeleted(foundIndex);
                    } else {
                        fullBIN.updateEntry
                            (foundIndex, info.getLsn(), info.getState());
                    }
                } else {

                    /*
                     * The entry doesn't exist, insert the delta entry.  We
                     * insert the entry even when it is known or pending
                     * deleted, since the deleted (and dirty) entry will be
                     * needed to log the next delta. [#20737]
                     */
                    final ChildReference entry =
                        new ChildReference(null,
                                           info.getKey(),
                                           info.getLsn(),
                                           info.getState());
                    final boolean insertOk = fullBIN.insertEntry(entry);
                    assert insertOk;
                }
            }

            /*
             * Reset the generation to 0, all this manipulation might have
             * driven it up.
             */
            fullBIN.setGeneration(0);

            /*
             * The applied deltas will leave some slots dirty, which is
             * necessary as a record of changes that will be included in the
             * next delta.  However, the BIN itself should not be dirty,
             * because this delta is a persistent record of those changes.
             */
            fullBIN.setDirty(false);
        } finally {
            fullBIN.releaseLatch();
        }
    }

    /*
     * Logging support
     */

    /*
     * @see Loggable#getLogSize()
     */
    public int getLogSize() {
        int numDeltas = deltas.size();
        int size =
            dbId.getLogSize() +
            LogUtils.getPackedLongLogSize(lastFullLsn) +
            LogUtils.getPackedLongLogSize(prevDeltaLsn) +
            LogUtils.getPackedIntLogSize(numDeltas);

        for (int i = 0; i < numDeltas; i++) {
            DeltaInfo info = deltas.get(i);
            size += info.getLogSize();
        }

        return size;
    }

    /*
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        dbId.writeToLog(logBuffer);
        LogUtils.writePackedLong(logBuffer, lastFullLsn);
        LogUtils.writePackedLong(logBuffer, prevDeltaLsn);
        LogUtils.writePackedInt(logBuffer, deltas.size());

        for (int i = 0; i < deltas.size(); i++) {
            DeltaInfo info = deltas.get(i);
            info.writeToLog(logBuffer);
        }
    }

    /*
     * @see Loggable#readFromLog()
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        dbId.readFromLog(itemBuffer, entryVersion);
        lastFullLsn = LogUtils.readLong(itemBuffer, (entryVersion < 6));
        if (entryVersion >= 8) {
            prevDeltaLsn = LogUtils.readPackedLong(itemBuffer);
        }
        int numDeltas = LogUtils.readInt(itemBuffer, (entryVersion < 6));

        for (int i=0; i < numDeltas; i++) {
            DeltaInfo info = new DeltaInfo();
            info.readFromLog(itemBuffer, entryVersion);
            deltas.add(info);
        }

        /* Use minimum memory. */
        deltas.trimToSize();
    }

    /*
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        dbId.dumpLog(sb, verbose);
        sb.append("<lastFullLsn>");
        sb.append(DbLsn.getNoFormatString(lastFullLsn));
        sb.append("</lastFullLsn>");
        sb.append("<prevDeltaLsn>");
        sb.append(DbLsn.getNoFormatString(prevDeltaLsn));
        sb.append("</prevDeltaLsn>");
        sb.append("<deltas size=\"").append(deltas.size()).append("\"/>");
        for (int i = 0; i < deltas.size(); i++) {
            DeltaInfo info = deltas.get(i);
            info.dumpLog(sb, verbose);
        }
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
     * Returns the number of bytes occupied by this object.  Deltas are not
     * stored in the Btree, but they are budgeted during a SortedLSNTreeWalker
     * run.
     */
    public long getMemorySize() {
        long size = MemoryBudget.BINDELTA_OVERHEAD +
                    MemoryBudget.ARRAYLIST_OVERHEAD +
                    MemoryBudget.objectArraySize(deltas.size());
        for (DeltaInfo info : deltas) {
            size += info.getMemorySize();
        }
        return size;
    }
}
