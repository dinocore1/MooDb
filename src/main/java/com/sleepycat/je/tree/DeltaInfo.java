/*
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.utilint.DbLsn;

/**
 * DeltaInfo holds the delta for one BIN entry in a partial BIN log entry.
 * The data here is all that we need to update a BIN to its proper state.
 */
public class DeltaInfo implements Loggable {
    private byte[] key;
    private long lsn;
    private byte state;

    DeltaInfo(byte[] key, long lsn, byte state) {
        this.key = key;
        this.lsn = lsn;
        this.state = state;
    }

    /**
     * For reading from the log only.
     *
     * Is public for Sizeof.
     */
    public DeltaInfo() {
        lsn = DbLsn.NULL_LSN;
    }

    /*
     * @see Loggable#getLogSize()
     */
    public int getLogSize() {
        return
            LogUtils.getByteArrayLogSize(key) +
            LogUtils.getPackedLongLogSize(lsn) + // LSN
            1; // state
    }

    /*
     * @see Loggable#writeToLog(java.nio.ByteBuffer)
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writeByteArray(logBuffer, key);
        LogUtils.writePackedLong(logBuffer, lsn);
        logBuffer.put(state);
    }

    /*
     * @seeLoggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        key = LogUtils.readByteArray(itemBuffer, unpacked);
        lsn = LogUtils.readLong(itemBuffer, unpacked);
        state = itemBuffer.get();
    }

    /*
     * @see Loggable#dumpLog(java.lang.StringBuilder)
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(Key.dumpString(key, 0));
            sb.append(DbLsn.toString(lsn));
        IN.dumpDeletedState(sb, state);
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
     * @return the Key.
     */
    byte[] getKey() {
        return key;
    }

    /**
     * @return the state flags.
     */
    byte getState() {
        return state;
    }

    /**
     * @return true if this is known to be deleted.
     */
    boolean isKnownDeleted() {
        return IN.isStateKnownDeleted(state);
    }

    /**
     * @return the LSN.
     */
    long getLsn() {
        return lsn;
    }

    /**
     * Returns the number of bytes occupied by this object.  Deltas are not
     * stored in the Btree, but they are budgeted during a SortedLSNTreeWalker
     * run.
     */
    long getMemorySize() {
        return MemoryBudget.DELTAINFO_OVERHEAD +
               MemoryBudget.byteArraySize(key.length);
    }
}
