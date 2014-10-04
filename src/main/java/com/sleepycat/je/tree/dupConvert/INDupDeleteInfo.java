/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree.dupConvert;

import java.nio.ByteBuffer;

import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.tree.Key;

/**
 * INDupDeleteInfo encapsulates the information logged about the removal of a
 * child from a duplicate IN during IN compression.
 *
 * Obsolete in log version 8, only used by DupConvert and some log readers.
 */
public class INDupDeleteInfo implements Loggable {

    private long deletedNodeId;
    private byte[] deletedMainKey;
    private byte[] deletedDupKey;
    private final DatabaseId dbId;
    private boolean dupRootDeletion;

    /**
     * Used by logging system only.
     */
    public INDupDeleteInfo() {
        dbId = new DatabaseId();
    }

    /*
     * Accessors.
     */
    public long getDeletedNodeId() {
        return deletedNodeId;
    }

    public byte[] getDeletedMainKey() {
        return deletedMainKey;
    }

    public byte[] getDeletedDupKey() {
        return deletedDupKey;
    }

    public DatabaseId getDatabaseId() {
        return dbId;
    }

    /**
     * Returns true if we are certain that this log entry reflects deletion of
     * a DIN root.  Returns false if it may or may not be a DIN root.  [#18663]
     */
    public boolean isDupRootDeletion() {
        return dupRootDeletion;
    }

    /*
     * Logging support for writing.
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        throw EnvironmentFailureException.unexpectedState();
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        boolean unpacked = (entryVersion < 6);
        deletedNodeId = LogUtils.readLong(itemBuffer, unpacked);
        deletedMainKey =
            LogUtils.readByteArray(itemBuffer, unpacked);
        deletedDupKey = LogUtils.readByteArray(itemBuffer, unpacked);
        dbId.readFromLog(itemBuffer, entryVersion);

        /*
         * This log entry is only logged for dup root deletion, starting in
         * JE 2.1.  We can't distinguish JE 2.1 through 3.2 using the log
         * version, so we are only certain that this is a dup root deletion for
         * version 6 (JE 3.3) and above. [#18663]
         */
        dupRootDeletion = (entryVersion >= 6);
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<INDupDeleteEntry node=\"").append(deletedNodeId);
        sb.append("\">");
        sb.append(Key.dumpString(deletedMainKey, 0));
        sb.append(Key.dumpString(deletedDupKey, 0));
        dbId.dumpLog(sb, verbose);
        sb.append("</INDupDeleteEntry>");
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
}
