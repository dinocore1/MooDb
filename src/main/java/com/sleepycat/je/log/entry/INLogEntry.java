/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log.entry;

import java.nio.ByteBuffer;

import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.tree.IN;
import com.sleepycat.je.utilint.DbLsn;

/**
 * INLogEntry embodies all IN log entries.
 * On disk, an IN log entry contains (pre version 6)
 * <pre>
 *        IN
 *        database id
 *        obsolete LSN  -- in version 2
 *
 * (version 6)
 *        database id
 *        obsolete LSN
 *        IN
 * </pre>
 */
public class INLogEntry extends BaseEntry
    implements LogEntry, INContainingEntry {

    /*
     * Persistent fields in an IN entry.
     */
    private IN in;
    private DatabaseId dbId;

    /*
     * obsoleteFile was added in version 1, and changed to obsoleteLsn in
     * version 2.  If the offset is zero in the LSN, we read a version 1 entry
     * since only the file number was stored.  In version 8, we renamed
     * obsoleteLsn to prevFullLsn and added prevDeltaLsn.
     */
    private long prevFullLsn;
    private long prevDeltaLsn;

    /**
     * Construct a log entry for reading.
     */
    public INLogEntry(Class<? extends IN> INClass) {
        super(INClass);
    }

    /**
     * Construct a log entry for writing to the log.
     */
    public INLogEntry(IN in) {
        setLogType(in.getLogType());
        this.in = in;
        this.dbId = in.getDatabase().getId();
        this.prevFullLsn = in.getLastFullVersion();
        this.prevDeltaLsn = in.getLastDeltaVersion();
    }

    /*
     * Read support
     */

    /**
     * Read in an IN entry.
     */
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer) {

        int logVersion = header.getVersion();
        boolean version6OrLater = (logVersion >= 6);
        if (version6OrLater) {
            dbId = new DatabaseId();
            dbId.readFromLog(entryBuffer, logVersion);
            prevFullLsn = LogUtils.readLong(entryBuffer, false/*unpacked*/);
            if (logVersion >= 8) {
                prevDeltaLsn = LogUtils.readPackedLong(entryBuffer);
            }
        }
        /* Read IN. */
        in = (IN) newInstanceOfType();
        in.readFromLog(entryBuffer, logVersion);
        if (!version6OrLater) {
            dbId = new DatabaseId();
            dbId.readFromLog(entryBuffer, logVersion);
        }
        if (logVersion < 1) {
            prevFullLsn = DbLsn.NULL_LSN;
        } else if (logVersion == 1) {
            long fileNum = LogUtils.readUnsignedInt(entryBuffer);
            if (fileNum == 0xffffffffL) {
                prevFullLsn = DbLsn.NULL_LSN;
            } else {
                prevFullLsn = DbLsn.makeLsn(fileNum, 0);
            }
        } else if (!version6OrLater) {
            prevFullLsn = LogUtils.readLong(entryBuffer, true/*unpacked*/);
        }
    }

    public long getPrevFullLsn() {
        return prevFullLsn;
    }

    public long getPrevDeltaLsn() {
        return prevDeltaLsn;
    }

    /**
     * Print out the contents of an entry.
     */
    @Override
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose) {
        in.dumpLog(sb, verbose);
        dbId.dumpLog(sb, verbose);
        if (prevFullLsn != DbLsn.NULL_LSN) {
            sb.append("<prevFullLsn>");
            sb.append(DbLsn.getNoFormatString(prevFullLsn));
            sb.append("</prevFullLsn>");
        }
        if (prevDeltaLsn != DbLsn.NULL_LSN) {
            sb.append("<prevDeltaLsn>");
            sb.append(DbLsn.getNoFormatString(prevDeltaLsn));
            sb.append("</prevDeltaLsn>");
        }
        return sb;
    }

    /** Never replicated. */
    public void dumpRep(@SuppressWarnings("unused") StringBuilder sb) {
    }

    /**
     * @return the item in the log entry
     */
    public Object getMainItem() {
        return in;
    }

    /**
     * @see LogEntry#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

    /*
     * Writing support
     */

    /**
     */
    public int getSize() {
        return (in.getLogSize() +
                dbId.getLogSize() +
                LogUtils.getPackedLongLogSize(prevFullLsn) +
                LogUtils.getPackedLongLogSize(prevDeltaLsn));
    }

    /**
     * @see LogEntry#writeEntry
     */
    public void writeEntry(@SuppressWarnings("unused") LogEntryHeader header,
                           ByteBuffer destBuffer) {
        dbId.writeToLog(destBuffer);
        LogUtils.writePackedLong(destBuffer, prevFullLsn);
        LogUtils.writePackedLong(destBuffer, prevDeltaLsn);
        in.writeToLog(destBuffer);
    }

    /*
     * Access the in held within the entry.
     * @see INContainingEntry#getIN()
     */
    public IN getIN(@SuppressWarnings("unused") DatabaseImpl dbImpl) {
        return in;
    }

    /**
     * @return the IN's node ID.
     */
    public long getNodeId() {
        return in.getNodeId();
    }

    /**
     * @see INContainingEntry#getDbId()
     */
    public DatabaseId getDbId() {
        return dbId;
    }

    /**
     * @see LogEntry#logicalEquals
     *
     * INs from two different environments are never considered equal,
     * because they have lsns that are environment-specific.
     */
    public boolean logicalEquals(@SuppressWarnings("unused") LogEntry other) {
        return false;
    }
}
