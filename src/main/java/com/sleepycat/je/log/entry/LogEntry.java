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
import com.sleepycat.je.log.LogEntryType;

/**
 * A Log entry allows you to read, write and dump a database log entry.  Each
 * entry may be made up of one or more loggable items.
 *
 * The log entry on disk consists of  a log header defined by LogManager
 * and the specific contents of the log entry.
 */
public interface LogEntry extends Cloneable {

    /**
     * Inform a LogEntry instance of its corresponding LogEntryType.
     */
    public void setLogType(LogEntryType entryType);

    /**
     * @return the type of log entry
     */
    public LogEntryType getLogType();

    /**
     * Read in a log entry.
     */
    public void readEntry(EnvironmentImpl envImpl,
                          LogEntryHeader header,
                          ByteBuffer entryBuffer);

    /**
     * Print out the contents of an entry.
     */
    public StringBuilder dumpEntry(StringBuilder sb, boolean verbose);

    /**
     * @return the first item of the log entry
     */
    public Object getMainItem();

    /**
     * Construct a complete item from a item entry, fetching additional log
     * entries as needed. For a BINDeltaLogEntry, fetches the full BIN and
     * merges the delta information.
     */
    public Object getResolvedItem(DatabaseImpl dbImpl);

    /**
     * @return the ID of the database containing this entry, or null if this
     * entry type is not part of a database.
     */
    public DatabaseId getDbId();

    /**
     * @return return the transaction id if this log entry is transactional,
     * 0 otherwise.
     */
    public long getTransactionId();

    /**
     * @return size of byte buffer needed to store this entry.
     */
    public int getSize();

    /**
     * @return total size of last logged entry, or zero if unknown.  The last
     * logged size is known for LNs, and is used for obsolete size counting.
     */
    public int getLastLoggedSize();

    /**
     * Serialize this object into the buffer.
     * @param logBuffer is the destination buffer
     */
    public void writeEntry(LogEntryHeader header, ByteBuffer logBuffer);

    /**
     * Returns true if this item should be counted as obsolete when logged.
     * This currently applies to deleted LNs only.
     */
    public boolean isDeleted();

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     */
    public void postLogWork(LogEntryHeader header, long justLoggedLsn);

    /**
     * @return a shallow clone.
     */
    public LogEntry clone();

    /**
     * @return true if these two log entries are logically the same.
     * Used for replication.
     */
    public boolean logicalEquals(LogEntry other);

    /**
     * Dump the contents of the log entry that are interesting for
     * replication.
     */
    public void dumpRep(StringBuilder sb);
}
