/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.dbi;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.utilint.StringUtils;

/**
 * DatabaseImpl Ids are wrapped in a class so they can be logged.
 */
public class DatabaseId implements Comparable<DatabaseId>, Loggable {

    /**
     * The unique id of this database.
     */
    private long id;

    /**
     *
     */
    public DatabaseId(long id) {
        this.id = id;
    }

    /**
     * Uninitialized database id, for logging.
     */
    public DatabaseId() {
    }

    /**
     * @return id value
     */
    public long getId() {
        return id;
    }

    /**
     * @return id as bytes, for use as a key
     */
    public byte[] getBytes()
        throws DatabaseException {

        return StringUtils.toUTF8(toString());
    }

    /**
     * Compare two DatabaseImpl Id's.
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DatabaseId)) {
            return false;
        }

        return ((DatabaseId) obj).id == id;
    }

    public int hashCode() {
        return (int) id;
    }

    public String toString() {
        return Long.toString(id);
    }

    /**
     * see Comparable#compareTo
     */
    public int compareTo(DatabaseId o) {
        if (o == null) {
            throw EnvironmentFailureException.unexpectedState("null arg");
        }

        if (id == o.id) {
            return 0;
        } else if (id > o.id) {
            return 1;
        } else {
            return -1;
        }
    }

    /*
     * Logging support.
     */

    /**
     * @see Loggable#getLogSize
     */
    public int getLogSize() {
        return LogUtils.getPackedLongLogSize(id);
    }

    /**
     * @see Loggable#writeToLog
     */
    public void writeToLog(ByteBuffer logBuffer) {
        LogUtils.writePackedLong(logBuffer, id);
    }

    /**
     * @see Loggable#readFromLog
     */
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {
        if (entryVersion < 6) {
            id = LogUtils.readInt(itemBuffer);
        } else {
            id = LogUtils.readPackedLong(itemBuffer);
        }
    }

    /**
     * @see Loggable#dumpLog
     */
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append("<dbId id=\"");
        sb.append(id);
        sb.append("\"/>");
    }

    /**
     * @see Loggable#getTransactionId
     */
    public long getTransactionId() {
        return 0;
    }

   /**
     * @see Loggable#logicalEquals
     */
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof DatabaseId))
            return false;

        return id == ((DatabaseId) other).id;
    }
}
