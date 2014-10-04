/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log.entry;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryHeader;
import com.sleepycat.je.log.LogEntryType;

/**
 * A Log entry allows you to read, write and dump a database log entry.  Each
 * entry may be made up of one or more loggable items.
 *
 * The log entry on disk consists of
 *  a. a log header defined by LogManager
 *  b. a VLSN, if this entry type requires it, and replication is on.
 *  c. the specific contents of the log entry.
 *
 * This class encompasses (b and c).
 */
public abstract class BaseEntry implements LogEntry {

    /*
     * These fields are transient and are not persisted to the log
     */

    /*
     * Constructor used to create log entries when reading.
     */
    private final Constructor<?> noArgsConstructor;

    /*
     * Attributes of the entry type may be used to conditionalizing the reading
     * and writing of the entry.
     */
    LogEntryType entryType;

    /**
     * Constructor to read an entry. The logEntryType must be set later,
     * through setLogType().
     */
    BaseEntry(Class<?> logClass) {
        noArgsConstructor = getNoArgsConstructor(logClass);
    }

    static Constructor<?> getNoArgsConstructor(Class<?> logClass) {
        try {
            return logClass.getConstructor((Class<?>[]) null);
        } catch (SecurityException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (NoSuchMethodException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * @return a new instance of the class used to create the log entry.
     */
    Object newInstanceOfType() {
        return newInstanceOfType(noArgsConstructor);
    }

    static Object newInstanceOfType(Constructor<?> noArgsConstructor) {
        try {
            return noArgsConstructor.newInstance((Object[]) null);
        } catch (IllegalAccessException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (InstantiationException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (IllegalArgumentException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (InvocationTargetException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Constructor to write an entry.
     */
    BaseEntry() {
        noArgsConstructor = null;
    }

    /**
     * Inform a BaseEntry instance of its corresponding LogEntryType.
     */
    public void setLogType(LogEntryType entryType) {
        this.entryType = entryType;
    }

    /**
     * @return the type of log entry
     */
    public LogEntryType getLogType() {
        return entryType;
    }

    /**
     * By default, this log entry is complete and does not require fetching
     * additional entries.  This method is overridden by BINDeltaLogEntry.
     */
    public Object getResolvedItem(DatabaseImpl dbImpl) {
        return getMainItem();
    }

    /**
     * By default, return zero because the last logged size is unknown.  This
     * method is overridden by LNLogEntry.
     */
    public int getLastLoggedSize() {
        return 0;
    }

    /**
     * Returns true if this item should be counted as obsoleted when logged.
     * This currently applies to deleted LNs only.
     */
    public boolean isDeleted() {
        return false;
    }

    /**
     * Do any processing we need to do after logging, while under the logging
     * latch.
     * @throws DatabaseException from subclasses.
     */
    public void postLogWork(@SuppressWarnings("unused") LogEntryHeader header,
                            @SuppressWarnings("unused") long justLoggedLsn) {

        /* by default, do nothing. */
    }

    public void postFetchInit(DatabaseImpl dbImpl) {
    }

    public abstract StringBuilder dumpEntry(StringBuilder sb, boolean verbose);

    @Override
    public LogEntry clone() {

        try {
            return (LogEntry) super.clone();
        } catch (CloneNotSupportedException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        dumpEntry(sb, true);
        return sb.toString();
    }
}
