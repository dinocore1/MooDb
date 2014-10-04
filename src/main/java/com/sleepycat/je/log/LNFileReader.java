/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.log;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import javax.transaction.xa.Xid;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.log.entry.NameLNLogEntry;
import com.sleepycat.je.txn.RollbackEnd;
import com.sleepycat.je.txn.RollbackStart;
import com.sleepycat.je.txn.TxnAbort;
import com.sleepycat.je.txn.TxnCommit;
import com.sleepycat.je.txn.TxnPrepare;

/**
 * LNFileReader scans log files for LNs. Also, if it's going backwards for the
 * undo phase in recovery, it reads transaction commit entries.
 */
public class LNFileReader extends FileReader {

    /*
     * targetEntryMap maps DbLogEntryTypes to log entries. We use this
     * collection to find the right LogEntry instance to read in the current
     * entry.
     */
    protected Map<LogEntryType, LogEntry> targetEntryMap;
    protected LogEntry targetLogEntry;

    private long ckptEnd;

    /**
     * Create this reader to start at a given LSN.
     * @param env The relevant EnvironmentImpl
     * @param readBufferSize buffer size in bytes for reading in log
     * @param startLsn where to start in the log
     * @param redo If true, we're going to go forward from
     *             the start LSN to the end of the log. If false, we're going
     *             backwards from the end of the log to the start LSN.
     * @param finishLsn the last LSN to read in the log. May be null if we
     *  want to read to the end of the log.
     * @param endOfFileLsn the virtual LSN that marks the end of the log. (The
     *  one off the end of the log). Only used if we're reading backwards.
     *  Different from the startLsn because the startLsn tells us where the
     *  beginning of the start entry is, but not the length/end of the start
     *  entry. May be null if we're going foward.
     */
    public LNFileReader(EnvironmentImpl env,
                        int readBufferSize,
                        long startLsn,
                        boolean redo,
                        long endOfFileLsn,
                        long finishLsn,
                        Long singleFileNum,
                        long ckptEnd)
        throws DatabaseException {

        super(env, readBufferSize, redo /*forward*/, startLsn,
              singleFileNum, endOfFileLsn, finishLsn);

        this.ckptEnd = ckptEnd;
        targetEntryMap = new HashMap<LogEntryType, LogEntry>();
    }

    public void addTargetType(LogEntryType entryType)
        throws DatabaseException {

        targetEntryMap.put(entryType, entryType.getNewLogEntry());
    }

    /**
     * @return true if this is a transactional LN or Locker Commit entry.
     */
    @Override
    protected boolean isTargetEntry() {

        if (currentEntryHeader.getProvisional().isProvisional
            (getLastLsn(), ckptEnd)) {
            /* Skip provisionial entries */
            targetLogEntry = null;
        } else {
            LogEntryType fromLogType =
                new LogEntryType(currentEntryHeader.getType());

            /* Is it a target entry? */
            targetLogEntry = targetEntryMap.get(fromLogType);
        }
        return (targetLogEntry != null);
    }

    /**
     * This reader instantiates an LN and key for every LN entry.
     */
    @Override
    protected boolean processEntry(ByteBuffer entryBuffer)
        throws DatabaseException {

        targetLogEntry.readEntry(envImpl, currentEntryHeader, entryBuffer);
        return true;
    }

    /**
     * @return true if the last entry was an LN.
     */
    public boolean isLN() {
        return (targetLogEntry instanceof LNLogEntry);
    }

    /**
     * Get the last LN log entry seen by the reader.  Note that
     * LNLogEntry.postFetchInit must be called before calling certain
     * LNLogEntry methods.
     */
    public LNLogEntry getLNLogEntry() {
        return (LNLogEntry) targetLogEntry;
    }

    /**
     * Returns a NameLNLogEntry if the LN is a NameLN, or null otherwise.
     */
    public NameLNLogEntry getNameLNLogEntry() {
        return (targetLogEntry instanceof NameLNLogEntry) ?
            ((NameLNLogEntry) targetLogEntry) :
            null;
    }

    public boolean isLNDeleted() {
        return ((LNLogEntry) targetLogEntry).isDeleted();
    }

    /**
     * Get the last databaseId seen by the reader.
     */
    public DatabaseId getDatabaseId() {
        return ((LNLogEntry) targetLogEntry).getDbId();
    }

    /**
     * @return the transaction id of the current entry.
     */
    public Long getTxnId() {
        return ((LNLogEntry) targetLogEntry).getTxnId();
    }

    /*
     * @return true if the last entry was a TxnPrepare record.
     */
    public boolean isPrepare() {
        return (targetLogEntry.getMainItem() instanceof TxnPrepare);
    }

    /**
     * Get the last txn prepare id seen by the reader.
     */
    public long getTxnPrepareId() {
        return ((TxnPrepare) targetLogEntry.getMainItem()).getId();
    }

    /**
     * Get the last txn prepare Xid seen by the reader.
     */
    public Xid getTxnPrepareXid() {
        return ((TxnPrepare) targetLogEntry.getMainItem()).getXid();
    }

    /*
     * @return true if the last entry was a TxnCommit record.
     */
    public boolean isCommit() {
        return (targetLogEntry.getMainItem() instanceof TxnCommit);
    }

    /*
     * @return true if the last entry was a RollbackStart record.
     */
    public boolean isRollbackStart() {
        return (targetLogEntry.getMainItem() instanceof RollbackStart);
    }

    /*
     * @return true if the last entry was a RollbackStart record.
     */
    public boolean isRollbackEnd() {
        return (targetLogEntry.getMainItem() instanceof RollbackEnd);
    }

    public Object getMainItem() {
        return targetLogEntry.getMainItem();
    }

    /*
     * For error message.
     */
    public String dumpCurrentHeader() {
        return currentEntryHeader.toString();
    }

    /*
     * @return true if the last entry was a TxnAbort record.
     */
    public boolean isAbort() {
        return (targetLogEntry.getMainItem() instanceof TxnAbort);
    }

    /**
     * Get the last txn abort id seen by the reader.
     */
    public long getTxnAbortId() {
        return ((TxnAbort) targetLogEntry.getMainItem()).getId();
    }

    /**
     * Get the last txn commit id seen by the reader.
     */
    public long getTxnCommitId() {
        return ((TxnCommit) targetLogEntry.getMainItem()).getId();
    }

    /**
     * Get last abort LSN seen by the reader (may be null).
     */
    public long getAbortLsn() {
        return ((LNLogEntry) targetLogEntry).getAbortLsn();
    }

    /**
     * Get last abort known deleted seen by the reader.
     */
    public boolean getAbortKnownDeleted() {
        return ((LNLogEntry) targetLogEntry).getAbortKnownDeleted();
    }

    public boolean isInvisible() {
        return currentEntryHeader.isInvisible();
    }

    /**
     * Return the type of this log entry.
     */
    public byte getLogEntryType() {
        return currentEntryHeader.getType();
    }

    /**
     * Return the VLSN if this entry is in replicated stream.
     */
    public long getVLSN() {
        assert entryIsReplicated();
        return currentEntryHeader.getVLSN().getSequence();
    }
}
