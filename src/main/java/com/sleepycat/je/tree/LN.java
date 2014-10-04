/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.LogContext;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogItem;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;
import com.sleepycat.je.txn.WriteLockInfo;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.SizeofMarker;
import com.sleepycat.je.utilint.VLSN;

/**
 * An LN represents a Leaf Node in the JE tree.
 */
public class LN extends Node implements Loggable {
    private static final String BEGIN_TAG = "<ln>";
    private static final String END_TAG = "</ln>";

    private byte[] data;

    /*
     * Flags: bit fields
     *
     * -Dirty means that the in-memory version is not present on disk.
     * -The last logged bits store the total size of the last logged entry.
     */
    private static final int DIRTY_BIT = 0x80000000;
    private static final int CLEAR_DIRTY_BIT = ~DIRTY_BIT;
    private static final int LAST_LOGGED_SIZE_MASK = 0x7FFFFFFF;
    private static final int CLEAR_LAST_LOGGED_SIZE = ~LAST_LOGGED_SIZE_MASK;
    private int flags; // not persistent

    /**
     * Create an empty LN, to be filled in from the log.  If VLSNs are
     * preserved for this environment, a VersionedLN will be created instead.
     */
    public LN() {
        this.data = null;
    }

    /**
     * Create a new LN from a byte array.  Pass a null byte array to create a
     * deleted LN.
     */
    public static LN makeLN(EnvironmentImpl envImpl, byte[] dataParam) {
        if (envImpl.getPreserveVLSN()) {
            return new VersionedLN(dataParam);
        }
        return new LN(dataParam);
    }

    LN(byte[] data) {
        if (data == null) {
            this.data = null;
        } else {
            init(data, 0, data.length);
        }
        setDirty();
    }

    /**
     * Create a new LN from a DatabaseEntry.
     */
    public static LN makeLN(EnvironmentImpl envImpl, DatabaseEntry dbt) {
        if (envImpl.getPreserveVLSN()) {
            return new VersionedLN(dbt);
        }
        return new LN(dbt);
    }

    LN(DatabaseEntry dbt) {
        byte[] dat = dbt.getData();
        if (dat == null) {
            data = null;
        } else if (dbt.getPartial()) {
            init(dat,
                 dbt.getOffset(),
                 dbt.getPartialOffset() + dbt.getSize(),
                 dbt.getPartialOffset(),
                 dbt.getSize());
        } else {
            init(dat, dbt.getOffset(), dbt.getSize());
        }
        setDirty();
    }

    /** For Sizeof. */
    public LN(SizeofMarker marker, DatabaseEntry dbt) {
        this(dbt);
    }

    private void init(byte[] data, int off, int len, int doff, int dlen) {
        if (len == 0) {
            this.data = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            this.data = new byte[len];
            System.arraycopy(data, off, this.data, doff, dlen);
        }
    }

    private void init(byte[] data, int off, int len) {
        init(data, off, len, 0, len);
    }

    public byte[] getData() {
        return data;
    }

    public boolean isDeleted() {
        return (data == null);
    }

    @Override
    public boolean isLN() {
        return true;
    }

    void makeDeleted() {
        data = null;
    }

    public boolean isDirty() {
        return ((flags & DIRTY_BIT) != 0);
    }

    public void setDirty() {
        flags |= DIRTY_BIT;
    }

    private void clearDirty() {
        flags &= CLEAR_DIRTY_BIT;
    }

    /**
     * Called by CursorImpl to get the record version.
     *
     * If VLSNs are not preserved for this environment, returns -1 which is the
     * sequence for VLSN.NULL_VLSN.
     *
     * If VLSNs are preserved for this environment, this method is overridden
     * by VersionedLN which returns the VLSN sequence.
     */
    public long getVLSNSequence() {
        return VLSN.NULL_VLSN.getSequence();
    }

    /**
     * Called by LogManager after writing an LN with a newly assigned VLSN, and
     * called by LNLogEntry after reading the LN with the VLSN from the log
     * entry header.
     *
     * If VLSNs are not preserved for this environment, does nothing.
     *
     * If VLSNs are preserved for this environment, this method is overridden
     * by VersionedLN which stores the VLSN sequence.
     */
    public void setVLSNSequence(long seq) {
        /* Do nothing. */
    }

    /*
     * If you get to an LN, this subtree isn't valid for delete. True, the LN
     * may have been deleted, but you can't be sure without taking a lock, and
     * the validate -subtree-for-delete process assumes that bin compressing
     * has happened and there are no committed, deleted LNS hanging off the
     * BIN.
     */
    @Override
    boolean isValidForDelete() {
        return false;
    }

    /**
     * Returns true by default, but is overridden by MapLN to prevent eviction
     * of open databases.  This method is meant to be a fast but not guaranteed
     * check and is used during selection of BINs for LN stripping.  [#13415]
     */
    boolean isEvictableInexact() {
        return true;
    }

    /**
     * Returns true by default, but is overridden by MapLN to prevent eviction
     * of open databases.  This method is meant to be a guaranteed check and is
     * used after a BIN has been selected for LN stripping but before actually
     * stripping an LN. [#13415]
     * @throws DatabaseException from subclasses.
     */
    boolean isEvictable(long lsn)
        throws DatabaseException {

        return true;
    }

    public void delete() {
        makeDeleted();
        setDirty();
    }

    public void modify(byte[] newData) {
        data = newData;
        setDirty();
    }

    /**
     * Sets data to empty and returns old data.  Called when converting an old
     * format LN in a duplicates DB.
     */
    public byte[] setEmpty() {
        final byte[] retVal = data;
        data = Key.EMPTY_KEY;
        return retVal;
    }

    /**
     * Add yourself to the in memory list if you're a type of node that should
     * belong.
     */
    @Override
    void rebuildINList(INList inList) {
        /*
         * Don't add, LNs don't belong on the list.
         */
    }

    /**
     * No need to do anything, stop the search.
     */
    @Override
    void accountForSubtreeRemoval(INList inList,
                                  LocalUtilizationTracker localTracker) {
        /* Don't remove, LNs not on this list. */
    }

    /**
     * No need to do anything, stop the search.
     */
    @Override
    void accountForDeferredWriteSubtreeRemoval(INList inList,
                                               IN subtreeParent) {
        /* Don't remove, LNs not on this list. */
    }

    /**
     * Compute the approximate size of this node in memory for evictor
     * invocation purposes.
     */
    @Override
    public long getMemorySizeIncludedByParent() {
        int size = MemoryBudget.LN_OVERHEAD;
        if (data != null) {
            size += MemoryBudget.byteArraySize(data.length);
        }
        return size;
    }

    /**
     * Release the memory budget for any objects referenced by this
     * LN. For now, only release treeAdmin memory, because treeMemory
     * is handled in aggregate at the IN level. Over time, transition
     * all of the LN's memory budget to this, so we update the memory
     * budget counters more locally. Called when we are releasing a LN
     * for garbage collection.
     */
    public void releaseMemoryBudget() {
        // nothing to do for now, no treeAdmin memory
    }

    /*
     * Dumping
     */

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder self = new StringBuilder();
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(beginTag());
            self.append('\n');
        }

        self.append(super.dumpString(nSpaces + 2, true));
        self.append('\n');
        if (data != null) {
            self.append(TreeUtils.indent(nSpaces+2));
            self.append("<data>");
            self.append(Key.DUMP_TYPE.dumpByteArray(data));
            self.append("</data>");
            self.append('\n');
        }
        if (dumpTags) {
            self.append(TreeUtils.indent(nSpaces));
            self.append(endTag());
        }
        return self.toString();
    }

    /*
     * Logging Support
     */

    /**
     * Convenience logging method.  See logInternal.
     *
     * For a deferred-write database, the logging will not actually occur and
     * a transient LSN may be returned.
     */
    public long optionalLog(EnvironmentImpl envImpl,
                            DatabaseImpl dbImpl,
                            byte[] key,
                            byte[] oldKey,
                            long oldLsn,
                            Locker locker,
                            WriteLockInfo writeLockInfo,
                            ReplicationContext repContext)
        throws DatabaseException {

        if (dbImpl.isDeferredWriteMode()) {
            return assignTransientLsn(envImpl, dbImpl, oldLsn, locker);
        } else {
            return logInternal(envImpl, dbImpl, key, oldKey, oldLsn, locker,
                               writeLockInfo, false /*backgroundIO*/,
                               false /*provisional*/, repContext);
        }
    }

    /**
     * Convenience logging method.  See logInternal.
     *
     * For a deferred-write database, the logging will not actually occur and
     * a transient LSN may be returned.
     */
    public long optionalLogProvisional(EnvironmentImpl envImpl,
                                       DatabaseImpl dbImpl,
                                       byte[] key,
                                       long oldLsn,
                                       ReplicationContext repContext)
        throws DatabaseException {

        if (dbImpl.isDeferredWriteMode()) {
            return assignTransientLsn(envImpl, dbImpl, oldLsn,
                                      null /*locker*/);
        } else {
            return logInternal(envImpl, dbImpl, key, null /*oldKey*/,
                               oldLsn, null /*locker*/, null /*writeLockInfo*/,
                               false /*backgroundIO*/, true /*provisional*/,
                               repContext);
        }
    }

    /** Convenience logging method.  See logInternal. */
    public long log(EnvironmentImpl envImpl,
                    DatabaseImpl dbImpl,
                    byte[] key,
                    long oldLsn,
                    boolean backgroundIO,
                    ReplicationContext repContext)
        throws DatabaseException {

        return logInternal(envImpl, dbImpl, key, null /*oldKey*/, oldLsn,
                           null /*locker*/, null /*writeLockInfo*/,
                           backgroundIO, false /*provisional*/, repContext);
    }

    /** Convenience logging method.  See logInternal. */
    public long log(EnvironmentImpl envImpl,
                    DatabaseImpl dbImpl,
                    byte[] key,
                    long oldLsn,
                    Locker locker,
                    WriteLockInfo writeLockInfo,
                    ReplicationContext repContext)
        throws DatabaseException {

        return logInternal(envImpl, dbImpl, key, null /*oldKey*/, oldLsn,
                           locker, writeLockInfo, false /*backgroundIO*/,
                           false /*provisional*/, repContext);
    }

    /**
     * Log this LN. Clear dirty bit. Locks the new LSN if a non-null locker is
     * passed.  Whether it's logged as a transactional entry or not depends on
     * the type of locker.
     *
     * WARNING: Be sure to pass null for the locker param if the new LSN should
     * not be locked.
     *
     * @param lnKey is the key of the BIN parent of the LN.
     *
     * @param oldLsn is the LSN of the previous version or NULL_LSN if we are
     * inserting a new record.  Is counted obsolete when this entry is part
     * of a committed transaction, or is non-transactional.  When reusing a BIN
     * slot for a deleted entry, this param should be NULL_LSN; in this case
     * the old LSN was counted obsolete at an earlier time.
     *
     * @param locker if transactional (and non-null), logs a transactional LN
     * log entry; otherwise, log entry is non-transactional.  If non-null, a
     * write lock is acquired for the new LSN after logging.
     *
     * @param writeLockInfo contains abort information that is required for a
     * transactional entry and must be non-null if the locker is transactional,
     * may be null otherwise.  When the new LSN is write-locked, this info is
     * copied to the new lock.  Normally this parameter should be obtained from
     * the prepareForInsert or prepareForUpdate method of
     * CursorImpl.LockStanding.
     */
    private long logInternal(final EnvironmentImpl envImpl,
                             final DatabaseImpl dbImpl,
                             final byte[] key,
                             final byte[] oldKey,
                             final long oldLsn,
                             final Locker locker,
                             final WriteLockInfo writeLockInfo,
                             final boolean backgroundIO,
                             final boolean isProvisional,
                             final ReplicationContext repContext)
        throws DatabaseException {

        if (envImpl.isReadOnly()) {
            /* Returning a NULL_LSN will not allow locking. */
            throw EnvironmentFailureException.unexpectedState
                ("Cannot log LNs in read-only env.");
        }

        LogEntryType entryType;
        long logAbortLsn;
        boolean logAbortKnownDeleted;
        Txn logTxn;
        LogContext context = new LogContext();

        boolean isInsert = (oldLsn == DbLsn.NULL_LSN);

        if (locker != null && locker.isTransactional()) {
            entryType = getLogType(isInsert, true);
            logAbortLsn = writeLockInfo.getAbortLsn();
            logAbortKnownDeleted = writeLockInfo.getAbortKnownDeleted();
            logTxn = locker.getTxnLocker();
            assert logTxn != null;
            context.obsoleteDupsAllowed = locker.isRolledBack();
        } else {
            entryType = getLogType(isInsert, false);
            logAbortLsn = DbLsn.NULL_LSN;
            logAbortKnownDeleted = false;
            logTxn = null;
        }

        LogItem item = new LogItem();
        item.entry = createLogEntry(entryType,
                                    dbImpl,
                                    key,
                                    logAbortLsn,
                                    logAbortKnownDeleted,
                                    logTxn,
                                    repContext);

        /*
         * Always log temporary DB LNs as provisional.  This prevents the
         * possibility of a FileNotFoundException during recovery, since
         * temporary DBs are not checkpointed.  And it speeds recovery --
         * temporary DBs are removed during recovery anyway.
         */
        item.provisional = (dbImpl.isTemporary() || isProvisional) ?
            Provisional.YES :
            Provisional.NO;

        /* Don't count abortLsn as obsolete, this is done during commit. */
        item.oldLsn = (oldLsn != logAbortLsn) ? oldLsn : DbLsn.NULL_LSN;

        item.repContext = repContext;
        context.backgroundIO = backgroundIO;
        context.nodeDb = dbImpl;

        /**
         * Sets the last logged size, if an estimate of the data size is
         * available for this database.  This method is called before updating
         * or deleting an LN, in the case where the old LN is not fetched and
         * so the last logged size is unknown.
         */
        if (!isInsert && getLastLoggedSize() == 0) {
            int dataSizeEstimate = dbImpl.estimateDataSize(oldKey);
            if (dataSizeEstimate >= 0) {
                setLastLoggedSize(item.entry.getSize() -
                                  getLogSize() +
                                  calcLogSize(dataSizeEstimate));
            }
        }

        /* Save obsolete size information to be used during commit. */
        if (logTxn != null && oldLsn == logAbortLsn) {
            writeLockInfo.setAbortInfo(dbImpl, getLastLoggedSize());
        }

        try {
            if (logTxn != null) {

                /*
                 * Writing an LN_TX entry requires looking at the Txn's
                 * lastLoggedTxn.  The Txn may be used by multiple threads so
                 * ensure that the view we get is consistent. [#17204]
                 */
                synchronized (logTxn) {
                    envImpl.getLogManager().log(item, context);
                }
            } else {
                envImpl.getLogManager().log(item, context);
            }
        } catch (DatabaseException e) {
            if (FileManager.continueAfterWriteException()) {

                /*
                 * Test mode.  Ensure txn is aborted. Use an
                 * OperationFailureException that is not exposed in the API.
                 * Create exception to invalidate the txn.  [#15768]
                 */
                if (locker != null) {
                    new LNWriteFailureException(locker, e);
                }
                throw e;
            } else {

                /*
                 * In production mode, if any exception occurs while logging an
                 * LN, ensure that the environment is invalidated.  This will
                 * also ensure that the txn cannot be committed.
                 */
                if (envImpl.isValid()) {
                    throw new EnvironmentFailureException
                        (envImpl,
                         EnvironmentFailureReason.LOG_INCOMPLETE,
                         "LN could not be logged", e);
                } else {
                    throw e;
                }
            }
        }

        /**
         * Lock the new LSN immediately after logging, with the BIN latched.
         * Lock non-blocking, since no contention is possible on the new LSN.
         */
        if (locker != null) {
            final LockResult lockResult = locker.nonBlockingLock
                (item.newLsn, LockType.WRITE, false /*jumpAheadOfWaiters*/,
                 dbImpl);
            assert lockResult.getLockGrant() != LockGrantType.DENIED:
                   DbLsn.getNoFormatString(item.newLsn);
            lockResult.copyWriteLockInfo(writeLockInfo);
        }
 
        clearDirty();
        return item.newLsn;
    }

    static class LNWriteFailureException
        extends OperationFailureException {

        LNWriteFailureException(Locker locker, Exception cause) {
            super(locker, true /*abortOnly*/, null /*message*/, cause);
        }

        private LNWriteFailureException(String message,
                                        LNWriteFailureException cause) {
            super(message, cause);
        }

        @Override
        public OperationFailureException wrapSelf(String msg) {
            return new LNWriteFailureException(msg, this);
        }
    }

    /*
     * Each LN knows what kind of log entry it uses to log itself. Overridden
     * by subclasses.
     */
    LNLogEntry createLogEntry(LogEntryType entryType,
                              DatabaseImpl dbImpl,
                              byte[] key,
                              long logAbortLsn,
                              boolean logAbortKnownDeleted,
                              Txn logTxn,
                              ReplicationContext repContext) {

        return new LNLogEntry(entryType,
                              this,
                              dbImpl.getId(),
                              key,
                              logAbortLsn,
                              logAbortKnownDeleted,
                              logTxn);
    }

    /**
     * @see Node#incFetchStats
     */
    @Override
    public void incFetchStats(EnvironmentImpl envImpl, boolean isMiss) {
        envImpl.getEvictor().incLNFetchStats(isMiss);
    }

    /**
     * @see Node#getGenericLogType
     */
    @Override
    public LogEntryType getGenericLogType() {
        return getLogType(true, false);
    }

    protected LogEntryType getLogType(boolean isInsert,
                                      boolean isTransactional) {
        if (isDeleted()) {
            assert !isInsert;
            return isTransactional ?
                   LogEntryType.LOG_DEL_LN_TRANSACTIONAL :
                   LogEntryType.LOG_DEL_LN;
        }

        if (isInsert) {
            return isTransactional ?
                LogEntryType.LOG_INS_LN_TRANSACTIONAL :
                LogEntryType.LOG_INS_LN;
        }

        return isTransactional ?
            LogEntryType.LOG_UPD_LN_TRANSACTIONAL :
            LogEntryType.LOG_UPD_LN;
    }

    /**
     * The first time we optionally-log an LN in a DeferredWrite database,
     * oldLsn will be NULL_LSN and we'll assign a new transient LSN.  When we
     * do subsequent optional-log operations, the old LSN will be non-null and
     * to conserve transient LSNs we'll continue to use the previously assigned
     * LSN rather than assigning a new one.  And of course, when old LSN is
     * persistent we'll continue to use it.
     *
     * If locker is non-null, this method write-locks the new LSN, whether it
     * has been assigned by this method or not.
     */
    private long assignTransientLsn(EnvironmentImpl envImpl,
                                    DatabaseImpl dbImpl,
                                    long oldLsn,
                                    Locker locker) {
        final long newLsn;
        if (oldLsn != DbLsn.NULL_LSN) {
            newLsn = oldLsn;
        } else {
            newLsn = envImpl.getNodeSequence().getNextTransientLsn();
        }

        /**
         * Lock immediately after assigning a new LSN, with the BIN latched.
         * Lock non-blocking, since no contention is possible on the new LSN.
         */
        if (locker != null) {
            final LockResult lockResult = locker.nonBlockingLock
                (newLsn, LockType.WRITE, false /*jumpAheadOfWaiters*/, dbImpl);
            assert lockResult.getLockGrant() != LockGrantType.DENIED:
                   DbLsn.getNoFormatString(newLsn);
        }

        return newLsn;
    }

    /**
     * Returns the total last logged log size, including the LNLogEntry
     * overhead of this LN when it was last logged and the log entry
     * header.  Used for computing obsolete size when an LNLogEntry is not in
     * hand.
     */
    public int getLastLoggedSize() {
        return flags & LAST_LOGGED_SIZE_MASK;
    }

    /**
     * Saves the last logged size.
     */
    public void setLastLoggedSize(int size) {
        /* Clear the old size and OR in the new size. */
        flags = (flags & CLEAR_LAST_LOGGED_SIZE) | size;
    }

    /**
     * @see Loggable#getLogSize
     */
    @Override
    public int getLogSize() {
        return calcLogSize(isDeleted() ? -1 : data.length);
    }

    /**
     * Calculates log size based on given dataLen, which is negative to
     * calculate the size of a deleted LN.
     */
    private int calcLogSize(int dataLen) {
        int size = super.getLogSize();

        if (dataLen < 0) {
            size += LogUtils.getPackedIntLogSize(-1);
        } else {
            size += LogUtils.getPackedIntLogSize(dataLen);
            size += dataLen;
        }

        return size;
    }

    /**
     * @see Loggable#writeToLog
     */
    @Override
    public void writeToLog(ByteBuffer logBuffer) {
        super.writeToLog(logBuffer);

        if (isDeleted()) {
            LogUtils.writePackedInt(logBuffer, -1);
        } else {
            LogUtils.writePackedInt(logBuffer, data.length);
            LogUtils.writeBytesNoLength(logBuffer, data);
        }
    }

    /**
     * @see Loggable#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        super.readFromLog(itemBuffer, entryVersion);
        
        if (entryVersion < 8) {
            /* Discard node ID from older version entry. */
            LogUtils.readLong(itemBuffer, entryVersion < 6 /*unpacked*/);
        }

        if (entryVersion < 6) {
            boolean dataExists = LogUtils.readBoolean(itemBuffer);
            if (dataExists) {
                data = LogUtils.readByteArray(itemBuffer, true/*unpacked*/);
            }
        } else {
            int size = LogUtils.readInt(itemBuffer, false/*unpacked*/);
            if (size >= 0) {
                data = LogUtils.readBytesNoLength(itemBuffer, size);
            }
        }
    }

    /**
     * @see Loggable#logicalEquals
     */
    public boolean logicalEquals(Loggable other) {

        if (!(other instanceof LN)) {
            return false;
        }

        LN otherLN = (LN) other;

        if (!Arrays.equals(getData(), otherLN.getData())) {
            return false;
        }

        return true;
    }

    /**
     * @see Loggable#dumpLog
     */
    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(beginTag());
        super.dumpLog(sb, verbose);

        if (data != null) {
            sb.append("<data>");
            if (verbose) {
                sb.append(Key.DUMP_TYPE.dumpByteArray(data));
            } else {
                sb.append("hidden");
            }
            sb.append("</data>");
        }

        dumpLogAdditional(sb, verbose);

        sb.append(endTag());
    }

    public void dumpKey(StringBuilder sb, byte[] key) {
        sb.append(Key.dumpString(key, 0));
    }

    /*
     * Allows subclasses to add additional fields before the end tag.
     */
    protected void dumpLogAdditional(StringBuilder sb,
                                     @SuppressWarnings("unused")
                                     boolean verbose) {
    }

    /**
     * Account for FileSummaryLN's extra marshaled memory. [#17462]
     */
    public void addExtraMarshaledMemorySize(BIN parentBIN) {
        /* Do nothing here.  Overwridden in FileSummaryLN. */
    }

    /*
     * DatabaseEntry utilities
     */

    /**
     * Copies the non-deleted LN's byte array to the entry.  Does not support
     * partial data.
     */
    public void setEntry(DatabaseEntry entry) {
        assert !isDeleted();
        int len = data.length;
        byte[] bytes = new byte[len];
        System.arraycopy(data, 0, bytes, 0, len);
        entry.setData(bytes);
    }

    /**
     * Copies the given byte array to the entry, copying only partial data if
     * the entry is specified to be partial.  If the byte array is null, clears
     * the entry.
     */
    public static void setEntry(DatabaseEntry entry, byte[] bytes) {

        if (bytes != null) {
            boolean partial = entry.getPartial();
            int off = partial ? entry.getPartialOffset() : 0;
            int len = partial ? entry.getPartialLength() : bytes.length;
            if (off + len > bytes.length) {
                len = (off > bytes.length) ? 0 : bytes.length  - off;
            }

            byte[] newdata = null;
            if (len == 0) {
                newdata = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
            } else {
                newdata = new byte[len];
                System.arraycopy(bytes, off, newdata, 0, len);
            }
            entry.setData(newdata);
            entry.setOffset(0);
            entry.setSize(len);
        } else {
            entry.setData(null);
            entry.setOffset(0);
            entry.setSize(0);
        }
    }

    /**
     * Returns a byte array that is a complete copy of the data in a
     * non-partial entry.
     */
    public static byte[] copyEntryData(DatabaseEntry entry) {
        assert !entry.getPartial();
        int len = entry.getSize();
        final byte[] newData =
            (len == 0) ? LogUtils.ZERO_LENGTH_BYTE_ARRAY : (new byte[len]);
        System.arraycopy(entry.getData(), entry.getOffset(),
                         newData, 0, len);
        return newData;
    }

    /**
     * Merges the partial entry with the given byte array, effectively applying
     * a partial entry to an existing record, and returns a enw byte array.
     */
    public static byte[] resolvePartialEntry(DatabaseEntry entry,
                                             byte[] foundDataBytes ) {
        assert foundDataBytes != null;
        final int dlen = entry.getPartialLength();
        final int doff = entry.getPartialOffset();
        final int origlen =
            (foundDataBytes != null) ? foundDataBytes.length : 0;
        final int oldlen = (doff + dlen > origlen) ? (doff + dlen) : origlen;
        final int len = oldlen - dlen + entry.getSize();

        final byte[] newData;
        if (len == 0) {
            newData = LogUtils.ZERO_LENGTH_BYTE_ARRAY;
        } else {
            newData = new byte[len];
        }
        int pos = 0;

        /* Keep 0..doff of the old data (truncating if doff > length). */
        int slicelen = (doff < origlen) ? doff : origlen;
        if (slicelen > 0) {
            System.arraycopy(foundDataBytes, 0, newData, pos, slicelen);
        }
        pos += doff;

        /* Copy in the new data. */
        slicelen = entry.getSize();
        System.arraycopy(entry.getData(), entry.getOffset(), newData, pos,
                         slicelen);
        pos += slicelen;

        /* Append the rest of the old data (if any). */
        slicelen = origlen - (doff + dlen);
        if (slicelen > 0) {
            System.arraycopy(foundDataBytes, doff + dlen, newData, pos,
                             slicelen);
        }

        return newData;
    }
}
