/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.tree;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.cleaner.LocalUtilizationTracker;
import com.sleepycat.je.cleaner.PackedObsoleteInfo;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentFailureReason;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.dbi.INList;
import com.sleepycat.je.dbi.MemoryBudget;
import com.sleepycat.je.evictor.Evictor.EvictionSource;
import com.sleepycat.je.latch.SharedLatch;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.Loggable;
import com.sleepycat.je.log.Provisional;
import com.sleepycat.je.log.ReplicationContext;
import com.sleepycat.je.log.entry.INLogEntry;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.tree.dupConvert.DupConvert;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.LoggerUtils;
import com.sleepycat.je.utilint.RelatchRequiredException;
import com.sleepycat.je.utilint.SizeofMarker;

/**
 * An IN represents an Internal Node in the JE tree.
 *
 * Explanation of KD (KnownDeleted) and PD (PendingDelete) entry flags
 * ===================================================================
 *
 * PD: set for all LN entries that are deleted, even before the LN is
 * committed.  Is used as an authoritative (transactionally correct) indication
 * that an LN is deleted. PD will be cleared if the txn for the deleted LN is
 * aborted.
 *
 * KD: set under special conditions for entries containing LNs which are known
 * to be obsolete.  Not used for entries in an active/uncommitted transaction.
 *
 * First notice that IN.fetchTarget will allow a FileNotFoundException when the
 * PD or KD flag is set on the entry.  And it will allow a NULL_LSN when the KD
 * flag is set.
 *
 * KD was implemented first, and was originally used when the cleaner attempts
 * to migrate an LN and discovers it is deleted (see Cleaner.migrateLN). We
 * need KD because the INCompressor may not have run, and may not have
 * compressed the BIN. There's the danger that we'll try to fetch that entry,
 * and that the file was deleted by the cleaner.
 *
 * KD was used more recently when an unexpected exception occurs while logging
 * an LN, after inserting the entry.  Rather than delete the entry to clean up,
 * we mark the entry KD so it won't cause a fetch error later.  In this case
 * the entry LSN is NULL_LSN. See Tree.insertNewSlot.
 *
 * PD is closely related to the first use of KD above (for cleaned deleted LNs)
 * and came about because of a cleaner optimization we make. The cleaner
 * considers all deleted LN log entries to be obsolete, without doing a tree
 * lookup, and without any record of an obsolete offset.  This makes the cost
 * of cleaning of deleted LNs very low.  For example, if the log looks like
 * this:
 *
 * 100  LNA
 * 200  delete of LNA
 *
 * then LSN 200 will be considered obsolete when this file is processed by the
 * cleaner. After all, only two things can happen: (1) the txn commits, and we
 * don't need LSN 200, because we can wipe this LN out of the tree, or (2) the
 * txn aborts, and we don't need LSN 200, because we are going to revert to LSN
 * 100/LNA.
 *
 * We set PD for the entry of a deleted LN at the time of the operation, and we
 * clear PD if the transaction aborts.  There is no real danger that this log
 * entry will be processed by the cleaner before it's committed, because
 * cleaning can only happen after the first active LSN.
 *
 * Just as in the first use of KD above, setting PD is necessary to avoid a
 * fetch error, when the file is deleted by the cleaner but the entry
 * containing the deleted LN has not been deleted by the INCompressor.
 *
 * PD is also set in replication rollback, when LNs are marked as
 * invisible.
 *
 * When LSN locking was implemented (see CursorImpl.lockLN), the PD flag took
 * on additional meaning.  PD is used to determine whether an LN is deleted
 * without fetching it, and therefore is relied on to be transactionally
 * correct.
 *
 * In addition to the setting and use of the KD/PD flags described above, the
 * situation is complicated by the fact that we must restore the state of these
 * flags during abort, recovery, and set them properly during slot reuse.
 *
 * We have been meaning to consider whether PD and KD can be consolidated into
 * one flag: simply the Deleted flag.  The Deleted flag would be set in the
 * same way as PD is currently set, as well as the second use of KD described
 * above (when the LSN is NULL_LSN after an insertion error).  The use of KD
 * and PD for invisible entries and recovery rollback should also be
 * considered.
 *
 * If we consolidate the two flags and set the Deleted flag during a delete
 * operation (like PD), we'll have to remove optimizations (in CursorImpl for
 * example) that consider a slot deleted when KD is set.  Since KD is rarely
 * set currently, this shouldn't have a noticeable performance impact.
 */
public class IN extends Node implements Comparable<IN>, Loggable {

    private static final String BEGIN_TAG = "<in>";
    private static final String END_TAG = "</in>";
    private static final String TRACE_SPLIT = "Split:";
    private static final String TRACE_DELETE = "Delete:";

    private static final byte KNOWN_DELETED_BIT = 0x1;
    private static final byte CLEAR_KNOWN_DELETED_BIT = ~0x1;
    private static final byte DIRTY_BIT = 0x2;
    private static final byte CLEAR_DIRTY_BIT = ~0x2;
    private static final byte MIGRATE_BIT = 0x4;
    private static final byte CLEAR_MIGRATE_BIT = ~0x4;
    private static final byte PENDING_DELETED_BIT = 0x8;
    private static final byte CLEAR_PENDING_DELETED_BIT = ~0x8;

    private static final int BYTES_PER_LSN_ENTRY = 4;
    private static final int MAX_FILE_OFFSET = 0xfffffe;
    private static final int THREE_BYTE_NEGATIVE_ONE = 0xffffff;

    /*
     * Levels:
     * The mapping tree has levels in the 0x20000 -> 0x2ffff number space.
     * The main tree has levels in the 0x10000 -> 0x1ffff number space.
     * The duplicate tree levels are in 0-> 0xffff number space.
     */
    public static final int DBMAP_LEVEL = 0x20000;
    public static final int MAIN_LEVEL = 0x10000;
    public static final int LEVEL_MASK = 0x0ffff;
    public static final int MIN_LEVEL = -1;
    public static final int MAX_LEVEL = Integer.MAX_VALUE;
    public static final int BIN_LEVEL = MAIN_LEVEL | 1;

    /*
     * IN eviction types returned by getEvictionType.
     */
    public static final int MAY_NOT_EVICT = 0;
    public static final int MAY_EVICT_LNS = 1;
    public static final int MAY_EVICT_NODE = 2;

    private static final int IN_DIRTY_BIT = 0x1;
    private static final int IN_RECALC_TOGGLE_BIT = 0x2;
    private static final int IN_IS_ROOT_BIT = 0x4;
    private int flags; // not persistent

    /* The unique id of this node. */
    private long nodeId;

    protected SharedLatch latch;
    private long generation;
    private int nEntries;
    private byte[] identifierKey;

    /*
     * The children of this IN. Only the ones that are actually in the cache
     * have non-null entries. Specialized sparse array represents are used to
     * represent the entries. The representation can mutate as modifications
     * are made to it.
     */
    private INTargetRep entryTargets;

    /*
     * entryKeyVals contains the keys in their entirety if key prefixing is not
     * being used. If prefixing is enabled, then keyPrefix contains the prefix
     * and entryKeyVals contains the suffixes.
     */
    private INKeyRep entryKeyVals;
    private byte[] keyPrefix;

    /*
     * The following entryLsnXXX fields are used for storing LSNs.  There are
     * two possible representations: a byte array based rep, and a long array
     * based one.  For compactness, the byte array rep is used initially.  A
     * single byte[] that uses four bytes per LSN is used. The baseFileNumber
     * field contains the lowest file number of any LSN in the array.  Then for
     * each entry (four bytes each), the first byte contains the offset from
     * the baseFileNumber of that LSN's file number.  The remaining three bytes
     * contain the file offset portion of the LSN.  Three bytes will hold a
     * maximum offset of 16,777,214 (0xfffffe), so with the default JE log file
     * size of 10,000,000 bytes this works well.
     *
     * If either (1) the difference in file numbers exceeds 127
     * (Byte.MAX_VALUE) or (2) the file offset is greater than 16,777,214, then
     * the byte[] based rep mutates to a long[] based rep.
     *
     * In the byte[] rep, DbLsn.NULL_LSN is represented by setting the file
     * offset bytes for a given entry to -1 (0xffffff).
     */
    private long baseFileNumber;
    private byte[] entryLsnByteArray;
    private long[] entryLsnLongArray;
    private byte[] entryStates;
    DatabaseImpl databaseImpl;
    private int level;
    private long inMemorySize;

    private boolean inListResident; // true if this IN is on the IN list

    /* Location of last full version. */
    private long lastFullVersion = DbLsn.NULL_LSN;

    /*
     * A sequence of obsolete info that cannot be counted as obsolete until an
     * ancestor IN is logged non-provisionally.
     */
    private PackedObsoleteInfo provisionalObsolete;

    /* See convertDupKeys. */
    private boolean needDupKeyConversion;

    /* Used to indicate that an exact match was found in findEntry. */
    public static final int EXACT_MATCH = (1 << 16);

    /* Used to indicate that an insert was successful. */
    public static final int INSERT_SUCCESS = (1 << 17);

    /*
     * accumluted memory budget delta.  Once this exceeds
     * MemoryBudget.ACCUMULATED_LIMIT we inform the MemoryBudget that a change
     * has occurred.  See SR 12273.
     */
    private int accumulatedDelta = 0;

    /*
     * Max allowable accumulation of memory budget changes before MemoryBudget
     * should be updated. This allows for consolidating multiple calls to
     * updateXXXMemoryBudget() into one call.  Not declared final so that the
     * unit tests can modify it.  See SR 12273.
     */
    public static int ACCUMULATED_LIMIT = 1000;

    /**
     * Create an empty IN, with no node ID, to be filled in from the log.
     */
    public IN() {
        init(null, Key.EMPTY_KEY, 0, 0);
    }

    /**
     * Create a new IN.
     */
    public IN(DatabaseImpl dbImpl,
              byte[] identifierKey,
              int capacity,
              int level) {

        nodeId =
            dbImpl.getDbEnvironment().getNodeSequence().getNextLocalNodeId();
        init(dbImpl, identifierKey, capacity,
             generateLevel(dbImpl.getId(), level));
        initMemorySize();
    }

    /**
     * For Sizeof, set all array fields to null, since they are not part of the
     * fixed overhead.
     */
    public IN(@SuppressWarnings("unused") SizeofMarker marker) {
        entryTargets = null;
        entryKeyVals = null;
        keyPrefix = null;
        entryLsnByteArray = null;
        entryLsnLongArray = null;
        entryStates = null;
    }

    /**
     * Initialize IN object.
     */
    protected void init(DatabaseImpl db,
                        @SuppressWarnings("hiding")
                        byte[] identifierKey,
                        int initialCapacity,
                        @SuppressWarnings("hiding")
                        int level) {
        setDatabase(db);
        latch = new SharedLatch(shortClassName() + getNodeId());
        latch.setExclusiveOnly(EnvironmentImpl.getSharedLatches() ?
                isAlwaysLatchedExclusively() :
                true);
        assert latch.setNoteLatch(true);
        generation = 0;
        flags = 0;
        nEntries = 0;
        this.identifierKey = identifierKey;
        entryTargets = INTargetRep.NONE;
        entryKeyVals = new INKeyRep.Default(initialCapacity);
        keyPrefix = null;
        baseFileNumber = -1;
        entryLsnByteArray = new byte[initialCapacity << 2];
        entryLsnLongArray = null;
        entryStates = new byte[initialCapacity];
        this.level = level;
        inListResident = false;
    }

    /**
     * Initialize the per-node memory count by computing its memory usage.
     */
    protected void initMemorySize() {
        entryKeyVals = entryKeyVals.compact(this);
        inMemorySize = computeMemorySize();
    }

    public long getNodeId() {
        return nodeId;
    }

    /* For unit tests only. */
    void setNodeId(long nid) {
        nodeId = nid;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IN)) {
            return false;
        }
        IN in = (IN) obj;
        return (this.getNodeId() == in.getNodeId());
    }

    /**
     * We would like as even a hash distribution as possible so that the
     * Evictor's LRU is as accurate as possible.  ConcurrentHashMap takes the
     * value returned by this method and runs its own hash algorithm on it.
     * So a bit complement of the node ID is sufficent as the return value and
     * is a little better than returning just the node ID.  If we use a
     * different container in the future that does not re-hash the return
     * value, we should probably implement the Wang-Jenkins hash function here.
     */
    @Override
    public int hashCode() {
        return (int) ~getNodeId();
    }

    /**
     * Sort based on equality key.
     */
    public int compareTo(IN argIN) {
        long argNodeId = argIN.getNodeId();
        long myNodeId = getNodeId();

        if (myNodeId < argNodeId) {
            return -1;
        } else if (myNodeId > argNodeId) {
            return 1;
        } else {
            return 0;
        }
    }

    /**
     * Create a new IN.  Need this because we can't call newInstance() without
     * getting a 0 for nodeId.
     */
    protected IN createNewInstance(byte[] identifierKey,
                                   int maxEntries,
                                   int level) {
        return new IN(databaseImpl, identifierKey, maxEntries, level);
    }

    /*
     * Return whether the shared latch for this kind of node should be of the
     * "always exclusive" variety.  Presently, only IN's are actually latched
     * shared.  BINs are latched exclusive only.
     */
    boolean isAlwaysLatchedExclusively() {
        return false;
    }

    /**
     * Initialize a node that has been read in from the log.
     */
    @Override
    public void postFetchInit(DatabaseImpl db, long lastLoggedLsn) {
        commonPostFetchInit(db, lastLoggedLsn);
        convertDupKeys(); // must be after initMemorySize
        db.getDbEnvironment().getInMemoryINs().add(this);
    }

    /**
     * Initialize a node read in during recovery.
     */
    public void postRecoveryInit(DatabaseImpl db, long lastLoggedLsn) {
        commonPostFetchInit(db, lastLoggedLsn);
    }

    /**
     * Common actions of postFetchInit and postRecoveryInit.
     */
    private void commonPostFetchInit(DatabaseImpl db, long lastLoggedLsn) {
        setDatabase(db);
        setLastLoggedLsn(lastLoggedLsn);
        initMemorySize(); // compute before adding to IN list
    }

    /**
     * Sets the last logged LSN, which for a BIN may be a delta.
     *
     * In this class, the last logged version and last full version are the
     * same.  In the BIN class, this method is overridden since they may be
     * different.
     */
    void setLastLoggedLsn(long lsn) {
        setLastFullLsn(lsn);
    }

    /**
     * Returns the last logged LSN, or NULL_LSN if never logged.
     *
     * In this class, the last logged version and last full version are the
     * same.  In the BIN class, this method is overridden since they may be
     * different.
     */
    public long getLastLoggedVersion() {
        return getLastFullVersion();
    }

    /**
     * Sets the last full version LSN.
     */
    void setLastFullLsn(long lsn) {
        lastFullVersion = lsn;
    }

    /**
     * Returns the last full version LSN, or NULL_LSN if never logged.
     * Public for unit testing.
     */
    public long getLastFullVersion() {
        return lastFullVersion;
    }

    /**
     * Returns the last delta version LSN, or NULL_LSN if a delta was not last
     * logged.  Public for unit testing.
     */
    public long getLastDeltaVersion() {
        return DbLsn.NULL_LSN;
    }

    /**
     * Latch this node exclusive, optionally setting the generation.
     */
    public void latch(CacheMode cacheMode)
        throws DatabaseException {

        setGeneration(cacheMode);
        latch.acquireExclusive();
    }

    /**
     * Latch this node shared, optionally setting the generation.
     */
    @Override
    public void latchShared(CacheMode cacheMode)
        throws DatabaseException {

        setGeneration(cacheMode);
        latch.acquireShared();
    }

    /**
     * Latch this node if it is not latched by another thread, optionally
     * setting the generation if the latch succeeds.
     */
    public boolean latchNoWait(CacheMode cacheMode)
        throws DatabaseException {

        if (latch.acquireExclusiveNoWait()) {
            setGeneration(cacheMode);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Latch this node exclusive and set the generation.
     */
    public void latch()
        throws DatabaseException {

        latch(CacheMode.DEFAULT);
    }

    /**
     * Latch this node shared and set the generation.
     */
    @Override
    public void latchShared()
        throws DatabaseException {

        latchShared(CacheMode.DEFAULT);
    }

    /**
     * Latch this node if it is not latched by another thread, and set the
     * generation if the latch succeeds.
     */
    public boolean latchNoWait()
        throws DatabaseException {

        return latchNoWait(CacheMode.DEFAULT);
    }

    public String getLatchString() { return latch.toString(); }

    /**
     * Release the latch on this node.
     */
    @Override
    public void releaseLatch() {
        latch.release();
    }

    /**
     * Release the latch on this node.
     */
    public void releaseLatchIfOwner() {
        latch.releaseIfOwner();
    }

    /**
     * @return true if this thread holds the IN's latch
     */
    public boolean isLatchOwnerForRead() {
        return latch.isOwner();
    }

    public boolean isLatchOwnerForWrite() {
        return latch.isWriteLockedByCurrentThread();
    }

    /* For unit testing. */
    public int getLatchQueueLength() {
        return latch.getQueueLength();
    }

    public long getGeneration() {
        return generation;
    }

    public void setGeneration(CacheMode cacheMode) {
        switch (cacheMode) {
        case DEFAULT:
            generation = Generation.getNextGeneration();
            break;
        case UNCHANGED:
            break;
        case KEEP_HOT:
            generation = Long.MAX_VALUE;
            break;
        case MAKE_COLD:
        case EVICT_LN:
        case EVICT_BIN:
            if (isBIN()) {
                generation = 0L;
            }
            break;
        default:
            throw EnvironmentFailureException.unexpectedState
                ("unknown cacheMode: " + cacheMode);
        }
    }

    public void setGeneration(long newGeneration) {
        generation = newGeneration;
    }

    @Override
    public int getLevel() {
        return level;
    }

    private static int generateLevel(DatabaseId dbId, int newLevel) {
        if (dbId.equals(DbTree.ID_DB_ID)) {
            return newLevel | DBMAP_LEVEL;
        } else {
            return newLevel | MAIN_LEVEL;
        }
    }

    /* This has default protection for access by the unit tests. */
    void setKeyPrefix(byte[] keyPrefix) {
        assert databaseImpl != null;
        int prevLength = (this.keyPrefix == null) ? 0 : this.keyPrefix.length;
        this.keyPrefix = keyPrefix;
        /* Update the memory budgeting to reflect changes in the key prefix. */
        int currLength = (keyPrefix == null) ? 0 : keyPrefix.length;
        updateMemorySize(prevLength, currLength);
    }

    byte[] getKeyPrefix() {
        return keyPrefix;
    }

    public boolean getDirty() {
        return (flags & IN_DIRTY_BIT) != 0;
    }

    /* public for unit tests */
    public void setDirty(boolean dirty) {
        if (dirty) {
            flags |= IN_DIRTY_BIT;
        } else {
            flags &= ~IN_DIRTY_BIT;
        }
    }

    public boolean getRecalcToggle() {
        return (flags & IN_RECALC_TOGGLE_BIT) != 0;
    }

    public void setRecalcToggle(boolean toggle) {
        if (toggle) {
            flags |= IN_RECALC_TOGGLE_BIT;
        } else {
            flags &= ~IN_RECALC_TOGGLE_BIT;
        }
    }

    public boolean isRoot() {
        return (flags & IN_IS_ROOT_BIT) != 0;
    }

    public boolean isDbRoot() {
        return (flags & IN_IS_ROOT_BIT) != 0;
    }

    void setIsRoot(boolean isRoot) {
        setIsRootFlag(isRoot);
        setDirty(true);
    }

    private void setIsRootFlag(boolean isRoot) {
        if (isRoot) {
            flags |= IN_IS_ROOT_BIT;
        } else {
            flags &= ~IN_IS_ROOT_BIT;
        }
    }

    /**
     * @return the identifier key for this node.
     */
    public byte[] getIdentifierKey() {
        return identifierKey;
    }

    /**
     * Set the identifier key for this node.
     *
     * @param key - the new identifier key for this node.
     */
    public void setIdentifierKey(byte[] key) {

        /*
         * The identifierKey is "intentionally" not kept track of in the
         * memory budget.  If we did, then it would look like this:

         int oldIDKeySz = (identifierKey == null) ?
                           0 :
                           MemoryBudget.byteArraySize(identifierKey.length);

         int newIDKeySz = (key == null) ?
                           0 :
                           MemoryBudget.byteArraySize(key.length);
         changeMemorySize(newIDKeySz - oldIDKeySz);

         */
        identifierKey = key;
        setDirty(true);
    }

    /**
     * Get the database for this IN.
     */
    public DatabaseImpl getDatabase() {
        return databaseImpl;
    }

    /**
     * Set the database reference for this node.
     */
    public void setDatabase(DatabaseImpl db) {
        databaseImpl = db;
    }

    /**
     * Needed only during duplicates conversion, not during normal operation.
     * The needDupKeyConversion field will only be true when first upgrading
     * from JE 4.1.  After the first time an IN is converted, it will be
     * written out in a later file format, so the needDupKeyConversion field
     * will be false and this method will do nothing.  See
     * DupConvert.convertInKeys.
     */
    private void convertDupKeys() {
        /* Do not convert more than once. */
        if (!needDupKeyConversion) {
            return;
        }
        needDupKeyConversion = false;
        DupConvert.convertInKeys(databaseImpl, this);
    }

    /*
     * Get the database id for this node.
     */
    public DatabaseId getDatabaseId() {
        return databaseImpl.getId();
    }

    void copyEntries(final int from, final int to, final int n) {
        entryTargets = entryTargets.copy(from, to, n, this);
        entryKeyVals = entryKeyVals.copy(from, to, n, this);
        System.arraycopy(entryStates, from, entryStates, to, n);
        if (entryLsnLongArray == null) {
            final int fromOff = from << 2;
            final int toOff = to << 2;
            final int nBytes = n << 2;
            System.arraycopy(entryLsnByteArray, fromOff,
                             entryLsnByteArray, toOff, nBytes);
        } else {
            System.arraycopy(entryLsnLongArray, from,
                             entryLsnLongArray, to,
                             n);
        }
    }

    void clearEntry(int idx) {
        entryTargets = entryTargets.set(idx, null, this);
        entryKeyVals = entryKeyVals.set(idx, null, this);
        setLsnElement(idx, DbLsn.NULL_LSN);
        entryStates[idx] = 0;
    }

    /**
     * Return the idx'th key.  If prefixing is enabled, construct a new byte[]
     * containing the prefix and suffix.  If prefixing is not enabled, just
     * return the current byte[] in entryKeyVals[].
     */
    public byte[] getKey(int idx) {
        if (keyPrefix != null) {
            int prefixLen = keyPrefix.length;
            byte[] suffix = entryKeyVals.get(idx);
            if (prefixLen == 0) {
                return suffix;
            }
            int suffixLen = (suffix == null ? 0 : suffix.length);
            byte[] ret = new byte[prefixLen + suffixLen];
            if (keyPrefix != null) {
                System.arraycopy(keyPrefix, 0, ret, 0, prefixLen);
            }

            if (suffix != null) {
                System.arraycopy(suffix, 0, ret, prefixLen, suffixLen);
            }
            return ret;
        }
        return entryKeyVals.get(idx);
    }

    /**
     * Set the idx'th key.
     *
     * @return true if a multi-slot change was made and the complete IN memory
     * size must be updated.
     */
    private boolean setKeyAndDirty(int idx, byte[] keyVal) {
        entryStates[idx] |= DIRTY_BIT;
        return setKeyAndPrefix(idx, keyVal);
    }

    /*
     * Set the key at idx and adjust the key prefix if necessary.
     *
     * @return true if a multi-slot change was made and the complete IN memory
     * size must be updated.
     */
    private boolean setKeyAndPrefix(int idx, byte[] keyVal) {

        /*
         * Only compute key prefix if prefixing is enabled and there's an
         * existing prefix.
         */
        assert databaseImpl != null;
        if (databaseImpl.getKeyPrefixing() && keyPrefix != null) {
            if (!compareToKeyPrefix(keyVal)) {

                /*
                 * The new key doesn't share the current prefix, so recompute
                 * the prefix and readjust all the existing suffixes.
                 */
                byte[] newPrefix = computeKeyPrefix(idx);
                if (newPrefix != null) {
                    /* Take the new key into consideration for new prefix. */
                    newPrefix = Key.createKeyPrefix(newPrefix, keyVal);
                }
                recalcSuffixes(newPrefix, keyVal, idx);
                return true;
            }
            final INKeyRep.Type prevRepType = entryKeyVals.getType();
            entryKeyVals = entryKeyVals.set
                (idx, computeKeySuffix(keyPrefix, keyVal), this);
            return prevRepType != entryKeyVals.getType();
        }

        if (keyPrefix != null) {

            /*
             * Key prefixing has been turned off on this database, but there
             * are existing prefixes. Remove prefixes for this IN.
             */
            recalcSuffixes(new byte[0], keyVal, idx);
            return true;
        } else {
            final INKeyRep.Type oldRepType = entryKeyVals.getType();
            entryKeyVals = entryKeyVals.set(idx, keyVal, this);
            return oldRepType != entryKeyVals.getType();
        }
    }

    /**
     * Forces computation of the key prefix, without requiring a split.
     * Is public for use by DbCacheSize.
     */
    public void recalcKeyPrefix() {
        recalcSuffixes(computeKeyPrefix(-1), null, -1);
    }

    /*
     * Iterate over all keys in this IN and recalculate their suffixes based on
     * newPrefix.  If keyVal and idx are supplied, it means that entry[idx] is
     * about to be changed to keyVal so use that instead of
     * entryKeyVals.get(idx) when computing the new suffixes. If idx is < 0,
     * and keyVal is null, then recalculate suffixes for all entries in this.
     */
    private void recalcSuffixes(byte[] newPrefix, byte[] keyVal, int idx) {
        for (int i = 0; i < nEntries; i++) {
            byte[] curKey = (i == idx ? keyVal : getKey(i));
            entryKeyVals =
                entryKeyVals.set(i, computeKeySuffix(newPrefix, curKey), this);
        }
        setKeyPrefix(newPrefix);
    }

    /*
     * Returns whether the given newKey is a prefix of, or equal to, the
     * current keyPrefix.
     *
     * This has default protection for the unit tests.
     */
    boolean compareToKeyPrefix(byte[] newKey) {
        if (keyPrefix == null ||
            keyPrefix.length == 0) {
            return false;
        }

        int newKeyLen = newKey.length;
        for (int i = 0; i < keyPrefix.length; i++) {
            if (i < newKeyLen &&
                keyPrefix[i] == newKey[i]) {
                continue;
            } else {
                return false;
            }
        }

        return true;
    }

    /*
     * Computes a key prefix based on all the keys in 'this'.  Return null if
     * the IN is empty or prefixing is not enabled or there is no common
     * prefix for the keys.
     */
    private byte[] computeKeyPrefix(int excludeIdx) {
        if (!databaseImpl.getKeyPrefixing() ||
            nEntries <= 1) {
            return null;
        }

        int firstIdx = (excludeIdx == 0) ? 1 : 0;
        byte[] curPrefixKey = getKey(firstIdx);
        int prefixLen = curPrefixKey.length;

        /*
         * Only need to look at first and last entries when keys are ordered
         * byte-by-byte.  But when there is a comparator, keys are not
         * necessarily ordered byte-by-byte.  [#21328]
         */
        boolean byteOrdered;
        if (true) {
            /* Disable optimization for now.  Needs testing. */
            byteOrdered = false;
        } else {
            byteOrdered = (databaseImpl.getKeyComparator() == null);
        }
        if (byteOrdered) {
            int lastIdx = nEntries - 1;
            if (lastIdx == excludeIdx) {
                lastIdx -= 1;
            }
            if (lastIdx > firstIdx) {
                byte[] lastKey = getKey(lastIdx);
                int newPrefixLen = Key.getKeyPrefixLength
                    (curPrefixKey, prefixLen, lastKey);
                if (newPrefixLen < prefixLen) {
                    curPrefixKey = lastKey;
                    prefixLen = newPrefixLen;
                }
            }
        } else {
            for (int i = firstIdx + 1; i < nEntries; i++) {
                if (i == excludeIdx) {
                    continue;
                }
                byte[] curKey = getKey(i);
                int newPrefixLen = Key.getKeyPrefixLength
                    (curPrefixKey, prefixLen, curKey);
                if (newPrefixLen < prefixLen) {
                    curPrefixKey = curKey;
                    prefixLen = newPrefixLen;
                }
            }
        }

        byte[] ret = new byte[prefixLen];
        System.arraycopy(curPrefixKey, 0, ret, 0, prefixLen);

        return ret;
    }

    /*
     * Given a prefix and a key, return the suffix portion of keyVal.
     */
    private byte[] computeKeySuffix(byte[] newPrefix, byte[] keyVal) {
        int prefixLen = (newPrefix == null ? 0 : newPrefix.length);

        if (prefixLen == 0) {
            return keyVal;
        }

        int suffixLen = keyVal.length - prefixLen;
        byte[] ret = new byte[suffixLen];
        System.arraycopy(keyVal, prefixLen, ret, 0, suffixLen);
        return ret;
    }

    /*
     * For debugging.
     */
    boolean verifyKeyPrefix() {
        byte[] computedKeyPrefix = computeKeyPrefix(-1);
        if (keyPrefix == null) {
            return computedKeyPrefix == null;
        }

        if (computedKeyPrefix == null ||
            computedKeyPrefix.length < keyPrefix.length) {
            System.out.println("VerifyKeyPrefix failed");
            System.out.println(dumpString(0, false));
            return false;
        }
        for (int i = 0; i < keyPrefix.length; i++) {
            if (keyPrefix[i] != computedKeyPrefix[i]) {
                System.out.println("VerifyKeyPrefix failed");
                System.out.println(dumpString(0, false));
                return false;
            }
        }
        return true;
    }

    /**
     * Get the idx'th migrate status.
     */
    public boolean getMigrate(int idx) {
        return (entryStates[idx] & MIGRATE_BIT) != 0;
    }

    /**
     * Set the idx'th migrate status.
     */
    public void setMigrate(int idx, boolean migrate) {
        if (migrate) {
            entryStates[idx] |= MIGRATE_BIT;
        } else {
            entryStates[idx] &= CLEAR_MIGRATE_BIT;
        }
    }

    public byte getState(int idx) {
        return entryStates[idx];
    }

    /**
     * Return the idx'th target.
     */
    public Node getTarget(int idx) {
        return entryTargets.get(idx);
    }

    /**
     * Sets the idx'th target. No need to make dirty, that state only applies
     * to key and LSN.
     *
     * <p>WARNING: This method does not update the memory budget.  The caller
     * must update the budget.</p>
     */
    void setTarget(int idx, Node target) {
        assert isLatchOwnerForWrite() :
            "Not latched for write " + getClass().getName() +
            " id=" + getNodeId();
        entryTargets = entryTargets.set(idx, target, this);
    }

    /**
     * Return the idx'th LSN for this entry.
     *
     * @return the idx'th LSN for this entry.
     */
    public long getLsn(int idx) {
        if (entryLsnLongArray == null) {
            int offset = idx << 2;
            int fileOffset = getFileOffset(offset);
            if (fileOffset == -1) {
                return DbLsn.NULL_LSN;
            } else {
                return DbLsn.makeLsn((baseFileNumber +
                                      getFileNumberOffset(offset)),
                                     fileOffset);
            }
        } else {
            return entryLsnLongArray[idx];
        }
    }

    /**
     * Sets the idx'th target LSN. Make this a private helper method, so we're
     * sure to set the IN dirty where appropriate.
     *
     * Should only be called by setLsn and prepareForSlotReuse methods.
     */
    private void setLsnNoValidation(int idx, long lsn) {

        int oldSize = computeLsnOverhead();
        /* setLsnElement can mutate to an array of longs. */
        setLsnElement(idx, lsn);
        changeMemorySize(computeLsnOverhead() - oldSize);
        entryStates[idx] |= DIRTY_BIT;
    }

    /* For unit tests. */
    long[] getEntryLsnLongArray() {
        return entryLsnLongArray;
    }

    /* For unit tests. */
    byte[] getEntryLsnByteArray() {
        return entryLsnByteArray;
    }

    /* For unit tests. */
    void initEntryLsn(int capacity) {
        entryLsnLongArray = null;
        entryLsnByteArray = new byte[capacity << 2];
        baseFileNumber = -1;
    }

    /* Use default protection for unit tests. */
    void setLsnElement(int idx, long value) {

        int offset = idx << 2;
        /* Will implement this in the future. Note, don't adjust if mutating.*/
        //maybeAdjustCapacity(offset);
        if (entryLsnLongArray != null) {
            entryLsnLongArray[idx] = value;
            return;
        }

        if (value == DbLsn.NULL_LSN) {
            setFileNumberOffset(offset, (byte) 0);
            setFileOffset(offset, -1);
            return;
        }

        long thisFileNumber = DbLsn.getFileNumber(value);

        if (baseFileNumber == -1) {
            /* First entry. */
            baseFileNumber = thisFileNumber;
            setFileNumberOffset(offset, (byte) 0);
        } else {
            if (thisFileNumber < baseFileNumber) {
                if (!adjustFileNumbers(thisFileNumber)) {
                    mutateToLongArray(idx, value);
                    return;
                }
                baseFileNumber = thisFileNumber;
            }
            long fileNumberDifference = thisFileNumber - baseFileNumber;
            if (fileNumberDifference > Byte.MAX_VALUE) {
                mutateToLongArray(idx, value);
                return;
            }
            setFileNumberOffset
                (offset, (byte) (thisFileNumber - baseFileNumber));
            //assert getFileNumberOffset(offset) >= 0;
        }

        int fileOffset = (int) DbLsn.getFileOffset(value);
        if (fileOffset > MAX_FILE_OFFSET) {
            mutateToLongArray(idx, value);
            return;
        }

        setFileOffset(offset, fileOffset);
        //assert getLsn(offset) == value;
    }

    /**
     * Mutates the compact LSN representation to the expanded representation.
     * Used by DbCacheSize.
     */
    public void clearLsnCompaction() {
        if (entryLsnByteArray != null) {
            mutateToLongArray(0, getLsn(0));
            updateMemorySize(inMemorySize, computeMemorySize());
        }
    }

    private void mutateToLongArray(int idx, long value) {
        int nElts = entryLsnByteArray.length >> 2;
        long[] newArr = new long[nElts];
        for (int i = 0; i < nElts; i++) {
            newArr[i] = getLsn(i);
        }
        newArr[idx] = value;
        entryLsnLongArray = newArr;
        entryLsnByteArray = null;
    }

    /* Will implement this in the future. Note, don't adjust if mutating.*/
    /***
      private void maybeAdjustCapacity(int offset) {
      if (entryLsnLongArray == null) {
      int bytesNeeded = offset + BYTES_PER_LSN_ENTRY;
      int currentBytes = entryLsnByteArray.length;
      if (currentBytes < bytesNeeded) {
      int newBytes = bytesNeeded +
      (GROWTH_INCREMENT * BYTES_PER_LSN_ENTRY);
      byte[] newArr = new byte[newBytes];
      System.arraycopy(entryLsnByteArray, 0, newArr, 0,
      currentBytes);
      entryLsnByteArray = newArr;
      for (int i = currentBytes;
      i < newBytes;
      i += BYTES_PER_LSN_ENTRY) {
      setFileNumberOffset(i, (byte) 0);
      setFileOffset(i, -1);
      }
      }
      } else {
      int currentEntries = entryLsnLongArray.length;
      int idx = offset >> 2;
      if (currentEntries < idx + 1) {
      int newEntries = idx + GROWTH_INCREMENT;
      long[] newArr = new long[newEntries];
      System.arraycopy(entryLsnLongArray, 0, newArr, 0,
      currentEntries);
      entryLsnLongArray = newArr;
      for (int i = currentEntries; i < newEntries; i++) {
      entryLsnLongArray[i] = DbLsn.NULL_LSN;
      }
      }
      }
      }
     ***/

    private boolean adjustFileNumbers(long newBaseFileNumber) {
        long oldBaseFileNumber = baseFileNumber;
        for (int i = 0;
             i < entryLsnByteArray.length;
             i += BYTES_PER_LSN_ENTRY) {
            if (getFileOffset(i) == -1) {
                continue;
            }

            long curEntryFileNumber =
                oldBaseFileNumber + getFileNumberOffset(i);
            long newCurEntryFileNumberOffset =
                (curEntryFileNumber - newBaseFileNumber);
            if (newCurEntryFileNumberOffset > Byte.MAX_VALUE) {
                long undoOffset = oldBaseFileNumber - newBaseFileNumber;
                for (int j = i - BYTES_PER_LSN_ENTRY;
                     j >= 0;
                     j -= BYTES_PER_LSN_ENTRY) {
                    if (getFileOffset(j) == -1) {
                        continue;
                    }
                    setFileNumberOffset
                        (j, (byte) (getFileNumberOffset(j) - undoOffset));
                    //assert getFileNumberOffset(j) >= 0;
                }
                return false;
            }
            setFileNumberOffset(i, (byte) newCurEntryFileNumberOffset);

            //assert getFileNumberOffset(i) >= 0;
        }
        return true;
    }

    private void setFileNumberOffset(int offset, byte fileNumberOffset) {
        entryLsnByteArray[offset] = fileNumberOffset;
    }

    private byte getFileNumberOffset(int offset) {
        return entryLsnByteArray[offset];
    }

    private void setFileOffset(int offset, int fileOffset) {
        put3ByteInt(offset + 1, fileOffset);
    }

    private int getFileOffset(int offset) {
        return get3ByteInt(offset + 1);
    }

    private void put3ByteInt(int offset, int value) {
        entryLsnByteArray[offset++] = (byte) (value >>> 0);
        entryLsnByteArray[offset++] = (byte) (value >>> 8);
        entryLsnByteArray[offset]   = (byte) (value >>> 16);
    }

    private int get3ByteInt(int offset) {
        int ret = (entryLsnByteArray[offset++] & 0xFF) << 0;
        ret += (entryLsnByteArray[offset++] & 0xFF) << 8;
        ret += (entryLsnByteArray[offset]   & 0xFF) << 16;
        if (ret == THREE_BYTE_NEGATIVE_ONE) {
            ret = -1;
        }

        return ret;
    }

    /**
     * @return true if the idx'th entry has been deleted, although the
     * transaction that performed the deletion may not be committed.
     */
    public boolean isEntryPendingDeleted(int idx) {
        return ((entryStates[idx] & PENDING_DELETED_BIT) != 0);
    }

    /**
     * Set pendingDeleted to true.
     */
    public void setPendingDeleted(int idx) {
        entryStates[idx] |= PENDING_DELETED_BIT;
        entryStates[idx] |= DIRTY_BIT;
    }

    /**
     * Set pendingDeleted to false.
     */
    public void clearPendingDeleted(int idx) {
        entryStates[idx] &= CLEAR_PENDING_DELETED_BIT;
        entryStates[idx] |= DIRTY_BIT;
    }

    /**
     * @return true if the idx'th entry is deleted for sure.  If a transaction
     * performed the deletion, it has been committed.
     */
    public boolean isEntryKnownDeleted(int idx) {
        return ((entryStates[idx] & KNOWN_DELETED_BIT) != 0);
    }

    /**
     * Set knownDeleted to true.
     */
    void setKnownDeleted(int idx) {
        entryStates[idx] |= KNOWN_DELETED_BIT;
        entryStates[idx] |= DIRTY_BIT;
    }

    /**
     * Set knownDeleted to false.
     */
    void clearKnownDeleted(int idx) {
        entryStates[idx] &= CLEAR_KNOWN_DELETED_BIT;
        entryStates[idx] |= DIRTY_BIT;
    }

    /**
     * @return true if the object is dirty.
     */
    boolean isDirty(int idx) {
        return ((entryStates[idx] & DIRTY_BIT) != 0);
    }

    /**
     * @return the number of entries in this node.
     */
    public int getNEntries() {
        return nEntries;
    }

    /*
     * In the future we may want to move the following static methods to an
     * EntryState utility class and share all state bit twidling among IN,
     * ChildReference, and DeltaInfo.
     */

    /**
     * Returns true if the given state is known deleted.
     */
    static boolean isStateKnownDeleted(byte state) {
        return ((state & KNOWN_DELETED_BIT) != 0);
    }

    /**
     * Returns true if the given state is pending deleted.
     */
    static boolean isStatePendingDeleted(byte state) {
        return ((state & PENDING_DELETED_BIT) != 0);
    }

    /**
     * @return the maximum number of entries in this node.
     */
    public int getMaxEntries() {
        return entryStates.length;
    }

    /**
     * Variant of fetchTarget that is called while holding an exclusive latch
     * and therefore does not throw RelatchRequiredException.
     */
    public final Node fetchTargetWithExclusiveLatch(int idx)
        throws DatabaseException {

        try {
            return fetchTarget(idx);
        } catch (RelatchRequiredException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Returns the target of the idx'th entry or null if a pendingDeleted or
     * knownDeleted entry has been cleaned.  Note that null can only be
     * returned for a slot that could contain a deleted LN, not other node
     * types and not a DupCountLN since DupCountLNs are never deleted.  Null is
     * also returned for a KnownDeleted slot with a NULL_LSN.
     *
     * @return the target node or null.
     */
    public Node fetchTarget(int idx)
        throws RelatchRequiredException, DatabaseException {

        EnvironmentImpl envImpl = databaseImpl.getDbEnvironment();
        boolean isMiss = false;

        if (entryTargets.get(idx) == null) {
            /* Fault object in from log. */
            long lsn = getLsn(idx);
            if (lsn == DbLsn.NULL_LSN) {
                if (!isEntryKnownDeleted(idx)) {
                    throw EnvironmentFailureException.unexpectedState
                        (makeFetchErrorMsg("NULL_LSN without KnownDeleted",
                                           this, lsn, entryStates[idx]));
                }

                /*
                 * Ignore a NULL_LSN (return null) if KnownDeleted is set.
                 * This is the remnant of an incomplete insertion -- see
                 * Tree.insert. [#13126] or a rollback.
                 */
            } else {
                if (!isLatchOwnerForWrite()) {
                    throw RelatchRequiredException.relatchRequiredException;
                }

                try {
                    LogEntry logEntry =
                        envImpl.getLogManager().
                        getLogEntryAllowInvisibleAtRecovery(lsn);
                    /* Ensure keys are transactionally correct. [#15704] */
                    byte[] lnSlotKey = null;
                    if (logEntry instanceof LNLogEntry) {
                        LNLogEntry lnEntry = (LNLogEntry) logEntry;
                        lnEntry.postFetchInit(databaseImpl);
                        lnSlotKey = lnEntry.getKey();
                    }
                    Node node = (Node) logEntry.getResolvedItem(databaseImpl);
                    node.postFetchInit(databaseImpl, lsn);
                    updateNode(idx, node, lnSlotKey);
                    isMiss = true;
                } catch (FileNotFoundException e) {
                    if (!isEntryKnownDeleted(idx) &&
                        !isEntryPendingDeleted(idx)) {
                        throw new EnvironmentFailureException
                            (envImpl,
                             EnvironmentFailureReason.LOG_FILE_NOT_FOUND,
                             makeFetchErrorMsg(null, this, lsn,
                                               entryStates[idx]),
                             e);
                    }

                    /*
                     * This is a LOG_FILE_NOT_FOUND for a KD or PD entry.
                     *
                     * Ignore. Cleaner got to the log file, so just return
                     * null.  It is safe to ignore a deleted file for a
                     * pendingDeleted entry because the cleaner will not clean
                     * files with active transactions.
                     */
                } catch (EnvironmentFailureException e) {
                    e.addErrorMessage(makeFetchErrorMsg(null, this, lsn,
                                                        entryStates[idx]));
                    throw e;
                } catch (RuntimeException e) {
                    throw new EnvironmentFailureException
                        (envImpl,
                         EnvironmentFailureReason.LOG_INTEGRITY,
                         makeFetchErrorMsg(e.toString(), this, lsn,
                                           entryStates[idx]),
                         e);
                }
            }
        }
        final Node targetNode = entryTargets.get(idx);
        if (targetNode != null) {
            targetNode.incFetchStats(envImpl, isMiss);
        }
        return targetNode;
    }

    static String makeFetchErrorMsg(String msg, IN in, long lsn, byte state) {

        /*
         * Bolster the exception with the LSN, which is critical for
         * debugging. Otherwise, the exception propagates upward and loses the
         * problem LSN.
         */
        StringBuilder sb = new StringBuilder();
        sb.append("fetchTarget of ");
        if (lsn == DbLsn.NULL_LSN) {
            sb.append("null lsn");
        } else {
            sb.append(DbLsn.getNoFormatString(lsn));
        }
        if (in != null) {
            sb.append(" parent IN=").append(in.getNodeId());
            sb.append(" IN class=").append(in.getClass().getName());
            sb.append(" lastFullVersion=");
            sb.append(DbLsn.getNoFormatString(in.getLastFullVersion()));
            sb.append(" lastLoggedVersion=");
            sb.append(DbLsn.getNoFormatString(in.getLastLoggedVersion()));
            sb.append(" parent.getDirty()=").append(in.getDirty());
        }
        sb.append(" state=").append(state);
        if (msg != null) {
            sb.append(" ").append(msg);
        }
        return sb.toString();
    }

    /*
     * All methods that modify the entry array must adjust memory sizing.
     */

    /**
     * Set the idx'th entry of this node using the specified entry of another
     * node.  Do not validate the LSN, just set it unconditionally.  Used for
     * moving entries between BINs during splits.
     *
     * WARNING: This method bumps nEntries if it is less than idx+1,
     * effectively appending the slot.
     */
    void copyEntry(int idx, IN from, int fromIdx) {

        final Node target = from.entryTargets.get(fromIdx);
        final byte[] keyVal = from.getKey(fromIdx);
        final long lsn = from.getLsn(fromIdx);
        final byte state = from.entryStates[fromIdx];

        long oldSize = computeLsnOverhead();
        int newNEntries = idx + 1;

        if (newNEntries > nEntries) {

            /*
             * If the new entry is going to bump nEntries, then we don't need
             * the entry size accounting included in oldSize.
             */
            nEntries = newNEntries;
        } else {
            oldSize += getEntryInMemorySize(idx);
        }

        entryTargets = entryTargets.set(idx, target, this);
        boolean multiSlotChange = setKeyAndPrefix(idx, keyVal);

        /* setLsnElement can mutate to an array of longs. */
        setLsnElement(idx, lsn);
        entryStates[idx] = state;
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSize = getEntryInMemorySize(idx) + computeLsnOverhead();
            updateMemorySize(oldSize, newSize);
        }
        setDirty(true);
    }

    /**
     * Set the LSN for the idx'th entry of this node.
     */
    private void setLsn(int idx, long newLsn) {
        if (shouldUpdateLsn(getLsn(idx), newLsn)) {
            setLsnNoValidation(idx, newLsn);
        }
    }

    /**
     * For a deferred write database, ensure that information is not lost when
     * a new LSN is assigned.  Also ensures that a transient LSN is not
     * accidentally assigned to a persistent entry.
     *
     * Because this method uses strict checking, prepareForSlotReuse must
     * sometimes be called when a new logical entry is being placed in a slot,
     * e.g., during an IN split or an LN slot reuse.
     *
     * The following transition is a NOOP and the LSN is not set:
     *   Any LSN to same value.
     * The following transitions are allowed and cause the LSN to be set:
     *   Null LSN to transient LSN
     *   Null LSN to persistent LSN
     *   Transient LSN to persistent LSN
     *   Persistent LSN to new persistent LSN
     * The following transitions should not occur and throw an exception:
     *   Transient LSN to null LSN
     *   Transient LSN to new transient LSN
     *   Persistent LSN to null LSN
     *   Persistent LSN to transient LSN
     */
    boolean shouldUpdateLsn(long oldLsn, long newLsn) {
        /* Save a little computation in packing/updating an unchanged LSN. */
        if (oldLsn == newLsn) {
            return false;
        }
        /* The rules for a new null LSN can be broken in a read-only env. */
        if (newLsn == DbLsn.NULL_LSN &&
            databaseImpl.getDbEnvironment().isReadOnly()) {
            return true;
        }
        /* Enforce LSN update rules.  Assume newLsn != oldLsn. */
        if (databaseImpl.isDeferredWriteMode()) {
            if (oldLsn != DbLsn.NULL_LSN && DbLsn.isTransientOrNull(newLsn)) {
                throw EnvironmentFailureException.unexpectedState
                    ("DeferredWrite LSN update not allowed" +
                     " oldLsn = " + DbLsn.getNoFormatString(oldLsn) +
                     " newLsn = " + DbLsn.getNoFormatString(newLsn));
            }
        } else {
            if (DbLsn.isTransientOrNull(newLsn)) {
                throw EnvironmentFailureException.unexpectedState
                    ("LSN update not allowed" +
                     " oldLsn = " + DbLsn.getNoFormatString(oldLsn) +
                     " newLsn = " + DbLsn.getNoFormatString(newLsn));
            }
        }
        return true;
    }

    /**
     * Called prior to changing a slot to contain a different logical node.
     * Necessary to support assertions for transient LSNs in shouldUpdateLsn.
     * Examples: LN slot reuse, and splits where a new node is placed in an
     * existing slot.
     */
    public void prepareForSlotReuse(int idx) {
        if (databaseImpl.isDeferredWriteMode()) {
            setLsnNoValidation(idx, DbLsn.NULL_LSN);
        }
    }

    /**
     * Update the idx'th entry of this node. This flavor is used when the
     * target LN is being modified, by an operation like a delete or update.
     * The target is not set by this method, and the nodeForLnSlotKey is used
     * only for debugging checks.  This method allows updating the slot without
     * replacing the target to support delete and update without fetching,
     * impacting the cache, or causing eviction.  The modification of the
     * target is done in the caller, so instead of passing in the old and new
     * nodes, we pass in the old size.
     */
    public void updateNode(int idx,
                           long oldNodeSize,
                           long lsn,
                           byte[] lnSlotKey,
                           Node nodeForLnSlotKey) {
        long oldSize = getEntryInMemorySize(idx);
        boolean multiSlotChange =
            setLNSlotKey(idx, nodeForLnSlotKey, lnSlotKey);
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            /* Update mem size for key change. */
            long newSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSize, newSize);
            /* Update mem size for node change. */
            final Node currentNode = getTarget(idx);
            long newNodeSize = (currentNode != null) ?
                               currentNode.getMemorySizeIncludedByParent() :
                               0;
            updateMemorySize(oldNodeSize, newNodeSize);
        }
        setLsn(idx, lsn);
        setDirty(true);
    }

    /**
     * Update the idx'th entry, replacing the node and, if appropriate, the LN
     * slot key.  See updateNode(int, Node, long, byte[]) for details.
     *
     * Note that the LSN is not passed to this method because the node has been
     * either (a) fetched in from disk and is not dirty, or (b) will be written
     * out later by something like a checkpoint.
     *
     * Note: does not dirty the node unless the LN slot key is changed.
     */
    public void updateNode(int idx, Node node, byte[] lnSlotKey) {
        long oldSize = getEntryInMemorySize(idx);
        setTarget(idx, node);
        boolean multiSlotChange = setLNSlotKey(idx, node, lnSlotKey);
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSize, newSize);
        }
    }

    /**
     * Update the idx'th entry, replacing the node and, if appropriate, the LN
     * slot key.
     *
     * The updateNode methods are special versions of updateEntry that are
     * called to update the node in a slot.  When an LN node is changed, the
     * slot key may also need to be updated when a partial key comparator is
     * used.  Callers must be sure to pass the correct lnSlotKey parameter when
     * passing an LN for the node parameter.  See setLNSlotKey for details.
     * [#15704]
     */
    public void updateNode(int idx, Node node, long lsn, byte[] lnSlotKey) {
        long oldSize = getEntryInMemorySize(idx);
        setLsn(idx, lsn);
        setTarget(idx, node);
        boolean multiSlotChange = setLNSlotKey(idx, node, lnSlotKey);
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSize, newSize);
        }
        setDirty(true);
    }

    /**
     * Sets the idx'th key of this node if it is not identical to the given
     * key, and the node is an LN. [#15704]
     *
     * This method is called when an LN is fetched in order to ensure the key
     * slot is transactionally correct.  A key can change in three
     * circumstances, when a key comparator is configured that may not compare
     * all bytes of the key:
     *
     * 1) The user calls Cursor.putCurrent to update the data of a duplicate
     * data item.  CursorImpl.putCurrent will call this method indirectly to
     * update the key.
     *
     * 2) A transaction aborts or a BIN becomes out of date due to the
     * non-transactional nature of INs.  The Node is set to null during abort
     * and recovery.  IN.fetchCurrent will call this method indirectly to
     * update the key.
     *
     * 3) A slot for a deleted LN is reused.  The key in the slot is updated
     * by IN.updateEntry along with the node and LSN.
     *
     * Note that transaction abort and recovery of BIN entries may cause
     * incorrect keys to be present in the tree, since these entries are
     * non-transactional.  However, an incorrect key in a BIN slot may only be
     * present when the node in that slot is null.  Undo/redo sets the node to
     * null.  When the LN node is fetched, the key in the slot is set to the
     * LN's key, which is the source of truth and is transactionally correct.
     *
     * @param node is the node that is being set in the slot.  The newKey is
     * set only if the node is an LN (and is non-null).
     *
     * @param newKey is the key to set in the slot and is the LN key.  It may
     * be null if it is known that the key cannot be changed (as in putCurrent
     * in a BIN).  It must be null if the node is not an LN.
     *
     * @return true if a multi-slot change was made and the complete IN memory
     * size must be updated.
     */
    private boolean setLNSlotKey(int idx, Node node, byte[] newKey) {

        assert newKey == null || node instanceof LN;

        /*
         * The new key may be null if a dup LN was deleted, in which case there
         * is no need to update it.  There is no need to compare keys if there
         * is no comparator configured, since a key cannot be changed when the
         * default comparator is used.
         */
        if (newKey != null &&
            getKeyComparator() != null &&
            !Arrays.equals(newKey, getKey(idx))) {
            setDirty(true);
            return setKeyAndDirty(idx, newKey);
        } else {
            return false;
        }
    }

    /**
     * Update the idx'th entry of this node.
     *
     * Note that although this method allows updating the node, it always
     * replaces the key and therefore does not need an lnSlotKey parameter.
     * See the updateNode methods for more information.  [#15704]
     */
    public void updateEntry(int idx, Node node, long lsn, byte[] key) {
        long oldSize = getEntryInMemorySize(idx);
        setLsn(idx, lsn);
        setTarget(idx, node);
        boolean multiSlotChange = setKeyAndDirty(idx, key);
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSize, newSize);
        }
        setDirty(true);
    }

    /**
     * Update the idx'th entry of this node.
     */
    public void updateEntry(int idx, long lsn) {
        setLsn(idx, lsn);
        setDirty(true);
    }

    /**
     * Update the idx'th entry of this node.
     */
    public void updateEntry(int idx, long lsn, byte state) {
        setLsn(idx, lsn);
        entryStates[idx] = state;
        setDirty(true);
    }

    /**
     * Update the idx'th entry of this node.  Only update the key if the new
     * key is less than the existing key.
     */
    private void updateEntryCompareKey(int idx,
                                       Node node,
                                       long lsn,
                                       byte[] key) {
        long oldSize = getEntryInMemorySize(idx);
        setLsn(idx, lsn);
        setTarget(idx, node);
        byte[] existingKey = getKey(idx);
        int s = Key.compareKeys(key, existingKey, getKeyComparator());
        boolean multiSlotChange = false;
        if (s < 0) {
            multiSlotChange = setKeyAndDirty(idx, key);
        }
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        } else {
            long newSize = getEntryInMemorySize(idx);
            updateMemorySize(oldSize, newSize);
        }
        setDirty(true);
    }

    /*
     * Memory usage calculations.
     */
    public boolean verifyMemorySize() {

        long calcMemorySize = computeMemorySize();
        if (calcMemorySize != inMemorySize) {

            String msg = "-Warning: Out of sync. " +
                "Should be " + calcMemorySize +
                " / actual: " +
                inMemorySize + " node: " + getNodeId();
            LoggerUtils.envLogMsg
                (Level.INFO, databaseImpl.getDbEnvironment(), msg);

            System.out.println(msg);
            printMemorySize();

            return false;
        }
        return true;
    }

    /**
     * Returns the amount of memory currently budgeted for this IN.
     */
    public long getBudgetedMemorySize() {
        return inMemorySize - accumulatedDelta;
    }

    /**
     * Called as part of a memory budget reset (during a checkpoint) to clear
     * the accumulated delta and return the total memory size.
     */
    public long resetAndGetMemorySize() {
        accumulatedDelta = 0;
        return inMemorySize;
    }

    /**
     * Returns the treeAdmin memory in objects referenced by this IN.
     * Specifically, this refers to the DbFileSummaryMap held by
     * MapLNs
     */
    public long getTreeAdminMemorySize() {
        return 0;  // by default, no treeAdminMemory
    }

    /**
     * For unit tests.
     */
    public long getInMemorySize() {
        return inMemorySize;
    }

    private long getEntryInMemorySize(int idx) {

        /*
         * Do not count state size here, since it is counted as overhead
         * during initialization.
         */
        long ret = 0;

        /*
         * Don't count the key size if the representation has already
         * accounted for it.
         */
        if (!entryKeyVals.accountsForKeyByteMemUsage()) {

            /*
             * Materialize the key object only if needed, thus avoiding the
             * object allocation cost when possible.
             */
            final byte[] key = entryKeyVals.get(idx);
            if (key != null) {
                ret += MemoryBudget.byteArraySize(key.length);
            }
        }

        final Node target = entryTargets.get(idx);
        if (target != null) {
            ret += target.getMemorySizeIncludedByParent();
        }
        return ret;
    }

    /* For unit testing */
    public INArrayRep<INTargetRep, INTargetRep.Type, Node> getTargets() {
        return entryTargets;
    }

    /* For unit testing */
    public INKeyRep getKeyVals() {
        return entryKeyVals;
    }

    /**
     * Count up the memory usage attributable to this node alone. LNs children
     * are counted by their BIN parents, but INs are not counted by their
     * parents because they are resident on the IN list.  The identifierKey is
     * "intentionally" not kept track of in the memory budget.
     */
    public long computeMemorySize() {
        long calcMemorySize = getFixedMemoryOverhead();

        calcMemorySize += MemoryBudget.byteArraySize(entryStates.length);

        calcMemorySize += computeLsnOverhead();

        for (int i = 0; i < nEntries; i++) {
            calcMemorySize += getEntryInMemorySize(i);
        }

        if (keyPrefix != null) {
            calcMemorySize += MemoryBudget.byteArraySize(keyPrefix.length);
        }

        if (provisionalObsolete != null) {
            calcMemorySize += provisionalObsolete.getMemorySize();
        }

        calcMemorySize += entryTargets.calculateMemorySize();
        calcMemorySize += entryKeyVals.calculateMemorySize();

        return calcMemorySize;
    }

    /* Utility method used during unit testing. */
    protected long printMemorySize() {

        final long inOverhead = getFixedMemoryOverhead();

        final long statesOverhead =
            MemoryBudget.byteArraySize(entryStates.length);

        final int lsnOverhead =  computeLsnOverhead();

        int entryOverhead = 0;
        for (int i = 0; i < nEntries; i++) {
            entryOverhead += getEntryInMemorySize(i);
        }

        final int keyPrefixOverhead =  (keyPrefix != null) ?
            MemoryBudget.byteArraySize(keyPrefix.length) : 0;

        final int provisionalOverhead = (provisionalObsolete != null) ?
            provisionalObsolete.getMemorySize() : 0;

        final long targetRepOverhead = entryTargets.calculateMemorySize();
        final long keyRepOverhead = entryKeyVals.calculateMemorySize();
        final long total = inOverhead + statesOverhead + lsnOverhead +
             entryOverhead + keyPrefixOverhead +  provisionalOverhead +
             targetRepOverhead + keyRepOverhead;

        System.out.println(" nEntries:" + nEntries +
                           "/" + entryStates.length +
                           " in: " + inOverhead +
                           " states: " + statesOverhead +
                           " entry: " + entryOverhead +
                           " lsn: " + lsnOverhead +
                           " keyPrefix: " + keyPrefixOverhead +
                           " provisional: " + provisionalOverhead +
                           " targetRep(" + entryTargets.getType() + "): " +
                           targetRepOverhead +
                           " keyRep(" + entryKeyVals.getType() +"): " +
                           keyRepOverhead +
                           " Total: " + total +
                           " inMemorySize: " + inMemorySize);
        return total;
    }

    /* Utility method used to print memory size in an assertion. */
    private boolean assertPrintMemorySize() {
        printMemorySize();
        return true;
    }

    private int computeLsnOverhead() {
        return (entryLsnLongArray == null) ?
            MemoryBudget.byteArraySize(entryLsnByteArray.length) :
            MemoryBudget.ARRAY_OVERHEAD +
                (entryLsnLongArray.length *
                 MemoryBudget.PRIMITIVE_LONG_ARRAY_ITEM_OVERHEAD);
    }

    /* Overridden by subclasses. */
    protected long getFixedMemoryOverhead() {
        return MemoryBudget.IN_FIXED_OVERHEAD;
    }

    protected void updateMemorySize(long oldSize, long newSize) {
        long delta = newSize - oldSize;
        changeMemorySize(delta);
    }

    /*
     * Compacts the representation of the BIN, currently just the
     * representation used by entryTargets if possible. Typically invoked after
     * a BIN has been stripped.
     */
    public void compactMemory() {

       final INKeyRep oldKeyRep = entryKeyVals;

       entryTargets = entryTargets.compact(this);
       entryKeyVals = entryKeyVals.compact(this);

       /*
        * Note that we only need to account for mem usage changes in the key
        * rep here, not the target rep.  The target rep, unlike the key rep,
        * updates its mem usage internally, and the responsibility for mem
        * usage of contained nodes is fixed -- it is always managed by the IN.
        */
       if (entryKeyVals.accountsForKeyByteMemUsage() !=
           oldKeyRep.accountsForKeyByteMemUsage()) {

           /*
            * When the key rep accountsForKeyByteMemUsage property changes,
            * punt and recalc the size of the entire IN, because responsibility
            * for managing contained key byte mem usage has shifted between the
            * key rep and the IN parent.
            */
           updateMemorySize(inMemorySize, computeMemorySize());
           return;
       }

       /*
        * Otherwise, adjust only the size difference between the old and new
        * reps.  This is much faster than computing the entire IN size
        * [#20845].
        */
       if (entryKeyVals != oldKeyRep) {
           updateMemorySize(oldKeyRep.calculateMemorySize(),
                            entryKeyVals.calculateMemorySize());
       }
    }

    protected int getCompactMaxKeyLength() {
        return databaseImpl.getDbEnvironment().getCompactMaxKeyLength();
    }

    void updateMemorySize(Node oldNode, Node newNode) {
        long delta = 0;
        if (newNode != null) {
            delta = newNode.getMemorySizeIncludedByParent();
        }

        if (oldNode != null) {
            delta -= oldNode.getMemorySizeIncludedByParent();
        }
        changeMemorySize(delta);
    }

    void changeMemorySize(long delta) {
        if (delta == 0) {
            return;
        }
        inMemorySize += delta;

        /*
         * Only update the environment cache usage stats if this IN is actually
         * on the IN list. For example, when we create new INs, they are
         * manipulated off the IN list before being added; if we updated the
         * environment wide cache then, we'd end up double counting.
         */
        if (inListResident) {
            final EnvironmentImpl env = databaseImpl.getDbEnvironment();

            assert (inMemorySize >= getFixedMemoryOverhead()) :
                "delta: " + delta + " inMemorySize: " + inMemorySize +
                " overhead: " + getFixedMemoryOverhead() +
                " computed: " + computeMemorySize() +
                " dump: " + toString() + assertPrintMemorySize();

            accumulatedDelta += delta;
            if (accumulatedDelta > ACCUMULATED_LIMIT ||
                accumulatedDelta < -ACCUMULATED_LIMIT) {
                env.getInMemoryINs().memRecalcUpdate(this, accumulatedDelta);
                env.getMemoryBudget().updateTreeMemoryUsage(accumulatedDelta);
                accumulatedDelta = 0;
            }
        }
    }

    /**
     * Called when adding/removing this IN to/from the INList.
     */
    public void setInListResident(boolean resident) {

        if (!resident) {
            /* Decrement the stats before clearing its residency */
            entryTargets.updateCacheStats(resident, this);
            entryKeyVals.updateCacheStats(resident, this);
        }

        inListResident = resident;

        if (resident) {
            /* Increment the stats after setting its residency. */
            entryTargets.updateCacheStats(resident, this);
            entryKeyVals.updateCacheStats(resident, this);
        }
    }

    /**
     * Returns whether this IN is on the INList.
     */
    public boolean getInListResident() {
        return inListResident;
    }

    /**
     * Returns whether the given key is greater than or equal to the first key
     * in the IN and less than or equal to the last key in the IN.  This method
     * is used to determine whether a key to be inserted belongs in this IN,
     * without doing a tree search.  If false is returned it is still possible
     * that the key belongs in this IN, but a tree search must be performed to
     * find out.
     */
    public boolean isKeyInBounds(byte[] keyVal) {

        if (nEntries < 2) {
            return false;
        }

        Comparator<byte[]> userCompareToFcn = getKeyComparator();
        int cmp;
        byte[] myKey;

        /* Compare key given to my first key. */
        myKey = getKey(0);
        cmp = Key.compareKeys(keyVal, myKey, userCompareToFcn);
        if (cmp < 0) {
            return false;
        }

        /* Compare key given to my last key. */
        myKey = getKey(nEntries - 1);
        cmp = Key.compareKeys(keyVal, myKey, userCompareToFcn);
        if (cmp > 0) {
            return false;
        }

        return true;
    }

    public int findEntry(byte[] key,
                         boolean indicateIfDuplicate,
                         boolean exact) {
        return findEntry(key, indicateIfDuplicate, exact,
                         null /*searchComparator*/);
    }

    /**
     * Find the entry in this IN for which key is LTE the key arg.
     *
     * Currently uses a binary search, but eventually, this may use binary or
     * linear search depending on key size, number of entries, etc.
     *
     * This method guarantees that the key parameter, which is the user's key
     * parameter in user-initiated search operations, is always the left hand
     * parameter to the Comparator.compare method.  This allows a comparator
     * to perform specialized searches, when passed down from upper layers.
     *
     * This is public so that DbCursorTest can access it.
     *
     * Note that the 0'th entry's key is treated specially in an IN.  It always
     * compares lower than any other key.
     *
     * @param key - the key to search for.
     * @param indicateIfDuplicate - true if EXACT_MATCH should
     * be or'd onto the return value if key is already present in this node.
     * @param exact - true if an exact match must be found.
     * @return offset for the entry that has a key LTE the arg.  0 if key
     * is less than the 1st entry.  -1 if exact is true and no exact match
     * is found.  If indicateIfDuplicate is true and an exact match was found
     * then EXACT_MATCH is or'd onto the return value.
     */
    public int findEntry(byte[] key,
                         boolean indicateIfDuplicate,
                         boolean exact,
                         Comparator<byte[]> searchComparator) {
        int high = nEntries - 1;
        int low = 0;
        int middle = 0;

        Comparator<byte[]> userCompareToFcn = (searchComparator != null) ?
            searchComparator :
            databaseImpl.getKeyComparator();

        /*
         * Special Treatment of 0th Entry
         * ------------------------------
         * IN's are special in that they have a entry[0] where the key is a
         * virtual key in that it always compares lower than any other key.
         * BIN's don't treat key[0] specially.  But if the caller asked for an
         * exact match or to indicate duplicates, then use the key[0] and
         * forget about the special entry zero comparison.
         *
         * We always use inexact searching to get down to the BIN, and then
         * call findEntry separately on the BIN if necessary.  So the behavior
         * of findEntry is different for BINs and INs, because it's used in
         * different ways.
         *
         * Consider a tree where the lowest key is "b" and we want to insert
         * "a".  If we did the comparison (with exact == false), we wouldn't
         * find the correct (i.e.  the left) path down the tree.  So the
         * virtual key ensures that "a" gets inserted down the left path.
         *
         * The insertion case is a good specific example.  findBinForInsert
         * does inexact searching in the INs only, not the BIN.
         *
         * There's nothing special about the 0th key itself, only the use of
         * the 0th key in the comparison algorithm.
         */
        boolean entryZeroSpecialCompare =
            entryZeroKeyComparesLow() && !exact && !indicateIfDuplicate;

        assert nEntries >= 0;

        while (low <= high) {
            middle = (high + low) / 2;
            int s;
            byte[] middleKey = null;
            if (middle == 0 && entryZeroSpecialCompare) {
                s = 1;
            } else {
                middleKey = getKey(middle);
                s = Key.compareKeys(key, middleKey, userCompareToFcn);
            }
            if (s < 0) {
                high = middle - 1;
            } else if (s > 0) {
                low = middle + 1;
            } else {
                int ret;
                if (indicateIfDuplicate) {
                    ret = middle | EXACT_MATCH;
                } else {
                    ret = middle;
                }

                if ((ret >= 0) && exact && isEntryKnownDeleted(ret & 0xffff)) {
                    return -1;
                } else {
                    return ret;
                }
            }
        }

        /*
         * No match found.  Either return -1 if caller wanted exact matches
         * only, or return entry for which arg key is > entry key.
         */
        if (exact) {
            return -1;
        } else {
            return high;
        }
    }

    /**
     * Inserts the argument ChildReference into this IN.  Assumes this node is
     * already latched by the caller.
     *
     * @param entry The ChildReference to insert into the IN.
     *
     * @return true if the entry was successfully inserted, false
     * if it was a duplicate.
     *
     * @throws EnvironmentFailureException if the node is full
     * (it should have been split earlier).
     */
    public boolean insertEntry(ChildReference entry)
        throws DatabaseException {

        return (insertEntry1(entry) & INSERT_SUCCESS) != 0;
    }

    /**
     * Same as insertEntry except that it returns the index where the dup was
     * found instead of false.  The return value is |'d with either
     * INSERT_SUCCESS or EXACT_MATCH depending on whether the entry was
     * inserted or it was a duplicate, resp.
     *
     * This returns a failure if there's a duplicate match. The caller must do
     * the processing to check if the entry is actually deleted and can be
     * overwritten. This is foisted upon the caller rather than handled in this
     * object because there may be some latch releasing/retaking in order to
     * check a child LN.
     *
     * Inserts the argument ChildReference into this IN.  Assumes this node is
     * already latched by the caller.
     *
     * @param entry The ChildReference to insert into the IN.
     *
     * @return either (1) the index of location in the IN where the entry was
     * inserted |'d with INSERT_SUCCESS, or (2) the index of the duplicate in
     * the IN |'d with EXACT_MATCH if the entry was found to be a duplicate.
     *
     * @throws EnvironmentFailureException if the node is full (it should have
     * been split earlier).
     */
    public int insertEntry1(ChildReference entry) {

        if (nEntries >= getMaxEntries()) {
            throw EnvironmentFailureException.unexpectedState
                ("Node " + getNodeId() +
                 " should have been split before calling insertEntry");
        }

        byte[] key = entry.getKey();

        /*
         * Search without requiring an exact match, but do let us know the
         * index of the match if there is one.
         */
        int index = findEntry(key, true, false);

        if (index >= 0 && (index & EXACT_MATCH) != 0) {

            /*
             * There is an exact match.  Don't insert; let the caller decide
             * what to do with this duplicate.
             */
            return index;
        }

        /* There was no key match, so insert to the right of this entry. */
        index++;

        /* We found a spot for insert, shift entries as needed. */
        if (index < nEntries) {
            int oldSize = computeLsnOverhead();

            /* Adding elements to the LSN array can change the space used. */
            shiftEntriesRight(index);
            changeMemorySize(computeLsnOverhead() - oldSize);
        }
        int oldSize = computeLsnOverhead();
        entryTargets = entryTargets.set(index, entry.getTarget(), this);
        /* setLsnElement can mutate to an array of longs. */
        setLsnElement(index, entry.getLsn());
        entryStates[index] = entry.getState();
        nEntries++;
        boolean multiSlotChange = setKeyAndPrefix(index, key);
        adjustCursorsForInsert(index);
        updateMemorySize(oldSize,
                         getEntryInMemorySize(index) +
                         computeLsnOverhead());
        setDirty(true);
        if (multiSlotChange) {
            updateMemorySize(inMemorySize, computeMemorySize());
        }
        return (index | INSERT_SUCCESS);
    }

    /**
     * Deletes the ChildReference with the key arg from this IN.  Assumes this
     * node is already latched by the caller.
     *
     * This seems to only be used by INTest.
     *
     * @param key The key of the reference to delete from the IN.
     *
     * @param maybeValidate true if assert validation should occur prior to
     * delete.  Set this to false during recovery.
     *
     * @return true if the entry was successfully deleted, false if it was not
     * found.
     */
    boolean deleteEntry(byte[] key, boolean maybeValidate)
        throws DatabaseException {

        if (nEntries == 0) {
            return false; // caller should put this node on the IN cleaner list
        }

        int index = findEntry(key, false, true);
        if (index < 0) {
            return false;
        }

        return deleteEntry(index, maybeValidate);
    }

    /**
     * Deletes the ChildReference at index from this IN.  Assumes this node is
     * already latched by the caller.
     *
     * @param index The index of the entry to delete from the IN.
     *
     * @param maybeValidate true if asserts are enabled.
     *
     * @return true if the entry was successfully deleted, false if it was not
     * found.
     */
    public boolean deleteEntry(int index, boolean maybeValidate)
        throws DatabaseException {

        if (nEntries == 0) {
            return false;
        }

        /* Check the subtree validation only if maybeValidate is true. */
        assert maybeValidate ?
            validateSubtreeBeforeDelete(index) :
            true;

        if (index < nEntries) {
            updateMemorySize(getEntryInMemorySize(index), 0);
            int oldLSNArraySize = computeLsnOverhead();
            /* LSNArray.setElement can mutate to an array of longs. */
            copyEntries(index + 1, index, nEntries - index - 1);
            clearEntry(nEntries - 1);
            updateMemorySize(oldLSNArraySize, computeLsnOverhead());
            nEntries--;
            setDirty(true);
            setProhibitNextDelta();

            /*
             * Note that we don't have to adjust cursors for delete, since
             * there should be nothing pointing at this record.
             */
            traceDelete(Level.FINEST, index);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Do nothing since INs don't support deltas.
     */
    public void setProhibitNextDelta() {
    }

    @Override
    public boolean isIN() {
        return true;
    }

    /*
     * Validate the subtree that we're about to delete.  Make sure there aren't
     * more than one valid entry on each IN and that the last level of the tree
     * is empty. Also check that there are no cursors on any bins in this
     * subtree. Assumes caller is holding the latch on this parent node.
     *
     * While we could latch couple down the tree, rather than hold latches as
     * we descend, we are presumably about to delete this subtree so
     * concurrency shouldn't be an issue.
     *
     * @return true if the subtree rooted at the entry specified by "index" is
     * ok to delete.
     */
    boolean validateSubtreeBeforeDelete(int index)
        throws DatabaseException {

        if (index >= nEntries) {

            /*
             * There's no entry here, so of course this entry is deletable.
             */
            return true;
        } else {
            Node child = fetchTargetWithExclusiveLatch(index);
            return child != null && child.isValidForDelete();
        }
    }

    /**
     * Return true if this node needs splitting.  For the moment, needing to be
     * split is defined by there being no free entries available.
     */
    public boolean needsSplitting() {
        return ((getMaxEntries() - nEntries) < 1);
    }

    /**
     * Indicates whether whether entry 0's key is "special" in that it always
     * compares less than any other key.  BIN's don't have the special key, but
     * IN's do.
     */
    boolean entryZeroKeyComparesLow() {
        return true;
    }

    /**
     * Split this into two nodes.  Parent IN is passed in parent and should be
     * latched by the caller.
     *
     * childIndex is the index in parent of where "this" can be found.
     */
    void split(IN parent, int childIndex, int maxEntries, CacheMode cacheMode)
        throws DatabaseException {

        splitInternal(parent, childIndex, maxEntries, -1, cacheMode);
    }

    protected void splitInternal(IN parent,
                                 int childIndex,
                                 int maxEntries,
                                 int splitIndex,
                                 CacheMode cacheMode)
        throws DatabaseException {

        /*
         * Find the index of the existing identifierKey so we know which IN
         * (new or old) to put it in.
         */
        if (identifierKey == null) {
            throw EnvironmentFailureException.unexpectedState();
        }
        int idKeyIndex = findEntry(identifierKey, false, false);

        if (splitIndex < 0) {
            splitIndex = nEntries / 2;
        }

        /* Range of entries to copy to new sibling. */
        final int low, high;

        if (idKeyIndex < splitIndex) {

            /*
             * Current node (this) keeps left half entries.  Right half entries
             * will go in the new node.
             */
            low = splitIndex;
            high = nEntries;
        } else {

            /*
             * Current node (this) keeps right half entries.  Left half entries
             * and entry[0] will go in the new node.
             */
            low = 0;
            high = splitIndex;
        }

        byte[] newIdKey = getKey(low);
        long parentLsn = DbLsn.NULL_LSN;

        IN newSibling = createNewInstance(newIdKey, maxEntries, level);
        newSibling.latch(cacheMode);

        try {
            int toIdx = 0;
            boolean deletedEntrySeen = false;
            BINReference binRef = null;
            for (int i = low; i < high; i++) {
                if (isEntryPendingDeleted(i)) {
                    if (!deletedEntrySeen) {
                        deletedEntrySeen = true;
                        assert newSibling.isBIN();
                        binRef = ((BIN) newSibling).createReference();
                    }
                }
                newSibling.copyEntry(toIdx++, this, i);
                clearEntry(i);
            }

            /*
             * Always add to compressor queue since a full BIN is logged below,
             * and never a delta.
             */
            if (deletedEntrySeen) {
                databaseImpl.getDbEnvironment().
                    addToCompressorQueue(binRef, false);
            }

            int newSiblingNEntries = (high - low);

            /*
             * Remove the entries that we just copied into newSibling from this
             * node.
             */
            if (low == 0) {
                shiftEntriesLeft(newSiblingNEntries);
            }

            newSibling.nEntries = toIdx;
            nEntries -= newSiblingNEntries;
            setDirty(true);

            adjustCursors(newSibling, low, high);

            /*
             * If this node has no key prefix, calculate it now that it has
             * been split.  This must be done before logging, to ensure the
             * prefix information is made persistent [#20799].
             */
            byte[] newKeyPrefix = computeKeyPrefix(-1);
            recalcSuffixes(newKeyPrefix, null, -1);
            /* Apply compaction after prefixing [#20799]. */
            entryKeyVals = entryKeyVals.compact(this);

            /* Only recalc if there are multiple entries in newSibling. */
            if (newSibling.getNEntries() > 1) {
                byte[] newSiblingPrefix = newSibling.getKeyPrefix();
                newSiblingPrefix = newSibling.computeKeyPrefix(-1);
                newSibling.recalcSuffixes(newSiblingPrefix, null, -1);
                /* initMemorySize calls entryKeyVals.compact. */
                newSibling.initMemorySize();
            }

            /*
             * Update size. newSibling and parent are correct, but this IN has
             * had its entries shifted and is not correct.
             *
             * Also, inMemorySize does not reflect changes that may have
             * resulted from key prefixing related changes, it needs to be
             * brought up to date, so update it appropriately for this and the
             * above reason.
             */
            EnvironmentImpl env = databaseImpl.getDbEnvironment();
            INList inMemoryINs = env.getInMemoryINs();
            long oldMemorySize = inMemorySize;
            long newSize = computeMemorySize();
            updateMemorySize(oldMemorySize, newSize);
            inMemoryINs.add(newSibling);

            /*
             * Parent refers to child through an element of the entries array.
             * Depending on which half of the BIN we copied keys from, we
             * either have to adjust one pointer and add a new one, or we have
             * to just add a new pointer to the new sibling.
             *
             * Note that we must use the provisional form of logging because
             * all three log entries must be read atomically. The parent must
             * get logged last, as all referred-to children must preceed
             * it. Provisional entries guarantee that all three are processed
             * as a unit. Recovery skips provisional entries, so the changed
             * children are only used if the parent makes it out to the log.
             */
            LogManager logManager = env.getLogManager();

            long newSiblingLsn =
                newSibling.optionalLogProvisional(logManager, parent);

            long myNewLsn = optionalLogProvisional(logManager, parent);

            /*
             * When we update the parent entry, we use updateEntryCompareKey so
             * that we don't replace the parent's key that points at 'this'
             * with a key that is > than the existing one.  Replacing the
             * parent's key with something > would effectively render a piece
             * of the subtree inaccessible.  So only replace the parent key
             * with something <= the existing one.  See tree/SplitTest.java for
             * more details on the scenario.
             */
            if (low == 0) {

                /*
                 * Change the original entry to point to the new child and add
                 * an entry to point to the newly logged version of this
                 * existing child.
                 */
                parent.prepareForSlotReuse(childIndex);
                if (childIndex == 0) {
                    parent.updateEntryCompareKey(childIndex, newSibling,
                                                 newSiblingLsn, newIdKey);
                } else {
                    parent.updateNode(childIndex, newSibling, newSiblingLsn,
                                      null /*lnSlotKey*/);
                }

                byte[] ourKey = getKey(0);
                boolean insertOk = parent.insertEntry
                    (new ChildReference(this, ourKey, myNewLsn));
                assert insertOk;
            } else {

                /*
                 * Update the existing child's LSN to reflect the newly logged
                 * version and insert new child into parent.
                 */
                if (childIndex == 0) {

                    /*
                     * this's idkey may be < the parent's entry 0 so we need to
                     * update parent's entry 0 with the key for 'this'.
                     */
                    parent.updateEntryCompareKey
                        (childIndex, this, myNewLsn, getKey(0));
                } else {
                    parent.updateNode(childIndex, this, myNewLsn,
                                      null /*lnSlotKey*/);
                }
                boolean insertOk = parent.insertEntry
                    (new ChildReference(newSibling, newIdKey, newSiblingLsn));
                assert insertOk;
            }

            /**
             * Log the parent.  Note that the grandparent slot is not updated
             * with the parent's LSN here; this is done by Tree.forceSplit.
             */
            parentLsn = parent.optionalLog(logManager);

            /*
             * Maintain dirtiness if this is the root, so this parent will be
             * checkpointed. Other parents who are not roots are logged as part
             * of the propagation of splits upwards.
             */
            if (parent.isRoot()) {
                parent.setDirty(true);
            }

            /* Debug log this information. */
            traceSplit(Level.FINE, parent,
                       newSibling, parentLsn, myNewLsn,
                       newSiblingLsn, splitIndex, idKeyIndex, childIndex);
        } finally {
            newSibling.releaseLatch();
        }
    }

    /**
     * Called when we know we are about to split on behalf of a key that is the
     * minimum (leftSide) or maximum (!leftSide) of this node.  This is
     * achieved by just forcing the split to occur either one element in from
     * the left or the right (i.e. splitIndex is 1 or nEntries - 1).
     */
    void splitSpecial(IN parent,
                      int parentIndex,
                      int maxEntriesPerNode,
                      byte[] key,
                      boolean leftSide,
                      CacheMode cacheMode)
        throws DatabaseException {

        int index = findEntry(key, false, false);
        if (leftSide &&
            index == 0) {
            splitInternal(parent, parentIndex, maxEntriesPerNode,
                          1, cacheMode);
        } else if (!leftSide &&
                   index == (nEntries - 1)) {
            splitInternal(parent, parentIndex,
                          maxEntriesPerNode, nEntries - 1, cacheMode);
        } else {
            split(parent, parentIndex, maxEntriesPerNode, cacheMode);
        }
    }

    void adjustCursors(IN newSibling,
                       int newSiblingLow,
                       int newSiblingHigh) {
        /* Cursors never refer to IN's. */
    }

    void adjustCursorsForInsert(int insertIndex) {
        /* Cursors never refer to IN's. */
    }

    /**
     * Convenience method to return the database key comparator.
     */
    public final Comparator<byte[]> getKeyComparator() {
        return databaseImpl.getKeyComparator();
    }

    /**
     * Shift entries to the right starting with (and including) the entry at
     * index. Caller is responsible for incrementing nEntries.
     *
     * @param index - The position to start shifting from.
     */
    private void shiftEntriesRight(int index) {
        copyEntries(index, index + 1, nEntries - index);
        clearEntry(index);
        setDirty(true);
    }

    /**
     * Shift entries starting at the byHowMuch'th element to the left, thus
     * removing the first byHowMuch'th elements of the entries array.  This
     * always starts at the 0th entry.  Caller is responsible for decrementing
     * nEntries.
     *
     * @param byHowMuch - The number of entries to remove from the left side
     * of the entries array.
     */
    private void shiftEntriesLeft(int byHowMuch) {
        copyEntries(byHowMuch, 0, nEntries - byHowMuch);
        for (int i = nEntries - byHowMuch; i < nEntries; i++) {
            clearEntry(i);
        }
        setDirty(true);
    }

    /**
     * Check that the IN is in a valid state.  For now, validity means that the
     * keys are in sorted order and that there are more than 0 entries.
     * maxKey, if non-null specifies that all keys in this node must be less
     * than maxKey.
     * @throws EnvironmentFailureException when implemented.
     */
    @Override
    public void verify(byte[] maxKey)
        throws EnvironmentFailureException {

        /********* never used, but may be used for the basis of a verify()
                   method in the future.
        try {
            Comparator<byte[]> userCompareToFcn =
                (databaseImpl == null ? null : getKeyComparator());

            byte[] key1 = null;
            for (int i = 1; i < nEntries; i++) {
                key1 = entryKeyVals.get(i);
                byte[] key2 = entryKeyVals.get(i - 1);

                int s = Key.compareKeys(key1, key2, userCompareToFcn);
                if (s <= 0) {
                    throw EnvironmentFailureException.unexpectedState
                        ("IN " + getNodeId() + " key " + (i-1) +
                         " (" + Key.dumpString(key2, 0) +
                         ") and " +
                         i + " (" + Key.dumpString(key1, 0) +
                         ") are out of order");
                }
            }

            boolean inconsistent = false;
            if (maxKey != null && key1 != null) {
                if (Key.compareKeys(key1, maxKey, userCompareToFcn) >= 0) {
                    inconsistent = true;
                }
            }

            if (inconsistent) {
                throw EnvironmentFailureException.unexpectedState
                    ("IN " + getNodeId() +
                     " has entry larger than next entry in parent.");
            }
        } catch (DatabaseException DE) {
            DE.printStackTrace(System.out);
        }
        *****************/
    }

    /**
     * Add self and children to this in-memory IN list. Called by recovery, can
     * run with no latching.
     */
    @Override
    void rebuildINList(INList inList)
        throws DatabaseException {

        /*
         * Recompute your in memory size first and then add yourself to the
         * list.
         */
        initMemorySize();
        inList.add(this);

        /*
         * Add your children if they're resident. (LNs know how to stop the
         * flow).
         */
        for (int i = 0; i < nEntries; i++) {
            Node n = getTarget(i);
            if (n != null) {
                n.rebuildINList(inList);
            }
        }
    }

    /**
     * For a regular (not deferred-write) DB, account for a deleted subtree.
     * <p>
     * Remove self and children from the in-memory IN list.  Also count removed
     * nodes as obsolete in the local tracker.  The local tracker will be
     * flushed by the compressor.
     * <p>
     * TODO: Note that we neglect to transfer any accumulated provisional
     * obsolete info from the deleted INs to the tracker.  See
     * accountForDeferredWriteSubtreeRemoval for the case where this comes up.
     * Ideally, in a future release we will record all obsolete info for
     * regular as well as deferred-write DBs, including any accumulated
     * provisional info, from the deleted INs to the subtree parent, as we do
     * in accountForDeferredWriteSubtreeRemoval, and then rely on the logging
     * of the subtree parent (for a regular DB) to flush that info.  This was
     * not done as part of SR [#21348] because logging the subtree parent does
     * not flush the utilization info all the way to the log, while the
     * compressor does flush the local tracker info all the way to the log.
     * With further thought we may decide this change in behavior is
     * acceptable, but it was too risky for a patch release.  See the SR
     * [#21348] for more details.
     */
    @Override
    void accountForSubtreeRemoval(INList inList,
                                  LocalUtilizationTracker localTracker)
        throws DatabaseException {

        if (nEntries > 1) {
            throw EnvironmentFailureException.unexpectedState
                ("Found non-deletable IN " + getNodeId() +
                 " while flushing INList. nEntries = " + nEntries);
        }

        /* Remove self. */
        inList.remove(this);

        /* Count full and delta versions as obsolete. */
        if (getLastFullVersion() != DbLsn.NULL_LSN) {
            localTracker.countObsoleteNode
                (getLastFullVersion(), getLogType(), 0, databaseImpl);
        }
        if (getLastDeltaVersion() != DbLsn.NULL_LSN) {
            localTracker.countObsoleteNode
                (getLastDeltaVersion(), getLogType(), 0, databaseImpl);
        }

        /* Remove your child, if any.  It should normally be resident. */
        if (nEntries > 0) {
            Node n = fetchTargetWithExclusiveLatch(0);
            if (n != null) {
                n.accountForSubtreeRemoval(inList, localTracker);
            }
        }
    }

    /**
     * For a deferred-write DB, account for a deleted subtree.  [#21348]
     * <p>
     * Remove self and children from the in-memory IN list.  Also count removed
     * nodes as provisionally obsolete, recording this information in the
     * parent of the subtree.  When the root IN is logged non-provisionally by
     * a checkpoint or Database.sync, the provisional obsolete info will be
     * flushed to the log.
     * <p>
     * When removed nodes are counted obsolete, also transfer their accumulated
     * provisional obsolete to the subtree parent.  This accounts for the case
     * where a subtree is removed in the middle of a checkpoint.
     * <p>
     * For example, say a two level subtree is removed, INa-BINb, and the
     * grandparent, INc, is the subtree parent.  Say a checkpoint logged BINb
     * provisionally, but did not log INa (or INc).  The provisional obsolete
     * info for BINb will be present in INa.  In this method, we transfer that
     * info to INc, so it will be flushed when INc is logged non-provisionally.
     */
    @Override
    void accountForDeferredWriteSubtreeRemoval(INList inList, IN subtreeParent)
        throws DatabaseException {

        if (nEntries > 1) {
            throw EnvironmentFailureException.unexpectedState
                ("Found non-deletable IN " + getNodeId() +
                 " while flushing INList. nEntries = " + nEntries);
        }

        /* Remove self. */
        inList.remove(this);

        /* Count full and delta versions as obsolete. */
        if (getLastFullVersion() != DbLsn.NULL_LSN) {
            subtreeParent.trackProvisionalObsolete
                (this, getLastFullVersion(), false /*isObsoleteLN*/, 0);
        }
        if (getLastDeltaVersion() != DbLsn.NULL_LSN) {
            subtreeParent.trackProvisionalObsolete
                (this, getLastDeltaVersion(), false /*isObsoleteLN*/, 0);
        }

        /* Remove your child, if any.  It should normally be resident. */
        if (nEntries > 0) {
            Node n = fetchTargetWithExclusiveLatch(0);
            if (n != null) {
                n.accountForDeferredWriteSubtreeRemoval(inList, subtreeParent);
            }
        }
    }

    /**
     * Check if this node fits the qualifications for being part of a deletable
     * subtree. It can only have one IN child and no LN children.
     *
     * We assume that this is only called under an assert.
     */
    @Override
    boolean isValidForDelete()
        throws DatabaseException {

        /*
         * Can only have one valid child, and that child should be
         * deletable.
         */
        if (nEntries > 1) {            // more than 1 entry.
            return false;
        } else if (nEntries == 1) {    // 1 entry, check child
            Node child = fetchTargetWithExclusiveLatch(0);
            if (child == null) {
                return false;
            }
            // TODO shared latching seems wrong, will call fetchTarget
            child.latchShared(CacheMode.UNCHANGED);
            boolean ret = child.isValidForDelete();
            child.releaseLatch();
            return ret;
        } else {                       // 0 entries.
            return true;
        }
    }

    /**
     * Determine if 'this' is the parent of a child IN (targetNodeId).  If not,
     * find a child of 'this' that may be the parent and return it.  If there
     * are no possibilities, then return null.  Note that the keys of the
     * target are passed in as args so we don't have to latch the target to
     * look at them.  Also, 'this' is latched upon entry.
     *
     * @param doFetch If true, fetch the child in the pursuit of this search.
     * If false, give up if the child is not resident. In that case, we have
     * a potential ancestor, but are not sure if this is the parent.
     *
     * On return, if result.parent is non-null, then the IN that it refers to
     * will be latched.  If an exception is thrown, then "this" is latched.
     */
    void findParent(Tree.SearchType searchType,
                    long targetNodeId,
                    boolean targetIsRoot,
                    byte[] targetTreeKey,
                    SearchResult result,
                    boolean requireExactMatch,
                    CacheMode cacheMode,
                    int targetLevel,
                    boolean doFetch)
        throws RelatchRequiredException, DatabaseException {

        assert isLatchOwnerForWrite();

        /* We are this node -- there's no parent in this subtree. */
        if (getNodeId() == targetNodeId) {
            releaseLatch();
            result.exactParentFound = false;  // no parent exists
            result.keepSearching = false;
            result.parent = null;
            return;
        }

        /* Find an entry */
        if (getNEntries() == 0) {

            /*
             * No more children, can't descend anymore. Return this node, you
             * could be the parent.
             */
            result.keepSearching = false;
            result.exactParentFound = false;
            if (requireExactMatch) {
                releaseLatch();
                result.parent = null;
            } else {
                result.parent = this;
                result.index = -1;
            }
            return;
        }

        /* Look for the entry matching key in the current node. */
        result.index = findEntry(targetTreeKey, false, false);

        if (result.index < 0) {
            result.keepSearching = false;
            result.exactParentFound = false;
            if (requireExactMatch) {
                releaseLatch();
                result.parent = null;
            } else {
                /* This node is going to be the prospective parent. */
                result.parent = this;
            }
            return;
        }

        /*
         * Get the child node that matches.  If fetchTarget returns null, a
         * deleted LN was cleaned.
         */
        IN child = null;
        boolean isDeleted = false;
        if (isEntryKnownDeleted(result.index)) {
            isDeleted = true;
        } else if (doFetch) {
            child = (IN) fetchTarget(result.index);
            if (child == null) {
                isDeleted = true;
            }
        } else {
            child = (IN) getTarget(result.index);
        }

        /* The child is a deleted cleaned entry or is knownDeleted. */
        if (isDeleted) {
            result.exactParentFound = false;
            result.keepSearching = false;
            if (requireExactMatch) {
                result.parent = null;
                releaseLatch();
            } else {
                result.parent = this;
            }
            return;
        }

        /* Try matching by level. */
        if (targetLevel >= 0 && level == targetLevel + 1) {
            result.exactParentFound = true;
            result.parent = this;
            result.keepSearching = false;
            return;
        }

        if (child == null) {
            assert !doFetch;

            /*
             * This node will be the possible parent.
             */
            result.keepSearching = false;
            result.exactParentFound = false;
            result.parent = this;
            result.childNotResident = true;
            return;
        }

        child.latch(cacheMode);
        if (child.getNodeId() == targetNodeId) {
            child.releaseLatch();
            /* We found the child, so this is the parent. */
            result.exactParentFound = true;
            result.parent = this;
            result.keepSearching = false;
            return;
        }

        /*
         * Decide whether we can descend, or the search is going to be
         * unsuccessful or whether this node is going to be the future parent.
         * It depends on what this node is, the target, and the child.
         *
         * If this search can go further, return the child. If it can't, and
         * you are a possible new parent to this child, return this IN. If the
         * search can't go further and this IN can't be a parent to this child,
         * return null.
         */
        if (child.isBIN()) {

            /*
             * Our search ends, we didn't find it. If we need an exact match,
             * give up, if we only need a potential match, keep this node
             * latched and return it.
             */
            child.releaseLatch();
            result.exactParentFound = false;
            result.keepSearching = false;

            if (requireExactMatch) {
                releaseLatch();
                result.parent = null;
            } else {
                result.parent = this;
            }
        } else {
            /* The child is an upper IN.  We can search further. */
            releaseLatch();
            result.parent = child;
        }
    }

    /**
     * Returns whether this node can be evicted.  This is slower than
     * (getEvictionType() == MAY_EVICT_NODE) because it does a more static,
     * stringent check and is used by the evictor after a node has been
     * selected, to check that it is still evictable. The more specific
     * evaluation done by getEvictionType() is used when initially selecting a
     * node for inclusion in the eviction set.
     *
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    public boolean isEvictable() {

        if (isEvictionProhibited()) {
            return false;
        }

        /*
         * An IN can be evicted only if its resident children are all evictable
         * LNs, because those children can be logged (if dirty) and stripped
         * before this node is evicted.  Non-LN children or pinned LNs (MapLNs
         * for open DBs) will prevent eviction.
         */
        if (hasPinnedChildren()) {
            return false;
        }

        for (int i = 0; i < getNEntries(); i++) {
            /* Target and LSN can be null in DW. Not evictable in that case. */
            if (getLsn(i) == DbLsn.NULL_LSN &&
                getTarget(i) == null) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the eviction type for this IN, for use by the evictor.  Uses the
     * internal isEvictionProhibited and getChildEvictionType methods that may
     * be overridden by subclasses.
     *
     * This differs from isEvictable() because it does more detailed evaluation
     * about the degree of evictability. It's used generally when selecting
     * candidates for eviction.
     *
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     *
     * @return MAY_EVICT_LNS if evictable LNs may be stripped; otherwise,
     * MAY_EVICT_NODE if the node itself may be evicted; otherwise,
     * MAY_NOT_EVICT.
     */
    public int getEvictionType() {

        if (isEvictionProhibited()) {
            return MAY_NOT_EVICT;
        } else {
            return getChildEvictionType();
        }
    }

    /**
     * Returns whether the node is not evictable, irrespective of the status
     * of the children nodes.
     *
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    protected boolean isEvictionProhibited() {

        if (isDbRoot()) {

            /*
             * Disallow eviction of a dirty DW DB root, since logging the MapLN
             * (via DbTree.modifyDbRoot) will make the all other changes to the
             * DW DB effectively non-provisional (durable).  This implies that
             * a DW DB root cannot be evicted until it is synced (or removed).
             * [#13415]
             */
            if (databaseImpl.isDeferredWriteMode() && getDirty()) {
                return true;
            }

            /*
             * Disallow eviction of the mapping and naming DB roots, because
             * the use count is not incremented for these DBs.  In addition,
             * their eviction and re-fetching is a special case that is not
             * worth supporting.  [#13415]
             */
            DatabaseId dbId = databaseImpl.getId();
            if (dbId.equals(DbTree.ID_DB_ID) ||
                dbId.equals(DbTree.NAME_DB_ID)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether any resident children are not LNs (are INs).
     * For an IN, that equates to whether there are any resident children
     * at all.
     *
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    boolean hasPinnedChildren() {

        return hasResidentChildren();
    }

    /**
     * Returns the eviction type based on the status of child nodes,
     * irrespective of isEvictionProhibited.
     *
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    int getChildEvictionType() {

        return hasResidentChildren() ? MAY_NOT_EVICT : MAY_EVICT_NODE;
    }

    /**
     * Returns whether any child is non-null.  Is final to indicate it is not
     * overridden (unlike hasPinnedChildren, isEvictionProhibited, etc).
     *
     * Note that the IN may or may not be latched when this method is called.
     * Returning the wrong answer is OK in that case (it will be called again
     * later when latched), but an exception should not occur.
     */
    final boolean hasResidentChildren() {

        for (int i = 0; i < getNEntries(); i++) {
            if (getTarget(i) != null) {
                return true;
            }
        }

        return false;
    }

    /*
     * DbStat support.
     */
    void accumulateStats(TreeWalkerStatsAccumulator acc) {
        acc.processIN(this, Long.valueOf(getNodeId()), getLevel());
    }

    /*
     * Logging support
     */

    /**
     * When splits and checkpoints intermingle in a deferred write databases,
     * a checkpoint target may appear which has a valid target but a null LSN.
     * Deferred write dbs are written out in checkpoint style by either
     * Database.sync() or a checkpoint which has cleaned a file containing
     * deferred write entries. For example,
     *   INa
     *    |
     *   BINb
     *
     *  A checkpoint or Database.sync starts
     *  The INList is traversed, dirty nodes are selected
     *  BINb is bypassed on the INList, since it's not dirty
     *  BINb is split, creating a new sibling, BINc, and dirtying INa
     *  INa is selected as a dirty node for the ckpt
     *
     * If this happens, INa is in the selected dirty set, but not its dirty
     * child BINb and new child BINc.
     *
     * In a durable db, the existence of BINb and BINc are logged
     * anyway. But in a deferred write db, there is an entry that points to
     * BINc, but no logged version.
     *
     * This will not cause problems with eviction, because INa can't be
     * evicted until BINb and BINc are logged, are non-dirty, and are detached.
     * But it can cause problems at recovery, because INa will have a null LSN
     * for a valid entry, and the LN children of BINc will not find a home.
     * To prevent this, search for all dirty children that might have been
     * missed during the selection phase, and write them out. It's not
     * sufficient to write only null-LSN children, because the existing sibling
     * must be logged lest LN children recover twice (once in the new sibling,
     * once in the old existing sibling.
     */
    public void logDirtyChildren()
        throws DatabaseException {

        EnvironmentImpl envImpl = getDatabase().getDbEnvironment();

        /* Look for targets that are dirty. */
        for (int i = 0; i < getNEntries(); i++) {

            IN child = (IN) getTarget(i);
            if (child != null) {
                child.latch(CacheMode.UNCHANGED);
                try {
                    if (child.getDirty()) {
                        /* Ask descendents to log their children. */
                        child.logDirtyChildren();
                        long childLsn =
                            child.log(envImpl.getLogManager(),
                                      false, // allowDeltas
                                      false, // allowCompress
                                      true,  // isProvisional
                                      true,  // backgroundIO
                                      this); // parent
                        updateEntry(i, childLsn);
                    }
                } finally {
                    child.releaseLatch();
                }
            }
        }
    }

    /**
     * Log this IN and clear the dirty flag.
     */
    public long log(LogManager logManager)
        throws DatabaseException {

        return logInternal(logManager,
                           false,  // allowDeltas
                           false,  // allowCompress
                           Provisional.NO,
                           false,  // backgroundIO
                           null);  // parent
    }

    /**
     * Log this node with all available options.
     */
    public long log(LogManager logManager,
                    boolean allowDeltas,
                    boolean allowCompress,
                    boolean isProvisional,
                    boolean backgroundIO,
                    IN parent) // for provisional
        throws DatabaseException {

        return logInternal(logManager,
                           allowDeltas,
                           allowCompress,
                           isProvisional ? Provisional.YES : Provisional.NO,
                           backgroundIO,
                           parent);
    }

    public long log(LogManager logManager,
                    boolean allowDeltas,
                    boolean allowCompress,
                    Provisional provisional,
                    boolean backgroundIO,
                    IN parent) // for provisional
        throws DatabaseException {

        return logInternal(logManager,
                           allowDeltas,
                           allowCompress,
                           provisional,
                           backgroundIO,
                           parent);
    }

    /**
     * Log this IN and clear the dirty flag.
     */
    public long optionalLog(LogManager logManager)
        throws DatabaseException {

        if (databaseImpl.isDeferredWriteMode()) {
            return getLastLoggedVersion();
        } else {
            return logInternal(logManager,
                               false,  // allowDeltas
                               false,  // allowCompress
                               Provisional.NO,
                               false,  // backgroundIO
                               null);  // parent
        }
    }

    /**
     * Log this node provisionally and clear the dirty flag.
     * @param item object to be logged
     * @return LSN of the new log entry
     */
    public long optionalLogProvisional(LogManager logManager, IN parent)
        throws DatabaseException {

        if (databaseImpl.isDeferredWriteMode()) {
            return getLastLoggedVersion();
        } else {
            return logInternal(logManager,
                               false,  // allowDeltas
                               false,  // allowCompress
                               Provisional.YES,
                               false,  // backgroundIO
                               parent);
        }
    }

    /**
     * Bottleneck method for all single-IN logging.  Multi-IN logging uses
     * beforeLog and afterLog instead.
     */
    private long logInternal(LogManager logManager,
                             boolean allowDeltas,
                             boolean allowCompress,
                             Provisional provisional,
                             boolean backgroundIO,
                             IN parent)
        throws DatabaseException {

        INLogItem item = new INLogItem();
        item.provisional = provisional;
        item.parent = parent;
        item.repContext = ReplicationContext.NO_REPLICATE;

        INLogContext context = new INLogContext();
        context.nodeDb = getDatabase();
        context.backgroundIO = backgroundIO;
        context.allowDeltas = allowDeltas;
        context.allowCompress = allowCompress;

        beforeLog(logManager, item, context);
        logManager.log(item, context);
        afterLog(logManager, item, context);

        return item.newLsn;
    }

    /**
     * Pre-log processing.  Used implicitly for single-item logging and
     * explicitly for multi-item logging.  Overridden by subclasses as needed.
     *
     * Decide how to log this node.  INs are always logged in full.  Cleaner LN
     * migration is never performed since it only applies to BINs.
     */
    public void beforeLog(LogManager logManager,
                          INLogItem item,
                          INLogContext context) {
        beforeLogCommon(item, context, lastFullVersion, DbLsn.NULL_LSN);
        item.entry = new INLogEntry(this);
    }

    /**
     * Pre-log processing shared by IN and BIN classes.
     */
    void beforeLogCommon(INLogItem item,
                         INLogContext context,
                         long oldLsn,
                         long auxOldLsn) {

        assert isLatchOwnerForWrite();
        assert item.parent == null || item.parent.isLatchOwnerForWrite();

        /* Count obsolete info when logging non-provisionally. */
        if (countObsoleteDuringLogging(item.provisional)) {
            item.oldLsn = oldLsn;
            item.auxOldLsn = auxOldLsn;
            context.packedObsoleteInfo = provisionalObsolete;
        }
    }

    /**
     * Post-log processing.  Used implicitly for single-item logging and
     * explicitly for multi-item logging.  Overridden by subclasses as needed.
     *
     * The last version of this node must be counted obsolete at the correct
     * time. If logging non-provisionally, the last version of this node and
     * any provisionally logged descendants are immediately obsolete and can be
     * flushed. If logging provisionally, the last version isn't obsolete until
     * an ancestor is logged non-provisionally, so propagate obsolete lsns
     * upwards.
     */
    public void afterLog(LogManager logManager,
                         INLogItem item,
                         INLogContext context) {

        afterLogCommon(logManager, item, context, lastFullVersion,
                       DbLsn.NULL_LSN);

        setLastFullLsn(item.newLsn);
    }

    /**
     * Post-log processing shared by IN and BIN classes.
     */
    void afterLogCommon(LogManager logManager,
                        INLogItem item,
                        INLogContext context,
                        long oldLsn,
                        long auxOldLsn) {

        if (countObsoleteDuringLogging(item.provisional)) {
            discardProvisionalObsolete(logManager);
        } else {
            if (item.parent != null) {
                if (oldLsn != DbLsn.NULL_LSN) {
                    item.parent.trackProvisionalObsolete
                        (this, oldLsn, false /*isObsoleteLN*/, 0);
                }
                if (auxOldLsn != DbLsn.NULL_LSN) {
                    item.parent.trackProvisionalObsolete
                        (this, auxOldLsn, false /*isObsoleteLN*/, 0);
                }
            }
        }

        setDirty(false);

        /*
         * When logging an IN, its parent IN slot should not have KD/PD flags
         * set, since these flags are applicable only to child LNs.  However,
         * in the past these flags were sometimes set incorrectly.  Clear the
         * flags here to guard against possible problems.
         */
        if (item.parent != null && item.parentIndex >= 0) {
            item.parent.clearKnownDeleted(item.parentIndex);
            item.parent.clearPendingDeleted(item.parentIndex);
        }
    }

    /**
     * Returns whether to count the prior version of an IN (as well as
     * accumulated provisionally obsolete LSNs for child nodes) obsolete when
     * logging the new version.
     *
     * True is returned if we are logging the IN non-provisionally, since the
     * non-provisional version durably replaces the prior version and causes
     * all provisional children to also become durable.
     *
     * True is also returned if the database is temporary. Since we never use a
     * temporary DB past recovery, prior versions of an IN are never used.
     * [#16928]
     */
    private boolean countObsoleteDuringLogging(Provisional provisional) {
        return provisional != Provisional.YES ||
               databaseImpl.isTemporary();
    }

    /**
     * Adds the given obsolete LSN and any tracked obsolete LSNs for the given
     * child IN to this IN's tracking list.  This method is called to track
     * obsolete LSNs when a child IN or LN is logged provisionally.  Such LSNs
     * cannot be considered obsolete until an ancestor IN is logged
     * non-provisionally.
     */
    void trackProvisionalObsolete(IN childIN,
                                  long obsoleteLsn,
                                  boolean isObsoleteLN,
                                  int obsoleteSize) {

        int oldMemSize = (provisionalObsolete != null) ?
                         provisionalObsolete.getMemorySize() :
                         0;

        if (childIN != null && childIN.provisionalObsolete != null) {
            if (provisionalObsolete != null) {
                /* Append child info to parent info. */
                provisionalObsolete.copyObsoleteInfo
                    (childIN.provisionalObsolete);
            } else {
                /* Move reference from child to parent. */
                provisionalObsolete = childIN.provisionalObsolete;
            }
            childIN.changeMemorySize
                (0 - childIN.provisionalObsolete.getMemorySize());
            childIN.provisionalObsolete = null;
        }

        if (obsoleteLsn != DbLsn.NULL_LSN) {
            if (provisionalObsolete == null) {
                provisionalObsolete = new PackedObsoleteInfo();
            }
            provisionalObsolete.addObsoleteInfo(obsoleteLsn, isObsoleteLN,
                                                obsoleteSize);
        }

        updateMemorySize(oldMemSize,
                         (provisionalObsolete != null) ?
                         provisionalObsolete.getMemorySize() :
                         0);
    }

    /**
     * Discards the provisional obsolete tracking information in this node
     * after it has been counted in the live tracker.  This method is called
     * after this node is logged non-provisionally.
     */
    private void discardProvisionalObsolete(LogManager logManager)
        throws DatabaseException {

        if (provisionalObsolete != null) {
            changeMemorySize(0 - provisionalObsolete.getMemorySize());
            provisionalObsolete = null;
        }
    }

    /**
     * We categorize eviction stats by the type of IN, so IN subclasses
     * update different stats.
     */
    public void incEvictStats(EvictionSource source) {
        databaseImpl.getDbEnvironment().getEvictor().incINEvictStats(source);
    }

    /**
     * @see Node#incFetchStats
     */
    @Override
    public void incFetchStats(EnvironmentImpl envImpl, boolean isMiss) {
        envImpl.getEvictor().incINFetchStats(isMiss);
    }

    /**
     * @see Node#getGenericLogType
     */
    @Override
    public LogEntryType getGenericLogType() {
        return getLogType();
    }

    /**
     * Get the log type of this node.
     */
    public LogEntryType getLogType() {
        return LogEntryType.LOG_IN;
    }

    /**
     * @see Loggable#getLogSize
     */
    @Override
    public int getLogSize() {
        int size = super.getLogSize();          // ancestors
        size += LogUtils.getPackedLongLogSize(nodeId);
        size += LogUtils.getByteArrayLogSize(identifierKey); // identifier key
        if (keyPrefix != null) {
            size += LogUtils.getByteArrayLogSize(keyPrefix);
        }
        size += 1;                              // isRoot
        size += LogUtils.getPackedIntLogSize(nEntries);
        size += LogUtils.getPackedIntLogSize(level);
        size += LogUtils.getPackedIntLogSize(getMaxEntries());
        size += LogUtils.getBooleanLogSize();   // compactLsnsRep
        boolean compactLsnsRep = (entryLsnLongArray == null);
        if (compactLsnsRep) {
            size += LogUtils.INT_BYTES;         // baseFileNumber
        }

        for (int i = 0; i < nEntries; i++) {    // entries
            size += LogUtils.getByteArrayLogSize(entryKeyVals.get(i)) + // key
                (compactLsnsRep ? LogUtils.INT_BYTES :
                 LogUtils.getLongLogSize()) +                       // LSN
                1;                                                  // state
        }
        return size;
    }

    /**
     * @see Loggable#writeToLog
     */
    @Override
    public void writeToLog(ByteBuffer logBuffer) {

        super.writeToLog(logBuffer);

        LogUtils.writePackedLong(logBuffer, nodeId);
        boolean hasKeyPrefix = (keyPrefix != null);
        LogUtils.writeByteArray(logBuffer, identifierKey);
        byte booleans = (byte) (isRoot() ? 1 : 0);
        booleans |= (hasKeyPrefix ? 2 : 0);
        logBuffer.put(booleans);
        if (hasKeyPrefix) {
            LogUtils.writeByteArray(logBuffer, keyPrefix);
        }
        LogUtils.writePackedInt(logBuffer, nEntries);
        LogUtils.writePackedInt(logBuffer, level);
        LogUtils.writePackedInt(logBuffer, getMaxEntries());

        /* true if compact representation. */
        boolean compactLsnsRep = (entryLsnLongArray == null);
        LogUtils.writeBoolean(logBuffer, compactLsnsRep);
        if (compactLsnsRep) {
            LogUtils.writeInt(logBuffer, (int) baseFileNumber);
        }

        for (int i = 0; i < nEntries; i++) {
            LogUtils.writeByteArray(logBuffer, entryKeyVals.get(i)); // key

            /*
             * A NULL_LSN may be stored when an incomplete insertion occurs,
             * but in that case the KnownDeleted flag must be set. See
             * Tree.insert.  [#13126]
             */
            assert checkForNullLSN(i) :
                "logging IN " + getNodeId() + " with null lsn child " +
                " db=" + databaseImpl.getDebugName() +
                " isDeferredWriteMode=" + databaseImpl.isDeferredWriteMode() +
                " isTemporary=" + databaseImpl.isTemporary();

            if (compactLsnsRep) {                                // LSN
                int offset = i << 2;
                int fileOffset = getFileOffset(offset);
                logBuffer.put(getFileNumberOffset(offset));
                logBuffer.put((byte) ((fileOffset >>> 0) & 0xff));
                logBuffer.put((byte) ((fileOffset >>> 8) & 0xff));
                logBuffer.put((byte) ((fileOffset >>> 16) & 0xff));
            } else {
                LogUtils.writeLong(logBuffer, entryLsnLongArray[i]);
            }
            logBuffer.put(entryStates[i]);                       // state
            entryStates[i] &= CLEAR_DIRTY_BIT;
        }
    }

    /*
     * Used for assertion to prevent writing a null lsn to the log.
     */
    private boolean checkForNullLSN(int index) {
        boolean ok;
        if (isBIN()) {
            ok = !(getLsn(index) == DbLsn.NULL_LSN &&
                   (entryStates[index] & KNOWN_DELETED_BIT) == 0);
        } else {
            ok = (getLsn(index) != DbLsn.NULL_LSN);
        }
        return ok;
    }

    /**
     * @see Loggable#readFromLog
     */
    @Override
    public void readFromLog(ByteBuffer itemBuffer, int entryVersion) {

        super.readFromLog(itemBuffer, entryVersion);

        boolean unpacked = (entryVersion < 6);
        nodeId = LogUtils.readLong(itemBuffer, unpacked);
        identifierKey = LogUtils.readByteArray(itemBuffer, unpacked);
        byte booleans = itemBuffer.get();
        setIsRootFlag((booleans & 1) != 0);
        if ((booleans & 2) != 0) {
            keyPrefix = LogUtils.readByteArray(itemBuffer, unpacked);
        }

        nEntries = LogUtils.readInt(itemBuffer, unpacked);
        level = LogUtils.readInt(itemBuffer, unpacked);
        int length = LogUtils.readInt(itemBuffer, unpacked);

        entryTargets = INTargetRep.NONE;
        entryKeyVals = new INKeyRep.Default(length);
        baseFileNumber = -1;
        long storedBaseFileNumber = -1;
        entryLsnByteArray = new byte[length << 2];
        entryLsnLongArray = null;
        entryStates = new byte[length];
        boolean compactLsnsRep = false;
        if (entryVersion > 1) {
            compactLsnsRep = LogUtils.readBoolean(itemBuffer);
            if (compactLsnsRep) {
                baseFileNumber = LogUtils.readInt(itemBuffer) & 0xffffffff;
                storedBaseFileNumber = baseFileNumber;
            }
        }
        for (int i = 0; i < nEntries; i++) {
            entryKeyVals = entryKeyVals.
                set(i, LogUtils.readByteArray(itemBuffer, unpacked), this);
            long lsn;
            if (compactLsnsRep) {
                /* LSNs in compact form. */
                byte fileNumberOffset = itemBuffer.get();
                int fileOffset = (itemBuffer.get() & 0xff);
                fileOffset |= ((itemBuffer.get() & 0xff) << 8);
                fileOffset |= ((itemBuffer.get() & 0xff) << 16);
                if (fileOffset == THREE_BYTE_NEGATIVE_ONE) {
                    lsn = DbLsn.NULL_LSN;
                } else {
                    lsn = DbLsn.makeLsn
                        (storedBaseFileNumber + fileNumberOffset, fileOffset);
                }
            } else {
                /* LSNs in long form. */
                lsn = LogUtils.readLong(itemBuffer);              // LSN
            }
            setLsnElement(i, lsn);

            byte entryState = itemBuffer.get();                   // state
            entryState &= CLEAR_DIRTY_BIT;
            entryState &= CLEAR_MIGRATE_BIT;

            /*
             * A NULL_LSN is the remnant of an incomplete insertion and the
             * KnownDeleted flag should be set.  But because of bugs in prior
             * releases, the KnownDeleted flag may not be set.  So set it here.
             * See Tree.insert.  [#13126]
             */
            if (lsn == DbLsn.NULL_LSN) {
                entryState |= KNOWN_DELETED_BIT;
            }

            entryStates[i] = entryState;
        }

        latch.setName(shortClassName() + getNodeId());

        /* Dup conversion will be done by postFetchInit. */
        needDupKeyConversion = (entryVersion < 8);
    }

    /**
     * @see Loggable#dumpLog
     */
    @Override
    public void dumpLog(StringBuilder sb, boolean verbose) {
        sb.append(beginTag());

        sb.append("<nodeId val=\"");
        sb.append(nodeId);
        sb.append("\"/>");

        sb.append(Key.dumpString(identifierKey, 0));

        // isRoot
        sb.append("<isRoot val=\"");
        sb.append(isRoot());
        sb.append("\"/>");

        // level
        sb.append("<level val=\"");
        sb.append(Integer.toHexString(level));
        sb.append("\"/>");

        if (keyPrefix != null) {
            sb.append("<keyPrefix>");
            sb.append(Key.dumpString(keyPrefix, 0));
            sb.append("</keyPrefix>");
        }

        // nEntries, length of entries array
        sb.append("<entries numEntries=\"");
        sb.append(nEntries);
        sb.append("\" length=\"");
        sb.append(getMaxEntries());
        boolean compactLsnsRep = (entryLsnLongArray == null);
        if (compactLsnsRep) {
            sb.append("\" baseFileNumber=\"");
            sb.append(baseFileNumber);
        }
        sb.append("\">");

        if (verbose) {
            for (int i = 0; i < nEntries; i++) {
                sb.append("<ref kd=\"").
                    append(isEntryKnownDeleted(i));
                sb.append("\" pd=\"").
                    append(isEntryPendingDeleted(i));
                sb.append("\">");
                sb.append(Key.dumpString(entryKeyVals.get(i), 0));
                sb.append(DbLsn.toString(getLsn(i)));
                sb.append("</ref>");
            }
        }

        sb.append("</entries>");

        /* Add on any additional items from subclasses before the end tag. */
        dumpLogAdditional(sb);

        sb.append(endTag());
    }

    /**
     * @see Loggable#logicalEquals
     * Always return false, this item should never be compared.
     */
    public boolean logicalEquals(Loggable other) {
        return false;
    }

    /**
     * Allows subclasses to add additional fields before the end tag. If they
     * just overload dumpLog, the xml isn't nested.
     */
    protected void dumpLogAdditional(StringBuilder sb) {
    }

    public String beginTag() {
        return BEGIN_TAG;
    }

    public String endTag() {
        return END_TAG;
    }

    void dumpKeys() {
        for (int i = 0; i < nEntries; i++) {
            System.out.println(Key.dumpString(entryKeyVals.get(i), 0));
        }
    }

    /**
     * For unit test support:
     * @return a string that dumps information about this IN, without
     */
    @Override
    public String dumpString(int nSpaces, boolean dumpTags) {
        StringBuilder sb = new StringBuilder();
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(beginTag());
            sb.append('\n');
        }

        if (dumpTags) {
            sb.append("<nodeId val=\"");
            sb.append(nodeId);
            sb.append("\"/>");
        } else {
            sb.append(nodeId);
        }
        sb.append('\n');

        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<idkey>");
        sb.append(identifierKey == null ?
                  "" :
                  Key.dumpString(identifierKey, 0));
        sb.append("</idkey>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<prefix>");
        sb.append(keyPrefix == null ? "" : Key.dumpString(keyPrefix, 0));
        sb.append("</prefix>\n");
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<dirty val=\"").append(getDirty()).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<generation val=\"").append(generation).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<level val=\"");
        sb.append(Integer.toHexString(level)).append("\"/>");
        sb.append('\n');
        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<isRoot val=\"").append(isRoot()).append("\"/>");
        sb.append('\n');

        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("<entries nEntries=\"");
        sb.append(nEntries);
        sb.append("\">");
        sb.append('\n');

        for (int i = 0; i < nEntries; i++) {
            sb.append(TreeUtils.indent(nSpaces + 4));
            sb.append("<entry id=\"" + i + "\">");
            sb.append('\n');
            if (getLsn(i) == DbLsn.NULL_LSN) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<lsn/>");
            } else {
                sb.append(DbLsn.dumpString(getLsn(i), nSpaces + 6));
            }
            sb.append('\n');
            if (entryKeyVals.get(i) == null) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<key/>");
            } else {
                sb.append(Key.dumpString(entryKeyVals.get(i), (nSpaces + 6)));
            }
            sb.append('\n');
            if (entryTargets.get(i) == null) {
                sb.append(TreeUtils.indent(nSpaces + 6));
                sb.append("<target/>");
            } else {
                sb.append(entryTargets.get(i).dumpString(nSpaces + 6, true));
            }
            sb.append('\n');
            sb.append(TreeUtils.indent(nSpaces + 6));
            dumpDeletedState(sb, getState(i));
            sb.append("<dirty val=\"").append(isDirty(i)).append("\"/>");
            sb.append('\n');
            sb.append(TreeUtils.indent(nSpaces + 4));
            sb.append("</entry>");
            sb.append('\n');
        }

        sb.append(TreeUtils.indent(nSpaces + 2));
        sb.append("</entries>");
        sb.append('\n');
        if (dumpTags) {
            sb.append(TreeUtils.indent(nSpaces));
            sb.append(endTag());
        }
        return sb.toString();
    }

    /**
     * Utility method for output of knownDeleted and pendingDelete.
     */
    static void dumpDeletedState(StringBuilder sb, byte state) {
        sb.append("<knownDeleted val=\"");
        sb.append(isStateKnownDeleted(state)).append("\"/>");
        sb.append("<pendingDeleted val=\"");
        sb.append(isStatePendingDeleted(state)).append("\"/>");
    }

    @Override
    public String toString() {
        return dumpString(0, true);
    }

    public String shortClassName() {
        return "IN";
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceSplit(Level level,
                            IN parent,
                            IN newSibling,
                            long parentLsn,
                            long myNewLsn,
                            long newSiblingLsn,
                            int splitIndex,
                            int idKeyIndex,
                            int childIndex) {
        Logger logger = databaseImpl.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(TRACE_SPLIT);
            sb.append(" parent=");
            sb.append(parent.getNodeId());
            sb.append(" child=");
            sb.append(getNodeId());
            sb.append(" newSibling=");
            sb.append(newSibling.getNodeId());
            sb.append(" parentLsn = ");
            sb.append(DbLsn.getNoFormatString(parentLsn));
            sb.append(" childLsn = ");
            sb.append(DbLsn.getNoFormatString(myNewLsn));
            sb.append(" newSiblingLsn = ");
            sb.append(DbLsn.getNoFormatString(newSiblingLsn));
            sb.append(" splitIdx=");
            sb.append(splitIndex);
            sb.append(" idKeyIdx=");
            sb.append(idKeyIndex);
            sb.append(" childIdx=");
            sb.append(childIndex);
            LoggerUtils.logMsg(logger,
                               databaseImpl.getDbEnvironment(),
                               level,
                               sb.toString());
        }
    }

    /**
     * Send trace messages to the java.util.logger. Don't rely on the logger
     * alone to conditionalize whether we send this message, we don't even want
     * to construct the message if the level is not enabled.
     */
    private void traceDelete(Level level, int index) {
        Logger logger = databaseImpl.getDbEnvironment().getLogger();
        if (logger.isLoggable(level)) {
            StringBuilder sb = new StringBuilder();
            sb.append(TRACE_DELETE);
            sb.append(" in=").append(getNodeId());
            sb.append(" index=");
            sb.append(index);
            LoggerUtils.logMsg(logger,
                               databaseImpl.getDbEnvironment(),
                               level,
                               sb.toString());
        }
    }
}
