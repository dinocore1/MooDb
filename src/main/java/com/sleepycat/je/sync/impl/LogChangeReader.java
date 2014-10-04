package com.sleepycat.je.sync.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DatabaseId;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.LNFileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.entry.LNLogEntry;
import com.sleepycat.je.sync.ChangeReader;
import com.sleepycat.je.sync.SyncDataSet;
import com.sleepycat.je.sync.SyncDatabase;
import com.sleepycat.je.sync.SyncProcessor;
import com.sleepycat.je.sync.impl.LogChangeSet.LogChangeSetBinding;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;
import com.sleepycat.je.utilint.VLSN;

/*
 * LogChangeReader implements its own LogChangeIterator, which uses a 
 * LNFileReader to scan the log to find out the log entries we're interested.
 *
 * To track the next/commit point on the log, the JEChangeTxn has to remembered
 * the start and commit (LSN for a standalone Environment, VLSN for a 
 * replicated Environment) point, and all JEChangeTxn that created during the
 * log scan are saved in LogChangeReader.txns.
 *
 * When the LogChangeReader is first created, we have to look for the VLSN->LSN
 * mapping to find out the LogChangeReader.readStart (the read start position
 * of the LNFileReader) if it's a replicated Environment. And we'll use the 
 * readStart to keep track of the reading start position of LNFileReader, so
 * that we don't need to look for the VLSN->LSN mapping when a new LNFileReader 
 * is created next time (former LNFileReader meets the end of the log, have to
 * create a new LNFileReader).
 *
 * Also, LogChangeReader uses the lastSyncEnd to keep track of the commit point
 * of the latest committed transaction, and we set it as 
 * LogChangeSet.lastSyncEnd when the discardChanges is invoked. 
 *
 * Note that LogChangeReader will only read the entries between the 
 * nextSyncStart and GlobalCBVLSN (it's durable now) to make sure hard recovery
 * wouldn't cause the server to roll back committed transactions. Also, the 
 * txns is a LinkedHashMap, so the first JEChangeTxn in that map while 
 * discardChanges is invoked is the earliest uncommitted transaction, and its
 * startPoint should be recorded as the LogChangeSet.nextSyncStart.
 *
 * The special case for the LogChangeReader is when it encounters a commit 
 * entry, it'll not only set the readStart as the commit entry's lsn, but also
 * add the entry size of the commit log entry, so that the reader won't read 
 * the commit entry twice, to avoid treating a single transaction commit entry
 * as a whole transaction.
 *
 * TODO list:
 * 1. Implement the consolidation. 
 * 2. Currently make the SyncProcessor.readChangeSetData and 
 *    SyncProcessor.writeChangeSetData public, need to make it protected.
 * 3. May change the LNFileReader's reading end position from 
 *    DbLsn.NULL_LSN (the log end) to some value to reduce the read size.
 * 4. Buffering of large transactions is an issue, since it could cause OOME.
 *    If a buffered transaction exceeds a configured size, we could discard all
 *    but the LSNs from the buffer and read the entries from the log after we
 *    encounter the commit.
 * 5. Figure out a way to throw out an exception if the user doesn't work in
 *    correct sequence.
 * 6. Adding the support for non-transactional LN log entries. 
 */
public class LogChangeReader implements ChangeReader {

    /* The target entry types for the reader. */
    public static final LogEntryType[] targetTypes = new LogEntryType[] {
        LogEntryType.LOG_INS_LN_TRANSACTIONAL,     // insert entry type
        LogEntryType.LOG_UPD_LN_TRANSACTIONAL,     // update entry type
        LogEntryType.LOG_DEL_LN_TRANSACTIONAL,     // delete entry type 
        LogEntryType.LOG_DEL_DUPLN_TRANSACTIONAL,  // dup delete entry type 
        LogEntryType.LOG_TXN_COMMIT,               // txn commit
        LogEntryType.LOG_TXN_ABORT                 // txn abort
    };

    /* A binding used to serialize and deserialize the LogChangeSet. */
    private static final LogChangeSetBinding binding = 
        new LogChangeSetBinding();

    private final EnvironmentImpl envImpl;
    private final String dataSetName;
    private final SyncProcessor processor;
    private final boolean consolidateTransactions;
    private final long consolidateMaxMemory;

    /* Used to keep track of next/commit point of changed transactions. */
    private LogChangeSet changeSet;

    /* The reading start position of the LNFileReader. */
    private long readStart;

    /*
     * LogChangeReader has to read the VLSN->LSN mapping if it's the first time
     * to create the LNFileReader, then it will use the readStart to keep track
     * of the next reading start position.
     */
    private boolean firstCreateReader = true;

    /* Save the last transaction commit point. */
    private long lastSyncEnd = LogChangeSet.NULL_POSITION;

    /* Keep track of the ChangeTxns read from JE log. */
    private final Map<Long, ChangeTxn> txns = 
        new LinkedHashMap<Long, ChangeTxn>();

    /* Save the database id and info for synced databases. */
    private final Map<DatabaseId, DbInfo> syncDbs = 
        new HashMap<DatabaseId, DbInfo>();

    /* TestHook used for concurrent unit tests. */
    private TestHook waitHook;

    public LogChangeReader(Environment env, 
                           String dataSetName, 
                           SyncProcessor processor,
                           boolean consolidateTransactions,
                           long consolidateMaxMemory) {
        this.envImpl = DbInternal.getEnvironmentImpl(env);
        this.dataSetName = dataSetName;
        this.processor = processor;
        this.consolidateTransactions = consolidateTransactions;
        this.consolidateMaxMemory = consolidateMaxMemory;

        initChangeSet(env);

        getSyncDbs(env);

        readStart = changeSet.getNextSyncStart();

        assert readStart != LogChangeSet.NULL_POSITION;
    }

    /* 
     * Initiate the LogChangeSet, if there exists any exceptions while reading 
     * the ChangeSet metadata from SyncDB, invalidate the Environment. 
     */
    private void initChangeSet(Environment env) {
        Transaction txn = env.beginTransaction(null, null);
        boolean success = false;

        try {
            DatabaseEntry changeSetData = new DatabaseEntry();

            processor.readChangeSetData(txn, dataSetName, changeSetData);

            /* 
             * If there is no ChangeSet information for this SyncDataSet in 
             * SyncDB, create a new one, otherwise deserialize the 
             * DatabaseEntry.
             */
            changeSet = (changeSetData.getData() == null) ?
                new LogChangeSet() : binding.entryToObject(changeSetData);
            success = true;
        } finally {
           if (success) {
              txn.commit();
           } else {
              txn.abort();
           }
        } 
    }

    /* 
     * Get database ID and name information for sync'ed databases in this 
     * SyncDataSet. 
     */
    private void getSyncDbs(Environment env) {
        /* Get the DatabaseId for all synced databases. */
        SyncDataSet dataSet = processor.getDataSets().get(dataSetName);
        Iterator<SyncDatabase> databases = dataSet.getDatabases().iterator();

        while (databases.hasNext()) {
            Locker readLocker = null;
            boolean operationOK = false;
            DatabaseImpl dbImpl = null;
            String dbName = databases.next().getLocalName();

            try {
                readLocker = LockerFactory.getReadableLocker
                    (env, null, 
                     false, /* transactional */
                     false /* readCommittedIsolation */);

                dbImpl = envImpl.getDbTree().getDb(readLocker, dbName, null);
                if (dbImpl != null) {
                    syncDbs.put(dbImpl.getId(),
                                new DbInfo(dbName,
                                           dbImpl.getSortedDuplicates()));
                }
                operationOK = true;
            } finally {
                if (dbImpl != null) {
                    envImpl.getDbTree().releaseDb(dbImpl);
                }

                if (readLocker != null) {
                    readLocker.operationEnd(operationOK);
                }
            }
        }
    }

    public class DbInfo {
        public final String name;
        public final boolean duplicates;

        DbInfo(final String name, final boolean duplicates) {
            this.name = name;
            this.duplicates = duplicates;
        }
    }

    public Map<DatabaseId, DbInfo> getSyncDbs() {
        return syncDbs;
    }

    public LogChangeSet getChangeSet() {
        return changeSet;
    }

    /* 
     * Create an LNFileReader to read the log entries. 
     *
     * TODO: Keep track of last fsynced LSN in the FileManager, so we can 
     * export entries in the last file if an fsync has occurred, also consider
     * updating the GlobalDurableVLSN to the last fsynced VLSN.
     *
     * We're currently reading entries until the second largest log file, but
     * if the log file size is fairly large, it is not efficient to do so. 
     * Because there will be some checkpoints in the last log file or a 
     * transaction commit with WRITE_SYNC would flush the entries to the disk
     * and make them fsynced. We may need to find a way to keep track of the
     * last fsynced LSN to improve the performance.
     */
    private LNFileReader createFileReader() {
        final int readBufferSize = envImpl.getConfigManager().getInt
            (EnvironmentParams.LOG_ITERATOR_READ_SIZE);
        
        /* 
         * We shouldn't read unfsynced entries for a standalone Environment. 
         * The last entry on the second largest log file must be fsynced, but
         * we won't use its concrete LSN since it needs to read the whole file.
         *
         * To avoid this, we can use a fake lsn that represents the largest LSN
         * on the second largest log file.
         */
        final long currentFileNum = 
            envImpl.getFileManager().getCurrentFileNum();
        long finishLsn = 
            DbLsn.makeLsn(currentFileNum - 1, DbLsn.MAX_FILE_OFFSET);

        try {

            /* 
             * To avoid hard recovery, only read log entries between the read 
             * start and GlobalDurablVLSN for a ReplicatedEnvironment. 
             */
            if (envImpl.isReplicated()) {
                VLSN durableVLSN = envImpl.getGroupDurableVLSN();

                /* Read nothing if GlobalCBVLSN is null. */
                if (durableVLSN.isNull()) {
                    return null;
                }

                /* 
                 * Read nothing if read start is greater than GroupDurableVLSN.
                 */
                if (durableVLSN.compareTo(new VLSN(readStart)) <= 0) {
                    return null;
                }

                if (firstCreateReader) {
                
                    /* 
                     * Find the LSN for readStart (it's a VLSN now) when 
                     * LNFileReader is first created. 
                     */
                    readStart = envImpl.getLsnForVLSN(new VLSN(readStart),
                                                      readBufferSize);
                    firstCreateReader = false;
                }
        
                /* Find LSN for the durableVLSN. */
                finishLsn = 
                    envImpl.getLsnForVLSN(durableVLSN, readBufferSize);
            }
        } catch (EnvironmentFailureException e) {
            e.addErrorMessage("SyncDataSet: " + dataSetName + 
                              ", SyncProcessor: " + processor.getName());
            throw e;
        }

        LNFileReader reader = new LNFileReader(envImpl,
                                               readBufferSize,
                                               readStart,
                                               true,
                                               DbLsn.NULL_LSN,
                                               finishLsn, 
                                               null,
                                               DbLsn.NULL_LSN);
        
        for (LogEntryType entryType : targetTypes) {
            reader.addTargetType(entryType);
        }

        return reader;
    }

    public class JEChange implements Change {
        private final ChangeType type;
        private final DatabaseEntry key;
        private final DatabaseEntry data;
        private final String dbName;

        public JEChange(ChangeType type, 
                        DatabaseEntry key, 
                        DatabaseEntry data, 
                        String dbName) {
            this.type = type;
            this.key = key;
            this.data = data;
            this.dbName = dbName;
        }

        public ChangeType getType() {
            return type;
        }

        public DatabaseEntry getKey() {
            return key;
        }

        public DatabaseEntry getData() {
            return data;
        }

        public String getDatabaseName() {
            return dbName;
        }
    }

    public class JEChangeTxn implements ChangeTxn {
        private final long txnId;
        /* Start point of this transaction, either VLSN or LSN. */
        private long startPoint = LogChangeSet.NULL_POSITION;
        /* Commit point of this transaction, either VLSN or LSN. */
        private long commitPoint = LogChangeSet.NULL_POSITION;

        private final Set<String> dbNames = new HashSet<String>();
        /* The changes should be keep in sequence. */
        private final ArrayList<Change> operations = new ArrayList<Change>();

        public JEChangeTxn(long txnId, long startPoint) {
            this.txnId = txnId;
            this.startPoint = startPoint;
        }

        public long getTransactionId() {
            return txnId;
        }

        public String getDataSetName() {
            return dataSetName;
        }

        public Set<String> getDatabaseNames() {
            return dbNames;
        }

        public Iterator<Change> getOperations() {
            return operations.iterator();
        }

        /* Write the ChangeSet information to SyncDB for JDBCProcessor. */
        public void discardChanges(Transaction txn) {
            synchronized (envImpl.getSyncCleanerBarrier()) {
                /* Free the memory held by this ChangeTxn. */
                clear();

                /* 
                 * Set the LogChangeSet.lastSyncEnd to the commit point of this 
                 * ChangeTxn. 
                 */
                changeSet.setLastSyncEnd(commitPoint);

                /*
                 * Reset the LogChangeSet.nextSyncStart and write the 
                 * LogChangeSet into SyncDB. 
                 */
                resetChangeSetNextSyncStart();
                writeSyncDB(txn);
            }
        }

        /* Add a change for this transaction. */
        private void addChange(String dbName, Change operation) {
            if (!dbNames.contains(dbName)) {
                dbNames.add(dbName);
            }
            operations.add(operation);
        }

        private void setCommitPoint(long commitPoint) {
            this.commitPoint = commitPoint;
        }

        private long getStartPoint() {
            return startPoint;
        }

        private long getCommitPoint() {
            return commitPoint;
        }

        /* Clear the Changes hold by the ChangeTxn. */
        private void clear() {
            dbNames.clear();
            operations.clear();
        }
    }

    class LogChangeIterator implements Iterator<ChangeTxn> {
        private LNFileReader reader;
        private ChangeTxn nextChangeTxn;

        public LogChangeIterator() {
            reader = createFileReader();
        }

        public boolean hasNext() {

            /* 
             * TODO: need to figure out a way to deal with the invalid 
             * invocation that users try to call LogChangeIterator.next() 
             * after they remove the SyncDataSet from the SyncProcessor.
             */
            if (reader == null) {
                return false;
            }

            if (nextChangeTxn != null) {
                return true;
            }

            if (!hasNextChangeTxn()) {
                nextChangeTxn = null;
                return false;
            }

            return true;
        }

        /* 
         * We can remove the ChangeTxn from txns, but we can't remove the 
         * changes saved in nextChangeTxn, otherwise users get no changes. 
         * Should we remind users it's responsibility to remove those changes
         * from the memory?
         */
        public ChangeTxn next() {
            if (!hasNext()) {
                throw new NoSuchElementException
                    ("No ChangeTxn can be read from the log.");
            }

            /* Remove the ChangeTxn from the list. */
            ChangeTxn returnVal = nextChangeTxn;
            txns.remove(nextChangeTxn.getTransactionId());
            nextChangeTxn = null;

            /* Set lastSyncEnd remember the latest commit position. */
            long commitPoint = ((JEChangeTxn) returnVal).getCommitPoint();
            if (lastSyncEnd < commitPoint) {
                lastSyncEnd = commitPoint;
            }

            return returnVal;
        }

        public void remove() {
            throw new UnsupportedOperationException
                ("Iterator returned by ChangeReader.getChangeTxns() doesn't " +
                 "support remove operation.");
        }

        /* True if we reads a new ChangeTxn. */
        private boolean hasNextChangeTxn() {
            while (reader.readNextEntry()) {
                /* Deal with a transactional target LN log entry. */
                if (reader.isLN() && !reader.isInvisible()) {
                    addChange();
                }

                /* Deal with a commit log entry. */
                if (reader.isCommit()) {
                    if (isValidChangeTxn()) {
                        nextChangeTxn = txns.get(reader.getTxnCommitId());
                        return true;
                    }
                }

                /* 
                 * Release the memory held by an Abort transaction and remove
                 * it from the ChangeTxns list.
                 */
                if (reader.isAbort()) {
                    JEChangeTxn txn = 
                        (JEChangeTxn) txns.get(reader.getTxnAbortId());

                    /* 
                     * Only do this when the transaction belongs to this 
                     * SyncDataSet. 
                     */
                    if (txn != null) {
                        resetReadStart(false);
                        txn.clear();
                        txns.remove(txn.getTransactionId());
                    }
                }
            }

            return false;
        }

        /* Add a data change. */
        private void addChange() {
            DatabaseId dbId = reader.getDatabaseId();

            /* Do nothing if this entry doesn't belong to this SyncDataSet. */
            if (!syncDbs.containsKey(dbId)) {
                return;
            }

            DbInfo dbInfo = syncDbs.get(dbId);
            long txnId = reader.getTxnId();

            /* Create a ChangeTxn if it doesn't exist. */
            JEChangeTxn txn = (JEChangeTxn) txns.get(txnId);
            if (txn == null) {
                txn = new JEChangeTxn(txnId, getEntryPoint());
                txns.put(txnId, txn);
            }

            /*
             * Get the key and data field of this LN. We want the 'user' key
             * and data, not the raw key and data, because we're replaying the
             * user-level operation.  Duplicate DBs have two-part keys
             * containing the key and data, and LNLogEntry.getUserKeyData is
             * used to convert this back to the user operation params.
             */
            LNLogEntry lnEntry = reader.getLNLogEntry();
            lnEntry.postFetchInit(dbInfo.duplicates);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = (lnEntry.getLN().getData() != null) ?
                (new DatabaseEntry()) :
                null;
            lnEntry.getUserKeyData(key, data);

            /* Make sure the entry type is valid. */
            LogEntryType entryType =
                LogEntryType.findType(reader.getLogEntryType());
            assert entryType.isUserLNType() && entryType.isTransactional();

            /* Figure out the correct change type. */
            ChangeType type = ChangeType.DELETE;
            if (entryType == LogEntryType.LOG_INS_LN_TRANSACTIONAL) {
                type = ChangeType.INSERT;
            } else if (entryType == LogEntryType.LOG_UPD_LN_TRANSACTIONAL) {
                type = ChangeType.UPDATE;
            }

            txn.addChange(dbInfo.name,
                          new JEChange(type, key, data, dbInfo.name));
            resetReadStart(false);
        }

        /* Check a commit entry is an valid commit. */
        private boolean isValidChangeTxn() {
            JEChangeTxn txn = (JEChangeTxn) txns.get(reader.getTxnCommitId());

            /*
             * Do nothing when encounters a transaction commit that doesn't 
             * belong to this SyncDataSet. Note: JE doesn't log a commit entry 
             * if the transaction does nothing.
             */
            if (txn == null) {
                return false;
            }

            /*
             * If this transaction is already synced, remove it from the 
             * memory, otherwise set commit point of this transaction.
             */
            if (getEntryPoint() > changeSet.getLastSyncEnd()) {
                txn.setCommitPoint(getEntryPoint());
                resetReadStart(true);
                return true;
            } else {
                txn.clear();
                txns.remove(txn.getTransactionId());
            }

            return false;
        }

        /* Reset the lastLsn if the reader reads a larger lsn. */
        private void resetReadStart(boolean commitEntry) {
            if (readStart < reader.getLastLsn()) {
                readStart = reader.getLastLsn();
                if (commitEntry) {

                    /*
                     * Increase the readStart by the entry size of last
                     * transaction commit, so that the LNFileReader won't read
                     * the same transaction twice.
                     */
                    readStart += reader.getLastEntrySize();
                }
            }
        }

        /*
         * Get the position of a log entry on the log, return the VLSN if it's
         * replicated, return LSN if it's standalone.
         */
        private long getEntryPoint() {
            return (envImpl.isReplicated()) ?
                   reader.getVLSN() : reader.getLastLsn();
        }
    }

    public Iterator<ChangeTxn> getChangeTxns() {
        return new LogChangeIterator();
    }

    /* Reset the LogChangeSet.nextSyncStart. */
    private void resetChangeSetNextSyncStart() {
        if (txns.size() == 0) {

            /*
             * All transaction commits, the nextSyncStart should be the same as
             * lastSyncEnd.
             */
            changeSet.setNextSyncStart(changeSet.getLastSyncEnd());
        } else {

            /* 
             * txns is a LinkedHashMap, so the startPoint of first entry in 
             * txns should be the LogChangeSet.nextSyncStart.
             */
             for (ChangeTxn changeTxn : txns.values()) {
                 changeSet.setNextSyncStart
                     (((JEChangeTxn) changeTxn).getStartPoint());
                 break;
             }
        }
    }

    /**
     * @hidden
     * Used by tests only.
     */
    public void setWaitHook(TestHook waitHook) {
        this.waitHook = waitHook;
    }

    public void discardChanges(Transaction txn) {
        synchronized (envImpl.getSyncCleanerBarrier()) {
            TestHookExecute.doHookIfSet(waitHook);

            /* Set the LogChangeSet.lastSyncEnd. */
            if (lastSyncEnd > changeSet.getLastSyncEnd()) {
                changeSet.setLastSyncEnd(lastSyncEnd);
            }

            resetChangeSetNextSyncStart();
            writeSyncDB(txn);
        }
    }
        
    private void writeSyncDB(Transaction txn) {
        DatabaseEntry data = new DatabaseEntry();
        binding.objectToEntry(changeSet, data);

        processor.writeChangeSetData(txn, dataSetName, data);
    }
}
