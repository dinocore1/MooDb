package com.sleepycat.je.sync.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.sync.impl.LogChangeSet.LogChangeSetBinding;
import com.sleepycat.je.sync.impl.SyncDB.DataType;
import com.sleepycat.je.trigger.PersistentTrigger;
import com.sleepycat.je.trigger.ReplicatedDatabaseTrigger;
import com.sleepycat.je.trigger.TransactionTrigger;
import com.sleepycat.je.trigger.Trigger;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.VLSN;

/*
 * SyncCleanerBarrier keeps track of minimal nextSyncStart for the SyncDataSets
 * registered in this Environment.
 *
 * JE cleaners will use this minSyncStart to calculate the cleaner barrier
 * file:
 *
 * 1. On standalone side, it will return the file that minSyncStart lives as
 * the cleaner barrier file.
 * 2. On replication side, it will compare the minSyncStart and the GloalCBVLSN
 * (it's the minimal fsynced VLSN on the replicas, see SR18728), and choose the
 * file where smaller one lives as the cleaner barrier file.
 *
 * TODO: use a trigger for doing changes to the syncStarts in case the exported
 * transaction aborts.
 */
public class SyncCleanerBarrier {
    private final EnvironmentImpl envImpl;

    /*
     * Saves sync start position information for all SyncDataSets in this
     * Environment. The Key is processorName + syncDataSetName, value is the
     * sync start position for the SyncDataSet.
     */
    private final Map<String, Long> syncStarts = new HashMap<String, Long>();

    /* The minimal sync start position for all SyncDataSets. */
    private long minSyncStart = LogChangeSet.NULL_POSITION;

    /**
     * The init() method must be called immediately after creation.
     * Initialization is separate to support trigger initialization and
     * reference from the trigger to the SyncCleanerBarrier.  See creation of
     * the SyncCleanerBarrier in EnvironmentImpl.
     */
    public SyncCleanerBarrier(EnvironmentImpl envImpl) {
        this.envImpl = envImpl;
    }

    /**
     * Read the SyncDB to get the LogChangeSet information.  Must be called
     * immediately after creation (see constructor).
     */
    public void init(Environment env) {
        if (envImpl.isReadOnly()) {
            /* Cleaning is not possible in a read-only environment. */
            return;
        }

        final SyncDB syncDb;
        try {
            syncDb = new SyncDB(envImpl, false /*allowCreate*/);
        } catch (DatabaseNotFoundException e) {
            return;
        }

        /* Read the ChangeSet information. */
        Map<String, DatabaseEntry> changeSets =
            syncDb.readDataForType(DataType.CHANGE_SET, env);

        /* Do nothing if there is no written ChangeSet information. */
        if (changeSets == null || changeSets.size() == 0) {
            return;
        }

        /* Go over the whole list. */
        LogChangeSetBinding binding = new LogChangeSetBinding();
        long minValue = Long.MAX_VALUE;
        for (Map.Entry<String, DatabaseEntry> entry : changeSets.entrySet()) {
            LogChangeSet changeSet = binding.entryToObject(entry.getValue());
            syncStarts.put(entry.getKey(), changeSet.getNextSyncStart());
            /* Find the minSyncStart. */
            if (doCompare(minValue, changeSet.getNextSyncStart()) > 0) {
                minValue = changeSet.getNextSyncStart();
            }
        }

        /*
         * Change minSyncStart only if there exists LogChangeSet
         * records in SyncDB.
         */
        if (minValue != Long.MAX_VALUE) {
            minSyncStart = minValue;
        }
    }

    private int doCompare(long value1, long value2) {
       if (envImpl.isReplicated()) {
           return (new VLSN(value1)).compareTo(new VLSN(value2));
       } else {
           return DbLsn.compareTo(value1, value2);
       }
    }

    /* Update the LogChangeSet information for a SyncDataSet. */
    synchronized void updateSyncStart(String key, StartInfo startInfo) {
        final long syncStart = startInfo.getNextSyncStart();
        final boolean firstDataSet = (syncStarts.size() == 0);

        /*
         * If the new syncStart is smaller than minSyncStart, should throw an
         * EnvironmentFailureException.
         */
        if (doCompare(syncStart, minSyncStart) < 0) {
            StringTokenizer tokenizer = new StringTokenizer(key, "-");
            throw EnvironmentFailureException.unexpectedState
                (envImpl, "Invalid behavior, Processor: " +
                 tokenizer.nextToken() + ", SyncDataSet: " +
                 tokenizer.nextToken() + " is reading log entries on file " +
                 "smaller than cleaner barrier file: " +
                 envImpl.getCleanerBarrierStartFile() + " at " +
                 (envImpl.isReplicated() ?
                  ("VLSN: " + syncStart) :
                  ("lsn: " + DbLsn.getNoFormatString(syncStart))));
        }


        /*
         * Remove the nextSyncStart from the map if a SyncDataSet is removed
         * from a SyncProcessor.
         */
        if (startInfo.isDelete()) {
            syncStarts.remove(key);
        } else {
            syncStarts.put(key, syncStart);
        }

        /*
         * If there is no SyncDataSet in the map, set the minSyncStart to
         * LogChangeSet.NULL_POSITION.
         */
        if (syncStarts.size() == 0) {
            minSyncStart = LogChangeSet.NULL_POSITION;
            return;
        }

        /* Go through the map to find the minSyncStart. */
        long minValue = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : syncStarts.entrySet()) {
            if (doCompare(minValue, entry.getValue()) > 0) {
                minValue = entry.getValue();
            }
        }

        /* Set the new minimal syncStart value. */
        assert minValue != Long.MAX_VALUE;
        minSyncStart = minValue;

        /* 
         * Enable LocalCBVLSN changes on the master after adding the first 
         * SyncDataSet. 
         */
        if (envImpl.isMaster() && firstDataSet) { 
            envImpl.unfreezeLocalCBVLSN();
        }
    }

    /* Return the sync start for a SyncDataSet. */
    public synchronized long getSyncStart(String key) {
        /* Make sure the SyncDataSet is already registered. */
        assert syncStarts.containsKey(key);

        return syncStarts.get(key);
    }

    /* Return the value of the minSyncStart to start the LogChangeReader. */
    public synchronized long getMinSyncStart() {
        return minSyncStart;
    }

    /* Return the current map size for the SyncDataSets. */
    public synchronized boolean isFirstSyncDataSet() {
        return syncStarts.size() == 0;
    }

    public static class SyncTrigger
        implements Trigger, TransactionTrigger, ReplicatedDatabaseTrigger,
                   PersistentTrigger {

        /*
         * Used to save the nextSyncStart changes caused by transactions,
         * a transaction may commit the change tracking information changes for
         * multiple SyncDataSets, so the value of this map has to be a map.
         *
         * The key of txnIdToSyncStarts is the transaction id, the key of the
         * txnIdToSyncStarts' value is the name of a SyncDataSet.
         */
        private transient Map<Long, Map<String, StartInfo>> txnIdToSyncStarts;
        /* The name of the trigger. */
        private final String triggerName;
        private transient SyncCleanerBarrier barrier;
        /* The name of the database. */
        private transient String dbName;

        public SyncTrigger(String triggerName) {
            this.triggerName = triggerName;
        }

        public String getName() {
            return triggerName;
        }

        public Trigger setDatabaseName(String dbName) {
            this.dbName = dbName;

            return this;
        }

        public String getDatabaseName() {
            return dbName;
        }

        public void addTrigger(@SuppressWarnings("unused")
                               Transaction txn) {
        }

        public void removeTrigger(@SuppressWarnings("unused")
                                  Transaction txn) {
        }

        public void open(@SuppressWarnings("unused")
                         Transaction txn,
                         Environment environment,
                         @SuppressWarnings("unused")
                         boolean isNew) {
            barrier = DbInternal.getEnvironmentImpl(environment).
                                 getSyncCleanerBarrier();
            assert barrier != null;
            txnIdToSyncStarts =
                new ConcurrentHashMap<Long, Map<String, StartInfo>>();
        }

        public void close() {
        }

        public void remove(@SuppressWarnings("unused")
                           Transaction txn) {
        }

        public void truncate(@SuppressWarnings("unused")
                             Transaction txn) {
        }

        public void rename(@SuppressWarnings("unused")
                           Transaction txn,
                           @SuppressWarnings("unused")
                           String newName) {
        }

        /* Invoked when inserting new records in the SyncDB. */
        public void put(Transaction txn,
                        DatabaseEntry key,
                        @SuppressWarnings("unused")
                        DatabaseEntry oldData,
                        DatabaseEntry newData) {
            addNewMapEntry(txn, key, newData, false);
        }

        /* Save a new nextSyncStart information for the transaction. */
        private void addNewMapEntry(Transaction txn,
                                    DatabaseEntry key,
                                    DatabaseEntry data,
                                    boolean isDelete) {
            String dataSetName = StringBinding.entryToString(key);

            /* Only do it if it's a CHANGE_SET information. */
            if (DataType.getDataType(dataSetName) == DataType.CHANGE_SET) {
                Map<String, StartInfo> syncStartInfos =
                    txnIdToSyncStarts.get(txn.getId());
                if (syncStartInfos == null) {
                    syncStartInfos =
                        new ConcurrentHashMap<String, StartInfo>();
                    txnIdToSyncStarts.put(txn.getId(), syncStartInfos);
                }
                LogChangeSetBinding binding = new LogChangeSetBinding();
                LogChangeSet set = binding.entryToObject(data);
                StartInfo startInfo =
                    new StartInfo(set.getNextSyncStart(), isDelete);
                syncStartInfos.put(dataSetName, startInfo);
            }
        }

        /* Invoked when deleting records from the SyncDB. */
        public void delete(Transaction txn,
                           DatabaseEntry key,
                           DatabaseEntry oldData) {
            addNewMapEntry(txn, key, oldData, true);
        }

        public void commit(Transaction txn) {
            Map<String, StartInfo> syncStartInfos =
                txnIdToSyncStarts.get(txn.getId());

            /*
             * Recalculate the minSyncStart if this transaction does put/delete
             * operations.
             */
            if (syncStartInfos != null) {
                for (Map.Entry<String, StartInfo> entry :
                     syncStartInfos.entrySet()) {
                    barrier.updateSyncStart(entry.getKey(), entry.getValue());
                }
            }

            /* Remove it from the in-memory map. */
            txnIdToSyncStarts.remove(txn.getId());
        }

        /* Do nothing, only remove information from the map. */
        public void abort(Transaction txn) {
            txnIdToSyncStarts.remove(txn.getId());
        }

        /* TODO: implement ReplicatedDatabaseTrigger 'repeat' methods. */

        public void repeatTransaction(Transaction txn) {
        }

        public void repeatAddTrigger(Transaction txn) {
        }

        public void repeatRemoveTrigger(Transaction txn) {
        }

        public void repeatCreate(Transaction txn) {
        }

        public void repeatRemove(Transaction txn) {
        }

        public void repeatTruncate(Transaction txn) {
        }

        public void repeatRename(Transaction txn, String newName) {
        }

        public void repeatPut(Transaction txn,
                              DatabaseEntry key,
                              DatabaseEntry newData) {
        }

        public void repeatDelete(Transaction txn,
                                 DatabaseEntry key) {
        }
    }

    /* Object used to save the sync start information for a SyncDataSet. */
    static class StartInfo {
        /* The new nextSyncStart information for the SyncDataSet. */
        private final long nextSyncStart;
        /* True if it's a SyncDataSet remove operation. */
        private final boolean isDelete;

        public StartInfo(long nextSyncStart, boolean isDelete) {
            this.nextSyncStart = nextSyncStart;
            this.isDelete = isDelete;
        }

        public long getNextSyncStart() {
            return nextSyncStart;
        }

        public boolean isDelete() {
            return isDelete;
        }
    }
}
