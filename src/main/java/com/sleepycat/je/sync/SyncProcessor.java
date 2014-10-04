package com.sleepycat.je.sync;

import java.util.Collection;
import java.util.Map;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.sync.impl.LogChangeReader;
import com.sleepycat.je.sync.impl.LogChangeSet;
import com.sleepycat.je.sync.impl.LogChangeSet.LogChangeSetBinding;
import com.sleepycat.je.sync.impl.SyncDB;
import com.sleepycat.je.sync.impl.SyncDB.OpType;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Manages one or more synchronized data sets for a particular external system
 * and processes sync operations.  Subclasses of this abstract class are used
 * for specific types of external systems.
 *
 * <p>This class provides two types of extensibility.</p>
 *
 * <p>First, it may be extended directly to implement its abstract methods in
 * in order to implement synchronization with an external system that is not
 * supported by one of the built-in SyncProcessor subclasses.  Such subclasses
 * may call the writeProcessorMetadata, readProcessorMetadata,
 * writeProcessorTxnData and readProcessorTxnData methods.</p>
 *
 * <p>Second, a concrete SyncProcessor subclass may itself by extended to
 * override the openChangeReader method to provide a custom source of change
 * set information. Such subclasses may call the writeChangeSetData and
 * readChangeSetData methods.</p>
 */
public abstract class SyncProcessor {
    /* The Environment that needs to be synced. */
    protected final Environment env;
    /* The EnvironmentImpl for the env. */
    protected final EnvironmentImpl envImpl;
    /* Name of the SyncProcessor. */
    protected final String processorName;
    /* The internal database used to store the metadata. */
    private final SyncDB syncDb;
    /* TestHook used for concurrent addDataSet unit tests. */
    private TestHook addHook;
    /* TestHook used for concurrent removeDataSet unit tests. */
    private TestHook removeHook;

    /**
     * Used by subclasses to create a SyncProcessor.
     */
    protected SyncProcessor(Environment env, String processorName) {
        this.env = env;
        this.processorName = processorName;
        this.envImpl = DbInternal.getEnvironmentImpl(env);
        syncDb = new SyncDB(envImpl, true /*allowCreate*/);
    }

    /**
     * Returns the name of the processor, which is unique among all processors
     * for a JE Environment.
     */
    public String getName() {
        return processorName;
    }

    /**
     * Returns the JE Environment associated with this processor.
     */
    public Environment getEnvironment() {
        return env;
    }

    /**
     * Adds a data set that is managed by this processor.
     *
     * <p>After calling this method, all changes to the JE databases in the
     * data set will be tracked by JE, so they can be exported to the external
     * system during a sync operation.  The tracking of changes in the external
     * system, if any, is defined by the SyncProcessor subclass.</p>
     *
     * <p>The user must ensure that no transactions for the databases in the
     * data set are active during the call to this method.  If transactions are
     * active, the results of the first sync operation with respect to these
     * transactions are undefined, and the sync is unlikely to succeed.</p>
     *
     * <p>Normally, each database in a newly added data set should be initially
     * empty or non-existent.  If a database is non-empty, the user must ensure
     * that the records in the database are present in the external system.</p>
     *
     * @throws IllegalStateException if a data set with the given name already
     * exists.
     */
    public abstract SyncDataSet addDataSet(String dataSetName,
                                           Collection<SyncDatabase> databases);

    /**
     * @hidden
     * Used by tests only.
     */
    public void setAddHook(TestHook addHook) {
        this.addHook = addHook;
    }

    /**
     * Called internally by the SyncProcessor during a call to {@link
     * #addDataSet}.  This method initializes change tracking for all databases
     * in the data set.
     *
     * <p>The user should only call this method when extending SyncProcessor
     * directly to implement a custom processor for an external system that is
     * not supported by the built-in SyncProcessor classes.</p>
     */
    protected void registerDataSet(SyncDataSet dataSet) {
        synchronized (envImpl.getSyncCleanerBarrier()) {

            /*
             * Disable the LocalCBVLSN changes on the master because of a 
             * concurrent issue, following is the description:
             *
             * 1. Thread A on the master is trying to add the first 
             *    SyncDataSet, but it sleeps before writing the 
             *    SyncCleanerBarrier. 
             * 2. Thread B on the master wakes up, and do database operations 
             *    to advance the GlobalCBVLSN to be larger than the endOfLog 
             *    got by thread B.
             * 3. Cleaners on the master wake up, but it is blocked at 
             *    SyncCleanerBarrier.getMinSyncStart, so it won't delete the
             *    log file where endOfLog lives.
             * 4. Cleaners on the replicas wake up, because the current
             *    GlobalCBVLSN is larger than endOfLog, so they can delete the
             *    log file where endOfLog lives.
             * 5. The master dies and a replica is elected to be a master, it
             *    will try to read log entries on the a deleted log file.
             */
            if (envImpl.isMaster() &&
                envImpl.getSyncCleanerBarrier().isFirstSyncDataSet()) {
                envImpl.freezeLocalCBVLSN();
            }

            /* Get the end of current JE log files. */
            long endOfLog = envImpl.getEndOfLog();

            TestHookExecute.doHookIfSet(addHook);

            /* Write the information to the database. */
            LogChangeSetBinding binding = new LogChangeSetBinding();
            DatabaseEntry data = new DatabaseEntry();
            binding.objectToEntry(new LogChangeSet(endOfLog, endOfLog), data);

            syncDb.writeChangeSetData(env, null, processorName, 
                                      dataSet.getName(), data, OpType.INSERT);
        }
    }

    /**
     * Removes a data set that is managed by this processor.
     *
     * @throws IllegalStateException if a data set with the given name does not
     * exist.
     */
    public abstract void removeDataSet(String dataSetName);

    /**
     * @hidden
     * Used by tests only.
     */
    public void setRemoveHook(TestHook removeHook) {
        this.removeHook = removeHook;
    }

    /**
     * Called internally by the SyncProcessor during a call to {@link 
     * #removeDataSet}. This method will remove the change tracking information
     * for this SyncDataSet from the internal databases.
     *
     * <p>The user should only call this method when extending SyncProcessor 
     * directly to implement a custom processor for an external system that is
     * not supported by the built-in SyncProcessor classes.</p>
     */
    protected void unregisterDataSet(String dataSetName) {
        synchronized (envImpl.getSyncCleanerBarrier()) {
            TestHookExecute.doHookIfSet(removeHook);
            syncDb.writeChangeSetData
                (env, null, processorName, dataSetName, null, OpType.DELETE);
        }
    }

    /**
     * Returns all SyncDataSets that are managed by this processor.
     */
    public abstract Map<String, SyncDataSet> getDataSets();

    /**
     * Performs a sync operation for all SyncDataSets managed by this
     * processor.
     */
    public abstract void syncAll(ExportConfig exportConfig,
                                 ImportConfig importConfig)
        throws SyncCanceledException;

    /**
     * Performs a sync operation for the specified SyncDataSets.
     */
    public abstract void sync(ExportConfig exportConfig,
                              ImportConfig importConfig,
                              String... dataSetName)
        throws SyncCanceledException;

    /**
     * Cancels a sync operation being performed in another thread.  Causes the
     * SyncCancelException to be thrown by the method that invoked the sync.
     */
    public abstract void cancelSync();

    /**
     * Called internally by the SyncProcessor during a sync operation to obtain
     * the set of local changes to be transferred to the external system.
     *
     * <p>The user should never have a need to call this method, but may wish
     * to override it as described below.</p>
     *
     * <p>The default implementation of this method returns a ChangeReader that
     * reads the JE log and keeps track of processed and pending changes
     * automatically.  Note that the portion the JE log that contains change
     * sets will be retained (the log cleaner will not delete the log files in
     * this portion of the log) until all changes in that portion of the log
     * are fully processed.</p>
     *
     * <p>For advanced use, a user may override this method in any
     * SyncProcessor to provide the change set by some other mechanism. For
     * example, if a JE Database is used as an insert-only input queue for data
     * to be transferred to an external system, a ChangeReader could be
     * implemented that uses key ranges to keep track of processed and pending
     * changes, and that reads the records in a key range from the Database, in
     * order to supply the changes to the SyncProcessor.  To keep track of
     * change set metadata, a custom implementation may call writeChangeSetData
     * and readChangeSetDat.</p>
     */
    protected ChangeReader openChangeReader(String dataSetName,
                                            boolean consolidateTransactions,
                                            long consolidateMaxMemory) {
        return new LogChangeReader(env, 
                                   dataSetName, 
                                   this, 
                                   consolidateTransactions, 
                                   consolidateMaxMemory);
    }

    /**
     * Called internally by the SyncProcessor to write change set data.
     *
     * <p>The user should only call this method when extending a SyncProcessor
     * class to override the openChangeReader method.</p>
     */
    public final void writeChangeSetData(Transaction txn,
                                         String dataSetName,
                                         DatabaseEntry data) {
        syncDb.writeChangeSetData
            (env, txn, processorName, dataSetName, data, OpType.UPDATE);
    }

    /**
     * Called internally by the SyncProcessor to read change set data.
     *
     * <p>The user should only call this method when extending a SyncProcessor
     * class to override the openChangeReader method.</p>
     */
    public final void readChangeSetData(Transaction txn,
                                        String dataSetName,
                                        DatabaseEntry data) {
        syncDb.readChangeSetData(env, txn, processorName, dataSetName, data);
    }

    /**
     * Called internally by the SyncProcessor to write processor-specific 
     * metadata, including a collection of SyncDataSet objects and processor
     * connection properties.
     *
     * <p>The user should only call this method when extending SyncProcessor
     * directly to implement a custom processor for an external system that is
     * not supported by the built-in SyncProcessor classes.  Such
     * implementations will also normally subclass ProcessorMetadata to add
     * processor specific connection properties and other metadata.</p>
     */
    public final <M extends ProcessorMetadata> void
        writeProcessorMetadata(Transaction txn, M metadata) {
        syncDb.writeProcessorMetadata(env, txn, processorName, metadata);
    }

    /**
     * Called internally by the SyncProcessor to read processor-specific 
     * configuration data, such as connection properties.
     *
     * <p>The user should only call this method when extending SyncProcessor
     * directly to implement a custom processor for an external system that is
     * not supported by the built-in SyncProcessor classes.</p>
     */
    public final <M extends ProcessorMetadata> M
        readProcessorMetadata(Transaction txn) {
        return (M) syncDb.readProcessorMetadata(env, txn, this);
    }

    /**
     * Called internally by the SyncProcessor to write processor-specific 
     * transaction data.
     *
     * <p>The user should only call this method when extending SyncProcessor
     * directly to implement a custom processor for an external system that is
     * not supported by the built-in SyncProcessor classes.</p>
     */
    protected final void writeProcessorTxnData(Transaction txn,
                                               String dataSetName,
                                               DatabaseEntry data) {
        syncDb.writeProcessorTxnData
            (env, txn, processorName, dataSetName, data);
    }

    /**
     * Called internally by the SyncProcessor to read processor-specific 
     * transaction data.
     *
     * <p>The user should only call this method when extending SyncProcessor
     * directly to implement a custom processor for an external system that is
     * not supported by the built-in SyncProcessor classes.</p>
     */
    protected final void readProcessorTxnData(Transaction txn,
                                              String dataSetName,
                                              DatabaseEntry data) {
        syncDb.readProcessorTxnData
            (env, txn, processorName, dataSetName, data);
    }

    /**
     * @hidden
     *
     * Unit test only.
     */
    public SyncDB getSyncDB() {
        return syncDb;
    }
}
