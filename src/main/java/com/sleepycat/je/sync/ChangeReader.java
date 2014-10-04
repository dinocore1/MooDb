package com.sleepycat.je.sync;

import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Transaction;

/**
 * Retrieves all pending and unprocessed changes for one or more SyncDataSets;
 * used only by SyncProcessor implementations and custom sources of change set
 * information.
 *
 * TODO: javadoc that no ChangeReader classes will be thread safe, users need
 * to provide their own mechanisms to make them thread safe if they need to do
 * so.
 */
public interface ChangeReader {

    /**
     * The operation type of a Change; used only by SyncProcessor
     * implementations and custom sources of change set information.
     */
    enum ChangeType { INSERT, UPDATE, DELETE }

    /**
     * A change operation that is part of a ChangeTxn; used only by
     * SyncProcessor implementations and custom sources of change set
     * information.
     */
    public interface Change {

        /**
         * Returns the operation type of this change.
         */
        ChangeType getType();

        /**
         * Returns the record key for this change.
         */
        DatabaseEntry getKey();

        /**
         * Returns the record data for this change, or null if this is a
         * DELETE operation.
         */
        DatabaseEntry getData();

        /**
         * Returns the name of the local database to which this change was
         * applied.
         */
        String getDatabaseName();
    }

    /**
     * A transaction that contains one or more Changes; used only by
     * SyncProcessor implementations and custom sources of change set
     * information.
     */
    public interface ChangeTxn {

        /**
         * Returns the transction ID that can be shared with the external
         * system.
         */
        long getTransactionId();

        /**
         * Returns the name of the data set to which this transaction should be
         * applied.
         */
        String getDataSetName();

        /**
         * Returns the names of all local databases that are effected by this
         * transaction and that are part of a single data set (specified by
         * getDataSetName).
         */
        Set<String> getDatabaseNames();

        /**
         * Returns an iterator over the changes in this transactions, for a
         * single data set (specified by getDataSetName).
         *
         * <p>This method may be called more than once for a single transaction
         * to iterate over the changes multiple times.</p>
         */
        Iterator<Change> getOperations();

        /**
         * Called when the changes in this transaction have been transferred to
         * the external system, and they can be discarded locally.
         *
         * <p>The changes will not be discarded until the given txn has been
         * committed.  By using an XA transaction, the changes can be discarded
         * locally only if they are also committed in the external system.</p>
         */
        void discardChanges(Transaction txn);
    }

    /**
     * Returns an iterator over all transactions containing changes for one of
     * the data sets of interest (specified by SyncProcessor.getChangeSet and
     * indirectly by SyncProcessor.sync or syncAll).
     *
     * <p>If a single transaction applies to more than one change set, a
     * separate transaction will be returned by the iterator.</p>
     *
     * <p>If this method is called more than once for a given change set, the
     * information returned may be different each time, because changes may be
     * discarded by ChangeTxn.discardChanges and additional changes may appear
     * as they are being written.</p>
     */
    Iterator<ChangeTxn> getChangeTxns();

    /**
     * Called when the changes in all transactions returned by the
     * getChangeTxns iterator have been transferred to the external system, and
     * they can be discarded locally.
     *
     * <p>The changes will not be discarded until the given txn has been
     * committed.  By using an XA transaction, the changes can be discarded
     * locally only if they are also committed in the external system.</p>
     */
    void discardChanges(Transaction txn);
}
