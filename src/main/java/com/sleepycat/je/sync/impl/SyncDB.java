package com.sleepycat.je.sync.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbType;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.sync.ProcessorMetadata;
import com.sleepycat.je.sync.SyncDataSet;
import com.sleepycat.je.sync.SyncProcessor;
import com.sleepycat.je.sync.impl.SyncCleanerBarrier.SyncTrigger;
import com.sleepycat.je.trigger.Trigger;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.LockerFactory;

/**
 * Retrieves all pending and unprocessed changes for one or more SyncDataSets;
 * used only by SyncProcessor implementations and custom sources of change set
 * information.
 */
public class SyncDB {
    private static final String SYNC_TRIGGER_NAME = "syncTrigger";
    
    /* Database operation types operated on SyncDB. */
    public static enum OpType {
        /* Delete an existed record. */
        DELETE,

        /* Insert a new record. */
        INSERT,

        /* Update an existed record. */
        UPDATE;
    }

    /* 
     * The type of the data records in the SyncDB. 
     *
     * Note: the order of the enum values can never be changed, since the 
     * ordinals are stored in SyncDB.
     */
    public static enum DataType {
        /* Processor metadata. */
        PROCESSOR_METADATA,    

        /* Changset metadata. */
        CHANGE_SET,            

        /* Processor import server transaction data. */
        PROCESSOR_TXN_DATA;    

        /* Return the DataType for a key in SyncDB. */
        static DataType getDataType(String key) {
            int ordinal = 
                new Integer(key.substring(key.length() - 1, key.length()));

            if (ordinal == PROCESSOR_METADATA.ordinal()) {
                return PROCESSOR_METADATA;
            }

            if (ordinal == CHANGE_SET.ordinal()) {
                return CHANGE_SET;
            }

            return PROCESSOR_TXN_DATA;
        }
    }

    private final EnvironmentImpl envImpl;
    private final Database db;
    private final DatabaseImpl dbImpl;

    /**
     * @throws DatabaseNotFoundException if allowCreate is false and the sync
     * DB does not exist.
     */
    public SyncDB(EnvironmentImpl envImpl, boolean allowCreate)
        throws DatabaseNotFoundException {

        assert envImpl != null;
        this.envImpl = envImpl;

        /* 
         * Initialize the SyncDB. If allowCreate is true, create the SyncDB if
         * it doesn't exist. 
         */
        boolean success = false;

        final Environment env = envImpl.getInternalEnvHandle();
        assert env != null;
        final Transaction txn = DbInternal.beginInternalTransaction(env, null);
        try {

            /*
             * TODO: Need to call close, or make SyncDB a singleton, to avoid
             * leaving a Database handle open every time the SyncDB is
             * constructed.
             */
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(allowCreate);
            dbConfig.setTransactional(true);
           
            /* Set the trigger for updating the SyncCleanerBarrier. */ 
            SyncTrigger trigger = new SyncTrigger(SYNC_TRIGGER_NAME);
            ArrayList<Trigger> list = new ArrayList<Trigger>();
            list.add(trigger);
            dbConfig.setTriggers(list);

            DbInternal.setReplicated(dbConfig, true);

            db = DbInternal.openInternalDatabase
                (env, txn, DbType.SYNC.getInternalName(), dbConfig);
            dbImpl = DbInternal.getDatabaseImpl(db);
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
     * Write SyncProcessor metadata. 
     */
    public void writeProcessorMetadata(Environment env, 
                                       Transaction txn, 
                                       String processorName,
                                       ProcessorMetadata metadata) {
        /* Serialize ProcessorMetadata. */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(metadata);
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
        DatabaseEntry data = new DatabaseEntry(baos.toByteArray());

        /* Write the serialized object to database. */
        writeData(env, txn, processorName, null, data, 
                  DataType.PROCESSOR_METADATA, null);
    }

    /* Read SyncProcessor metadata. */
    public ProcessorMetadata readProcessorMetadata(Environment env,
                                                   Transaction txn, 
                                                   SyncProcessor processor) {
        DatabaseEntry data = new DatabaseEntry();
        readData(env,
                 txn, 
                 processor.getName(), 
                 null, 
                 data, 
                 DataType.PROCESSOR_METADATA);

        if (data.getData() == null) {
            return null;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream
            (data.getData(), data.getOffset(), data.getSize());
        try {
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object object = ois.readObject();
            assert ois.available() == 0;
            
            /* Set SyncDataSet's SyncProcessor field after de-serialization. */
            ProcessorMetadata<SyncDataSet> metadata = 
                (ProcessorMetadata<SyncDataSet>) object;
            for (SyncDataSet dataSet : metadata.getDataSets()) {
                dataSet.initSyncProcessor(processor);
            }

            return metadata;
        } catch (ClassNotFoundException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        } catch (IOException e) {
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /* Write ChangeSet metadata of a SyncDataSet. */
    public void writeChangeSetData(Environment env,
                                   Transaction txn,
                                   String processorName,
                                   String dataSetName,
                                   DatabaseEntry data,
                                   OpType opType) {
        writeData(env, txn, processorName, dataSetName, data, 
                  DataType.CHANGE_SET, opType);
    }

    /* Write data into the database, the key is defined by the data type. */
    private void writeData(Environment env,
                           Transaction txn, 
                           String processorName,
                           String dataSetName,
                           DatabaseEntry data,
                           DataType dataType,
                           OpType opType) {
        /* This method can only be invoked when env is not null. */
        assert env != null;

        Locker locker = null;
        Cursor cursor = null;
        boolean operationOK = false;

        try {
            locker = LockerFactory.getWritableLocker
                (env, txn, true /*isInternalDb*/, dbImpl.isTransactional(),
                 dbImpl.isReplicated());
            cursor = makeCursor(locker);

            DatabaseEntry key = new DatabaseEntry();
            StringBinding.stringToEntry
                (generateKey(processorName, dataSetName, dataType), key);

            DatabaseEntry oldData = new DatabaseEntry();
            cursor.getSearchKey(key, oldData, null);

            if (dataType == DataType.CHANGE_SET) {
                checkUsageErrors(oldData, opType, dataSetName);
            }

            OperationStatus status = null;
            if (opType == OpType.DELETE) {
                status = cursor.delete();
            } else {
                status = cursor.put(key, data);
            }

            assert status == OperationStatus.SUCCESS;
            
            operationOK = true;
        } finally {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }

            if (locker != null) {
                locker.operationEnd(operationOK);
            }
        }
    }

    private void checkUsageErrors(DatabaseEntry data,
                                  OpType opType,
                                  String dataSetName) {
        /* Check the usage errors. */
        switch (opType) {
            case DELETE:
                /* Usage error: remove an unexisted SyncDataSet. */
                if (data.getData() == null) {
                    throw new IllegalStateException
                        ("Data set does not exist: " + dataSetName);
                }
                break;
            case INSERT:
                /* Usage error: add an existed SyncDataSet again. */
                if (data.getData() != null) {
                    throw new IllegalStateException
                        ("Data set already exists: " + dataSetName);
                }
                break;
            case UPDATE:

                /*
                 * Usage error: update nextSyncStart for an unexisted
                 * SyncDataSet.
                 */
                if (data.getData() == null) {
                    throw new IllegalStateException
                        ("Data set does not exist:" + dataSetName);
                }
                break;
            default:
                throw EnvironmentFailureException.unexpectedState
                    ("Unrecognized SyncDB operation type: " + opType);
        }
    }
    
    public static String generateKey(String processorName, 
                                     String dataSetName, 
                                     DataType dataType) {
        String key = null;
        
        switch (dataType) {
            case PROCESSOR_METADATA:
                key = processorName + "-";
                break;
            case CHANGE_SET:
                key = processorName + "-" + dataSetName + "-";
                break;
            case PROCESSOR_TXN_DATA:
                key = processorName + "-" + dataSetName + "-";
                break;
            default:
                throw new IllegalArgumentException("Unrecognized data type.");
        }

        return (key + dataType.ordinal());
    }

    private Cursor makeCursor(Locker locker) {
        Cursor cursor = DbInternal.makeCursor(dbImpl, 
                                              locker,
                                              CursorConfig.DEFAULT);

        return cursor;
    }

    /* Read ChangeSet metadata of a SyncDataSet. */
    public void readChangeSetData(Environment env,
                                  Transaction txn,
                                  String processorName,
                                  String dataSetName,
                                  DatabaseEntry data) {
        readData
            (env, txn, processorName, dataSetName, data, DataType.CHANGE_SET);
    }

    /* Read data from database, the key is defined by the data type. */
    private void readData(Environment env,
                          Transaction txn,
                          String processorName,
                          String dataSetName,
                          DatabaseEntry data,
                          DataType dataType) {
        /* This method can only be invoked when env is not null. */
        assert env != null;

        Locker locker = null;
        Cursor cursor = null;
        boolean operationOK = false;

        try {
            locker = LockerFactory.getReadableLocker
                (env, txn, dbImpl.isTransactional(), false /*readCommitted*/);
            cursor = makeCursor(locker);

            DatabaseEntry key = new DatabaseEntry();
            StringBinding.stringToEntry
                (generateKey(processorName, dataSetName, dataType), key);

            OperationStatus status =
                cursor.getSearchKey(key, data, null);

            /* 
             * Read operations should be either succeed or read a non-existing
             * key. 
             */
            assert data.getData() == null || status == OperationStatus.SUCCESS;

            operationOK = true;
        } finally {
            if (cursor != null) {
                cursor.close();
                cursor = null;
            }

            if (locker != null) {
                locker.operationEnd(operationOK);
            }
        }
    }

    /* Write the imported server transactions information. */
    public void writeProcessorTxnData(Environment env,
                                      Transaction txn,
                                      String processorName,
                                      String dataSetName,
                                      DatabaseEntry data) {
        writeData(env, txn, processorName, dataSetName, data, 
                  DataType.PROCESSOR_TXN_DATA, null);
    }

    /* Read the imported server transactions information. */
    public void readProcessorTxnData(Environment env,
                                     Transaction txn,
                                     String processorName,
                                     String dataSetName,
                                     DatabaseEntry data) {
        readData(env, 
                 txn, 
                 processorName, 
                 dataSetName, 
                 data, 
                 DataType.PROCESSOR_TXN_DATA);
    }

    /* Return the db.count(). */
    public long getCount() {
        return dbImpl.count();
    }

    /* Return the DatabaseImpl for use. */
    public DatabaseImpl getDatabaseImpl() {
        return dbImpl;
    }

    /* Read the data for a specified DataType in SyncDB. */
    public Map<String, DatabaseEntry> readDataForType(DataType dataType,
                                                      Environment env) {
        if (dbImpl == null) {
            return null;
        }

        Locker locker = null;
        boolean operationOK = false;
        Map<String, DatabaseEntry> readData = 
            new HashMap<String, DatabaseEntry>();

        try {
            locker = LockerFactory.getReadableLocker
                (env, null, dbImpl.isTransactional(), false);
            
            Cursor cursor = makeCursor(locker);
            
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            while (OperationStatus.SUCCESS == 
                   cursor.getNext(key, data, null)) {
                String keyName = StringBinding.entryToString(key);
                if (DataType.getDataType(keyName).equals(dataType)) {
                    readData.put(keyName, data);
                }
                data = new DatabaseEntry();
            }

            operationOK = true;
        } finally {
            if (locker != null) {
                locker.operationEnd(operationOK);
            }
        }

        return readData;
    }                                                      
}
