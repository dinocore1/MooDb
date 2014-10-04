package com.sleepycat.je.sync.mobile;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.sync.ExportConfig;
import com.sleepycat.je.sync.ImportConfig;
import com.sleepycat.je.sync.SyncDataSet;
import com.sleepycat.je.sync.SyncDatabase;
import com.sleepycat.je.sync.SyncProcessor;

/**
 * Manages one or more synchronized data sets for Oracle Mobile Server and
 * processes sync operations.
 */
public class MobileSyncProcessor extends SyncProcessor {

    /* Save datasets registered to this MobileSyncProcessor. */
    private final Map<String, SyncDataSet> dataSets = 
        new HashMap<String, SyncDataSet>(); 

    /**
     * Creates a SyncProcessor for Oracle Mobile Server.
     */
    public MobileSyncProcessor(Environment env,
                               String processorName,
                               MobileConnectionConfig connectionConfig) {
        super(env, processorName);
    }

    /**
     * Returns the connection configuration.
     */
    public MobileConnectionConfig getConnectionConfig() {
        return null;
    }

    /**
     * Changes the connection configuration.
     */
    public void setConnectionConfig(MobileConnectionConfig connectionConfig) {
    }

    /**
     * @see com.sleepycat.je.sync.SyncProcessor#addDataSet
     */
    @Override
    public SyncDataSet addDataSet(String dataSetName,
                                  Collection<SyncDatabase> databases) {

        /* Create a new MobileDataSet, register it to this SyncProcessor. */
        MobileDataSet dataSet = 
            new MobileDataSet(dataSetName, this, databases);
        registerDataSet(dataSet);
        dataSets.put(dataSetName, dataSet);

        return dataSet;
    }

    public void removeDataSet(String dataSetName) {
    }

    /**
     * @see com.sleepycat.je.sync.SyncProcessor#getDataSets
     */
    @Override
    public Map<String, SyncDataSet> getDataSets() {
        return dataSets;
    }

    public void syncAll(ExportConfig exportConfig, ImportConfig importConfig) {
    }

    public void sync(ExportConfig exportConfig,
                     ImportConfig importConfig,
                     String... dataSetName) {
    }

    public void cancelSync() {
    }
}
