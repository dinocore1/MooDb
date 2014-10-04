package com.sleepycat.je.sync.jdbc;

import java.util.Collection;
import java.util.Map;

import com.sleepycat.je.Environment;
import com.sleepycat.je.sync.ExportConfig;
import com.sleepycat.je.sync.ImportConfig;
import com.sleepycat.je.sync.SyncDataSet;
import com.sleepycat.je.sync.SyncDatabase;
import com.sleepycat.je.sync.SyncProcessor;

public class JDBCSyncProcessor extends SyncProcessor {

    /**
     * Creates a SyncProcessor for synchronizing data between JE and an
     * external data repository via JDBC.
     */
    public JDBCSyncProcessor(Environment env,
                             String processorName,
                             JDBCConnectionConfig connectionConfig) {
        super(env, processorName);
    }

    /**
     * Returns the connection configuration.
     */
    public JDBCConnectionConfig getConnectionConfig() {
        return null;
    }

    /**
     * Changes the connection configuration.
     */
    public void setConnectionConfig(JDBCConnectionConfig connectionConfig) {
    }

    public SyncDataSet addDataSet(String dataSetName,
                                  Collection<SyncDatabase> databases) {
        //return new MobileDataSet(dataSetName, this, databases);
        return null;
    }

    public void removeDataSet(String dataSetName) {
    }

    public Map<String, SyncDataSet> getDataSets() {
        return null;
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
