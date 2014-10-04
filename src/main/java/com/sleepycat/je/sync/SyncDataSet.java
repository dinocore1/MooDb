package com.sleepycat.je.sync;

import java.io.Serializable;
import java.util.Collection;

/**
 * Defines a sychronized data set as a set of local databases to be
 * synchronized with an external system.  An instance of this class is created
 * by calling SyncProcessor.addDataSet.
 */
public abstract class SyncDataSet implements Serializable {
    /* The name of the data set. */
    private final String dataSetName;

    /* The processor to which this data set belongs. */
    private transient SyncProcessor processor;

    /* The collection of SyncDatabases this data set monitors. */
    private final Collection<SyncDatabase> databases;

    /**
     * Used by subclasses to create a SyncDataSet.
     */
    protected SyncDataSet(String dataSetName,
                          SyncProcessor processor, 
                          Collection<SyncDatabase> databases) {
        this.dataSetName = dataSetName;
        this.databases = databases;
        initSyncProcessor(processor);
    }

    /**
     * @hidden
     *
     * Need to invoke it after de-serialization in SyncDB, so the method has to
     * be public.
     */
    public void initSyncProcessor(SyncProcessor processor) {
        this.processor = processor;
    }

    /**
     * Returns the name of the data set, which is unique among all data sets
     * for each SyncProcessor instance.
     */
    public String getName() {
        return dataSetName;
    }

    /**
     * Returns the SyncProcessor that manages this data set.
     */
    public SyncProcessor getProcessor() {
        return processor;
    }

    /**
     * Returns the databases that are synchronized.
     */
    public Collection<SyncDatabase> getDatabases() {
        return databases;
    }
}
