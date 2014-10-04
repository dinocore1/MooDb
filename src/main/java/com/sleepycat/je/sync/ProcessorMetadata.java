package com.sleepycat.je.sync;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Used for storing a serialized form of processor-specific metadata, including
 * a collection of SyncDataSet objects and processor connection properties;
 * used only by SyncProcessor implementations.  A SyncProcessor implementation
 * calls {@link SyncProcessor#writeProcessorMetadata} {@link
 * SyncProcessor#readProcessorMetadata}.
 *
 * <p>A SyncProcessor implementation will normally subclass ProcessorMetadata
 * to add processor specific connection properties and other metadata.  Because
 * Java serialization is used to store the metadata object, the subclass must
 * be serializable.  Note that SyncDataSet may also be subclassed to add
 * processor-specific information.</p>
 */
public class ProcessorMetadata<S extends SyncDataSet> implements Serializable {

    private Map<String, S> dataSets = new HashMap<String, S>();

    public Collection<S> getDataSets() {
        return dataSets.values();
    }

    public void addDataSet(S dataSet) {
        dataSets.put(dataSet.getName(), dataSet);
    }

    public void removeDataSet(String name) {
        dataSets.remove(name);
    }
}
