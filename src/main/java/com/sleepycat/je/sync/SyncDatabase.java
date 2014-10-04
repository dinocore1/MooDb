package com.sleepycat.je.sync;

import java.io.Serializable;

/**
 * Defines a synchronized database as a mapping between external table/view
 * name and local JE database name, and includes a RecordConverter for
 * converting between local and external record data.
 */
public class SyncDatabase implements Serializable {
    /* The snapshot name of this database on Mobile server. */
    private final String externalName;

    /* The name of this JE database in JE Environment. */
    private final String localName;

    /* RecordConverter used to convert records in this database. */
    private final RecordConverter converter;

    /**
     * Creates a synchronized database definition.
     */
    public SyncDatabase(String externalName,
                        String localName,
                        RecordConverter converter) {
        this.externalName = externalName;
        this.localName = localName;
        this.converter = converter;
    }

    /**
     * Returns the name of the external table/view.
     */
    public String getExternalName() {
        return externalName;
    }

    /**
     * Returns the name of the local JE Database, or null if a default local
     * name is to be used but has not yet been assigned.
     */
    public String getLocalName() {
        return localName;
    }

    /**
     * Returns the record converter. Note that this may be a RecordMapping,
     * which implements the RecordConverter interface.
     */
    public RecordConverter getConverter() {
        return converter;
    }
}
