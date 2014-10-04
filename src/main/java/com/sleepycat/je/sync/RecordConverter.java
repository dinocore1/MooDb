package com.sleepycat.je.sync;

import java.io.Serializable;

import com.sleepycat.je.DatabaseEntry;

/**
 * Provides custom data conversion between records in an external table/view
 * and raw data records in a local database.
 *
 * <p>This low-level interface is implemented by the RecordMapping class, which
 * is normally used whenever a JE binding is available.  When a binding is not
 * available, or the RecordMapping class is not sufficient, the user may
 * implement this interface directly.</p>
 */
public interface RecordConverter extends Serializable {

    /**
     * Initializes the converter with the external field/column names and data
     * types.  The array of external field values will correspond by inddex to
     * the two arrays given here.
     */
    void initializeConverter(Class[] externalFieldTypes,
                             String[] externalFieldNames);

    /**
     * Converts the local raw data record to an array of external record
     * values.
     */
    void convertLocalToExternal(DatabaseEntry localKey,
                                DatabaseEntry localData,
                                Object[] externalFieldValues);

    /**
     * Converts an array of external record values to a local raw data record.
     *
     * <p>If localData is non-empty (DatabaseEntry.getSize() returns greater
     * than zero), updates the existing record; otherwise, creates a new local
     * record.</p>
     */
    void convertExternalToLocal(Object[] externalFieldValues,
                                DatabaseEntry localKey,
                                DatabaseEntry localData);

    /**
     * Returns an array of the types of fields in the external RDBMS.
     *
     * @return an array of the fields' types in the external RDBMS.
     */
    Class[] getExternalFieldTypes();

    /**
     * Returns an array of the names of fields in the external RDBMS.
     *
     * @return an array of fields' names in the external RDBMS.
     */
    String[] getExternalFieldNames();
}
