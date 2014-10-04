package com.sleepycat.je.sync;

/**
 * Provides custom conversion between local field/property values and external
 * field/column values.
 */
public interface FieldConverter {

    /**
     * Converts a external field/column value to a local field/property value,
     * or returns fromValue unchanged if no custom conversion is needed.
     */
    Object externalToLocal(Object fromValue,
                           Class toClass,
                           String localFieldName,
                           String externalFieldName);

    /**
     * Converts a local field/property value to a external field/column value,
     * or returns fromValue unchanged if no custom conversion is needed.
     */
    Object localToExternal(Object fromValue,
                           Class toClass,
                           String localFieldName,
                           String externalFieldName);
}
