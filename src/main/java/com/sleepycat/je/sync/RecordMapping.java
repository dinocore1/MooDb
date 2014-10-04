package com.sleepycat.je.sync;

import java.util.Map;

import com.sleepycat.bind.EntityBinding;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

/**
 * Defines record conversion based on the fields/properties of a Java object,
 * and uses a JE binding to convert between local raw records and Java objects.
 *
 * <p>The set of field/properties in a Java object that are converted are those
 * that are considered to be persistent.  When bean properties are not used,
 * the definition of persistent fields is the same as used in the DPL (TODO:
 * add doc reference).  When bean properties are used, all Java bean properties
 * are considered to be persistent.  The set of persistent fields/properties
 * can be customized using the fieldMapping.</p>
 */
public class RecordMapping implements RecordConverter {

    /**
     * Creates a complete record mapping using an entity binding, with a null
     * fieldMapping, fieldConverter, and newRecordClass.
     */
    public RecordMapping(EntityBinding entityBinding,
                         boolean beanProperties) {
    }

    /**
     * Creates a record mapping using an entity binding, with an optional
     * fieldMapping, fieldConverter, and newRecordClass.
     */
    public RecordMapping(EntityBinding entityBinding,
                         boolean beanProperties,
                         Map<String, String> fieldMapping,
                         boolean partialMapping,
                         Class newRecordClass) {
    }

    /**
     * Creates a complete record mapping using separate key and value bindings,
     * with a null fieldMapping, fieldConverter, and newRecordClass.
     */
    public RecordMapping(EntryBinding keyBinding,
                         EntryBinding valueBinding,
                         boolean beanProperties) {
    }

    /**
     * Creates a record mapping using separate key and value bindings, with an
     * optional fieldMapping, fieldConverter, and newRecordClass.
     */
    public RecordMapping(EntryBinding keyBinding,
                         EntryBinding valueBinding,
                         boolean beanProperties,
                         Map<String, String> fieldMapping,
                         boolean partialMapping,
                         Class newRecordClass) {
    }

    /**
     * Returns the entity binding, or null if key and value bindings are used.
     */
    public EntityBinding getEntityBinding() {
        return null;
    }

    /**
     * Returns the key binding, or null if an entity binding is used.
     */
    public EntryBinding getKeyBinding() {
        return null;
    }

    /**
     * Returns the value binding, or null if an entity binding is used.
     */
    public EntryBinding getValueBinding() {
        return null;
    }

    /**
     * Returns true if local bean property names are used or false if local
     * field names are used.
     */
    public boolean getBeanProperties() {
        return false;
    }

    /**
     * Returns a Map from local field/property name to external field/column
     * name, or null if no custom mapping is used.
     */
    public Map<String, String> getFieldMapping() {
        return null;
    }

    /**
     * Returns true if the field map is a partial mapping and other field names
     * that match should be transferred, or false if it is a complete mapping
     * and omitted names should not be transferred.
     */
    public boolean getPartialMapping() {
        return false;
    }

    /**
     * Returns the class used to bind a newly inserted record in a JE Database.
     *
     * <p>When performing an import and no JE record currently exists, the
     * Class.forName method is called to create a Java object for binding.</p>
     *
     * <p>The newRecordClass may be null when import operations are not used
     * for this data set.
     */
    public Class getNewRecordClass() {
        return null;
    }

    /**
     * Called internally to initialize the converter.
     */
    public void initializeConverter(Class[] externalFieldTypes,
                                    String[] externalFieldNames) {
    }

    /**
     * Called internally to convert the local raw data record to an array of
     * external record values.
     *
     * <p>Converts the local raw key/value to a Java object using the entity or
     * key/value bindings, and returns the external field/column values
     * obtained from the Java object using reflection.</p>
     */
    public void convertLocalToExternal(DatabaseEntry localKey,
                                       DatabaseEntry localData,
                                       Object[] externalFieldValues) {
    }

    /**
     * Called internally to convert an array of external record values to a
     * local raw data record.
     *
     * <p>Converts the external field/column values to a Java object using
     * reflection, and returns the local raw key/value obtained using the
     * entity or key/value bindings.</p>
     */
    public void convertExternalToLocal(Object[] externalFieldValues,
                                       DatabaseEntry localKey,
                                       DatabaseEntry localData) {
    }

    /**
     * @see RecordConverter#getExternalFieldTypes
     */
    public Class[] getExternalFieldTypes() {
        return null;
    }

    /**
     * @see RecordConverter#getExternalFieldNames
     */
    public String[] getExternalFieldNames() {
        return null;
    }
}
