package com.devsmart.moodb;

import com.devsmart.moodb.objectquery.ObjectOperation;
import com.devsmart.moodb.objectquery.QueryBuilder;
import com.devsmart.moodb.objects.DBElement;
import com.devsmart.moodb.objects.DBPrimitive;
import com.sleepycat.bind.tuple.SortedDoubleBinding;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryMultiKeyCreator;

import java.util.Set;

public class Index implements SecondaryMultiKeyCreator, Comparable<Index> {

    public final String indexQuery;
    private ObjectOperation mObjectOperation;
    SecondaryDatabase indexDB;

    Index(String indexQuery) {
        this.indexQuery = indexQuery;
    }

    private ObjectOperation getObjectOperation() {
        if(mObjectOperation == null) {
            mObjectOperation = QueryBuilder.compile(indexQuery);
        }
        return mObjectOperation;
    }

    public SecondaryDatabase getIndexDB() {
        return indexDB;
    }

    @Override
    public void createSecondaryKeys(SecondaryDatabase secondary, DatabaseEntry key, DatabaseEntry data, Set<DatabaseEntry> results) {
        DBElement dbObj = Utils.toDBElement(data);
        if(dbObj.isCollection()) {
            for(DBElement el : dbObj.getAsCollection()){
                emitKey(el, results);
            }
        } else {
            emitKey(dbObj, results);
        }
    }

    private void emitKey(DBElement dbEl, Set<DatabaseEntry> results) {
        DBElement result = getObjectOperation().eval(dbEl);
        if(result != null) {
            if(result.isCollection()){
                for(DBElement el : result.getAsCollection()){
                    if(el.isPrimitive()){
                        emitKey(el.getAsPrimitive(), results);
                    }
                }
            } else if(result.isPrimitive()) {
                emitKey(result.getAsPrimitive(), results);
            }
        }
    }

    private void emitKey(DBPrimitive key, Set<DatabaseEntry> results) {
        DatabaseEntry entry = new DatabaseEntry();
        if(key.isString() || key.isBoolean()) {
            entry.setData(Utils.toBytes(key.getAsString()));
            results.add(entry);
        } else if(key.isNumber()) {
            SortedDoubleBinding.doubleToEntry(key.getAsDouble(), entry);
            results.add(entry);
        }

    }

    @Override
    public int compareTo(Index other) {
        return indexQuery.compareTo(other.indexQuery);
    }
}
