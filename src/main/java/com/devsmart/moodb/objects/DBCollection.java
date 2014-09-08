package com.devsmart.moodb.objects;


import java.util.Collection;
import java.util.Iterator;

public abstract class DBCollection implements DBElement, Iterable<DBElement> {

    public static DBCollection wrap(final Collection<DBElement> collection) {
        return new DBCollection() {
            @Override
            public DBCollection getAsCollection() {
                return this;
            }

            @Override
            public DBObject getAsObject() {
                return null;
            }

            @Override
            public DBPrimitive getAsPrimitive() {
                return null;
            }

            @Override
            public Iterator<DBElement> iterator() {
                return collection.iterator();
            }
        };
    }

    @Override
    public boolean isCollection() {
        return true;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean isObject() {
        return false;
    }

}
