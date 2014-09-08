package com.devsmart.moodb.objects;

import java.util.Collection;
import java.util.Iterator;

public class POJOCollection extends DBCollection {

    private final Collection mCollection;

    public POJOCollection(Collection obj) {
        mCollection = obj;
    }

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
        return new Iterator<DBElement>() {

            Iterator iterator = mCollection.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public DBElement next() {
                return POJODBElement.wrap(iterator.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
