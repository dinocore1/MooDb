package com.devsmart.moodb.objects;


import java.lang.reflect.Array;
import java.util.Iterator;

public class POJOArray extends DBCollection {

    public final Object mPOJO;
    private final int mLength;

    public POJOArray(Object obj) {
        mPOJO = obj;
        mLength = Array.getLength(mPOJO);
    }


    @Override
    public Iterator<DBElement> iterator() {
        return new Iterator<DBElement>() {

            int i = -1;

            @Override
            public boolean hasNext() {
                return mLength > 0 && i < mLength-1;
            }

            @Override
            public DBElement next() {
                Object pojo = Array.get(mPOJO, ++i);
                return POJODBElement.wrap(pojo);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
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
}
