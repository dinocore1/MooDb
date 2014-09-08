package com.devsmart.moodb.objects;


import java.lang.reflect.Field;

public class POJOObject extends DBObject {

    public final Object mPOJO;

    public POJOObject(Object obj) {
        mPOJO = obj;
    }

    @Override
    public DBElement get(String fieldName) {

        Object pojo = null;
        Field field = getField(mPOJO.getClass(), fieldName);
        if(field != null) {
            try {
                field.setAccessible(true);
                pojo = field.get(mPOJO);
            } catch (Exception e) {}
        }

        if(pojo != null){
            return POJODBElement.wrap(pojo);
        }

        return null;
    }

    private static Field getField(Class<?> classType, String fieldName) {
        if(classType == null) {
            return null;
        }
        try {
            Field retval = classType.getDeclaredField(fieldName);
            return retval;
        } catch (NoSuchFieldException e) {}

        return getField(classType.getSuperclass(), fieldName);
    }

    @Override
    public int hashCode() {
        return mPOJO.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return mPOJO.equals(obj);
    }

    @Override
    public DBCollection getAsCollection() {
        return null;
    }

    @Override
    public DBObject getAsObject() {
        return this;
    }

    @Override
    public DBPrimitive getAsPrimitive() {
        return null;
    }
}
