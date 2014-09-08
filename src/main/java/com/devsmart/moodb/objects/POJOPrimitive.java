package com.devsmart.moodb.objects;

import com.google.gson.internal.LazilyParsedNumber;

public class POJOPrimitive extends DBPrimitive {

    static final Class<?>[] PRIMITIVE_TYPES = { int.class, long.class, short.class,
            float.class, double.class, byte.class, boolean.class, char.class, Integer.class, Long.class,
            Short.class, Float.class, Double.class, Byte.class, Boolean.class, Character.class };

    static boolean isPrimitiveOrString(Object target) {
        if (target instanceof String) {
            return true;
        }

        Class<?> classOfPrimitive = target.getClass();
        for (Class<?> standardPrimitive : PRIMITIVE_TYPES) {
            if (standardPrimitive.isAssignableFrom(classOfPrimitive)) {
                return true;
            }
        }
        return false;
    }

    public final Object mPOJO;

    public POJOPrimitive(Object obj) {
        mPOJO = obj;
    }

    @Override
    public boolean isString() {
        return mPOJO instanceof String;
    }

    @Override
    public String getAsString() {
        if (isNumber()) {
            return getAsNumber().toString();
        } else if (isBoolean()) {
            return ((Boolean)mPOJO).toString();
        } else {
            return (String) mPOJO;
        }
    }

    @Override
    public boolean isBoolean() {
        return mPOJO instanceof Boolean;
    }

    @Override
    public boolean getAsBoolean() {
        return (Boolean) mPOJO;
    }

    @Override
    public boolean isNumber() {
        return mPOJO instanceof Number;
    }

    public Number getAsNumber() {
        return mPOJO instanceof String ? new LazilyParsedNumber((String) mPOJO) : (Number) mPOJO;
    }

    @Override
    public double getAsDouble() {
        return isNumber() ? getAsNumber().doubleValue() : Double.parseDouble(getAsString());
    }

    @Override
    public DBCollection getAsCollection() {
        return null;
    }

    @Override
    public DBObject getAsObject() {
        return null;
    }

    @Override
    public DBPrimitive getAsPrimitive() {
        return this;
    }

    @Override
    public int hashCode() {
        return mPOJO.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof POJOPrimitive){
            return mPOJO.equals(((POJOPrimitive) obj).mPOJO);
        } else {
            return mPOJO.equals(obj);
        }
    }
}
