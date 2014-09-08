package com.devsmart.moodb.objects;


public abstract class DBPrimitive implements DBElement {

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    @Override
    public boolean isObject() {
        return false;
    }

    public abstract boolean isString();
    public abstract String getAsString();
    public abstract boolean isNumber();
    public abstract double getAsDouble();
    public abstract boolean isBoolean();
    public abstract boolean getAsBoolean();
}
