package com.devsmart.moodb.objects;

public abstract class DBObject implements DBElement {

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public boolean isObject() {
        return true;
    }

    public abstract DBElement get(String fieldName);
}
