package com.devsmart.moodb.objects;


public interface DBElement {

    boolean isCollection();
    DBCollection getAsCollection();

    boolean isObject();
    DBObject getAsObject();

    boolean isPrimitive();
    DBPrimitive getAsPrimitive();

}
