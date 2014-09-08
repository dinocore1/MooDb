package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBElement;

public class NoOp implements ObjectOperation {
    @Override
    public DBElement eval(DBElement obj) {
        return obj;
    }
}
