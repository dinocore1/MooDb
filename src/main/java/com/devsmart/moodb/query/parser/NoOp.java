package com.devsmart.moodb.query.parser;


public class NoOp implements ObjectOperation {
    @Override
    public Object eval(Object obj) {
        return obj;
    }
}
