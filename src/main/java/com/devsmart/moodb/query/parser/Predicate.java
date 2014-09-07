package com.devsmart.moodb.query.parser;


public interface Predicate {

    boolean matches(Object input);
}
