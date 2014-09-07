package com.devsmart.moodb.query.parser;


public class EqualsPredicate implements Predicate {

    public EqualsPredicate(ExtractField extractField, String value) {

    }

    @Override
    public boolean matches(Object input) {
        return false;
    }
}
