package com.devsmart.moodb.query.parser;


public class EqualsPredicate implements Predicate {

    private final ObjectOperation mOp;
    private final String mValue;

    public EqualsPredicate(ObjectOperation op, String value) {
        mOp = op;
        mValue = value;
    }

    @Override
    public boolean matches(Object input) {
        Object obj = mOp.eval(input);
        return obj != null && obj.equals(mValue);
    }
}
