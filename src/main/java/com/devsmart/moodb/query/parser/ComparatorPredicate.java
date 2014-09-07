package com.devsmart.moodb.query.parser;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class ComparatorPredicate implements Predicate {

    Logger logger = LoggerFactory.getLogger(ComparatorPredicate.class);

    private final ObjectOperation mLeftOp;
    private final String mValue;
    private final String mOperation;

    public ComparatorPredicate(ObjectOperation op, String value, String operation) {
        mLeftOp = op;
        mValue = value;
        mOperation = operation;
    }

    @Override
    public boolean matches(Object input) {
        Object obj = mLeftOp.eval(input);
        if(obj == null) {
            return false;
        }

        if(obj instanceof String) {
            return compare(((String) obj).compareTo(mValue));
        } else if(obj instanceof Number) {
            return compare(new BigDecimal(((Number) obj).doubleValue()).compareTo(new BigDecimal(mValue)));
        } else {
            logger.warn("no comparator for class: {} {}", obj.getClass().getName(), obj);
            return false;
        }
    }

    private boolean compare(int value) {
        if("=".equals(mOperation)){
            return value == 0;
        } else if(">=".equals(mOperation)) {
            return value >= 0;
        } else if("<=".equals(mOperation)) {
            return value <= 0;
        } else if(">".equals(mOperation)) {
            return value > 0;
        } else if("<".equals(mOperation)) {
            return value < 0;
        } else {
            logger.error("unknown compare operator: {}", mOperation);
            return false;
        }
    }
}
