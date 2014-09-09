package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBElement;
import com.devsmart.moodb.objects.DBPrimitive;

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
    public boolean matches(DBElement input) {
        DBElement obj = mLeftOp.eval(input);
        if(obj == null) {
            return false;
        }

        if(obj.isPrimitive()){
            DBPrimitive primitive = obj.getAsPrimitive();
            if(primitive.isString()) {
                return compare(primitive.getAsString().compareTo(mValue));
            } else if(primitive.isNumber()){
                return compare(Double.compare(primitive.getAsDouble(), Double.parseDouble(mValue)));
            } else if(primitive.isBoolean()) {
                return compare(primitive.getAsBoolean() == Boolean.parseBoolean(mValue) ? 0 : 1);
            } else {
                logger.error("wtf");
                return false;
            }
        } else {
            logger.warn("no comparator for obj: {}", obj);
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
