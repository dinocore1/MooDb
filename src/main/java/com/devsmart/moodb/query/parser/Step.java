package com.devsmart.moodb.query.parser;


import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

public class Step implements ObjectOperation {

    private final ObjectOperation mOperation;
    private final Predicate mPredicate;
    private Step mNextStep;

    public Step(ObjectOperation step, Predicate predicate) {
        mOperation = step;
        mPredicate = predicate;

    }

    public void setNextStep(Step step) {
        mNextStep = step;
    }

    public Object eval(Object input) {

        Object retval = null;
        if(input != null) {
            if(input instanceof Collection) {
                retval = evalCollection((Collection) input);
            } else if(input.getClass().isArray()) {
                retval = evalCollection(Arrays.asList(input));
            } else {
                retval = evalOne(input);
            }
        }

        if(mNextStep != null) {
            retval = mNextStep.eval(retval);
        }

        return retval;
    }

    private Object evalCollection(Collection input) {
        LinkedList<Object> retval = new LinkedList<Object>();
        for(Object obj : input) {
            obj = evalOne(obj);
            if(obj != null){
                retval.add(obj);
            }
        }

        if(retval.size() == 0) {
            return null;
        } if(retval.size() == 1){
            return retval.getFirst();
        } else {
            return retval;
        }
    }

    private Object evalOne(Object input) {
        if(mPredicate != null) {
            if(mPredicate.matches(input)){
                return mOperation.eval(input);
            } else {
                return null;
            }
        } else {
            return mOperation.eval(input);
        }
    }
}
