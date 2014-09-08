package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBCollection;
import com.devsmart.moodb.objects.DBElement;

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

    public DBElement eval(DBElement input) {

        DBElement retval = null;
        if(input != null) {
            if(input.isCollection()) {
                retval = evalCollection(input.getAsCollection());
            } else {
                retval = evalOne(input);
            }
        }

        if(mNextStep != null) {
            retval = mNextStep.eval(retval);
        }

        return retval;
    }

    private DBElement evalCollection(DBCollection input) {
        LinkedList<DBElement> retval = new LinkedList<DBElement>();
        for(DBElement obj : input) {
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
            return DBCollection.wrap(retval);
        }
    }

    private DBElement evalOne(DBElement input) {
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
