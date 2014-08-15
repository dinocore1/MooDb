package com.devsmart.moodb;

import org.apache.commons.jxpath.ri.Parser;
import org.apache.commons.jxpath.ri.compiler.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class QueryTest {

    private Map<String, Object> createWidget(String type, int value) {
        HashMap<String, Object> retval = new HashMap<String, Object>();
        retval.put("type", type);
        retval.put("value", value);
        retval.put("id", UUID.randomUUID());

        return retval;
    }

    public LocationPath compileXPath(String xpath) {
        return (LocationPath)Parser.parseExpression(xpath, new TreeCompiler());
    }

    @Test
    public void testQuery() {

        ArrayList<Map> db = new ArrayList<Map>();
        for(int i=0;i<5;i++){
            db.add(createWidget("car", i));
        }
        for(int i=0;i<5;i++){
            db.add(createWidget("plane", i));
        }

        LocationPath index1 = compileXPath("type");
        LocationPath index2 = compileXPath("value");
        LocationPath index3 = compileXPath(".[type='car']");

        LocationPath query = compileXPath(".[type='car' and value='4']");
        //LocationPath query = compileXPath(".[type='car']/");

        findIndex(query, index1);

    }

    public void findIndex(LocationPath query, LocationPath index) {
        for(int i=0;i<query.getSteps().length;i++){
            Step queryStep = query.getSteps()[i];

            if(index.getSteps().length >= i){
                Step indexStep = index.getSteps()[i];

                for(Expression queryPredicate : queryStep.getPredicates()){
                    if(predicateMatchIndex(queryPredicate, indexStep)){
                        System.out.println("yes");
                    }
                }
            }

        }
    }

    public boolean predicateMatchIndex(Expression queryPredicate, Step indexStep) {

        if(queryPredicate instanceof CoreOperationEqual) {
            CoreOperationEqual equalOperation = (CoreOperationEqual) queryPredicate;
            for(Expression arg : equalOperation.getArguments()){
                if(arg instanceof LocationPath) {
                    if (arg.toString().equals(indexStep.toString())) {
                        return true;
                    }
                }
            }
        } else if(queryPredicate instanceof CoreOperationAnd) {
            CoreOperationAnd andOperation = (CoreOperationAnd) queryPredicate;
            for(Expression e : andOperation.getArguments()){
                if(predicateMatchIndex(e, indexStep)){
                    return true;
                }
            }
        }

        return false;
    }
}
