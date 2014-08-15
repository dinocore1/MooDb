package com.devsmart.moodb.query;


import org.apache.commons.jxpath.ri.compiler.Expression;

import java.util.ArrayList;
import java.util.Arrays;

public class EqualQueryEvalNode extends QueryEvalNode {

    public EqualQueryEvalNode(Expression query) {
        super(query);
    }

    public final ArrayList<String> possibleIndexes = new ArrayList<String>();

    @Override
    public String toString() {
        return String.format("[%s]", Arrays.toString(possibleIndexes.toArray(new String[possibleIndexes.size()])));
    }
}
