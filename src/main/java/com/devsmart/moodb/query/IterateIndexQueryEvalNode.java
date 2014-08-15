package com.devsmart.moodb.query;


import org.apache.commons.jxpath.ri.compiler.LocationPath;

public class IterateIndexQueryEvalNode extends QueryEvalNode {

    private final LocationPath mIndex;

    public IterateIndexQueryEvalNode(LocationPath index) {
        mIndex = index;
    }

    @Override
    public String toString() {
        return String.format("all %s", mIndex);
    }
}
