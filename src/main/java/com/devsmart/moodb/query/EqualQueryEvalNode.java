package com.devsmart.moodb.query;


import org.apache.commons.jxpath.ri.compiler.LocationPath;

public class EqualQueryEvalNode extends QueryEvalNode {

    private final LocationPath mIndex;
    private final String mValue;

    public EqualQueryEvalNode(LocationPath index, String value) {
        mIndex = index;
        mValue = value;
    }


}
