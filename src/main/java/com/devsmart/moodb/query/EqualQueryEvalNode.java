package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;
import org.apache.commons.jxpath.ri.compiler.LocationPath;

public class EqualQueryEvalNode extends QueryEvalNode {

    private final LocationPath mIndex;
    private final Object mValue;

    public EqualQueryEvalNode(LocationPath index, Object value) {
        mIndex = index;
        mValue = value;
    }

    @Override
    public String toString() {
        return String.format("[i:%s = %s]", mIndex, mValue);
    }

    @Override
    public MooDBCursor createCursor(MooDB context) {
        return null;
    }
}
