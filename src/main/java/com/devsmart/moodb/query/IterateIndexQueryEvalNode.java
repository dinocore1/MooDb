package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;
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

    @Override
    public MooDBCursor createCursor(MooDB context) {
        return null;
    }
}
