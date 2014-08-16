package com.devsmart.moodb.query;

import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;

public class IterateAllEvalNode extends QueryEvalNode {
    @Override
    public MooDBCursor createCursor(MooDB context) {
        return null;
    }

    @Override
    public String toString() {
        return "ALL";
    }
}
