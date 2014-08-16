package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBCursor;

public abstract class QueryEvalNode {

    public abstract MooDBCursor createCursor(MooDB context);
}
