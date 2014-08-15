package com.devsmart.moodb.query;


import org.apache.commons.jxpath.ri.compiler.Expression;

public abstract class QueryEvalNode {

    final Expression queryExpression;

    public QueryEvalNode(Expression query){
        queryExpression = query;
    }
}
