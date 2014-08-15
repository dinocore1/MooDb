package com.devsmart.moodb.query;


import org.apache.commons.jxpath.ri.compiler.Expression;

import java.util.ArrayList;

public abstract class CombineQueryEvalNode extends QueryEvalNode {

    protected ArrayList<QueryEvalNode> mChildren = new ArrayList<QueryEvalNode>();

    public CombineQueryEvalNode(Expression query) {
        super(query);
    }

    public void add(QueryEvalNode node) {
        mChildren.add(node);
    }


}
