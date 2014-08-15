package com.devsmart.moodb.query;


import java.util.ArrayList;

public abstract class CombineQueryEvalNode extends QueryEvalNode {

    protected ArrayList<QueryEvalNode> mChildren = new ArrayList<QueryEvalNode>();

    public void add(QueryEvalNode node) {
        mChildren.add(node);
    }


}
