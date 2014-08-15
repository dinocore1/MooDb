package com.devsmart.moodb.query;

import com.google.common.base.Joiner;
import org.apache.commons.jxpath.ri.compiler.Expression;


public class OrQueryEvalNode extends CombineQueryEvalNode {

    public OrQueryEvalNode(Expression query) {

    }

    @Override
    public String toString() {
        return Joiner.on(" or ").join(mChildren);
    }
}
