package com.devsmart.moodb.query;


import com.google.common.base.Joiner;
import org.apache.commons.jxpath.ri.compiler.Expression;

public class JoinQueryEvalNode extends CombineQueryEvalNode {

    public JoinQueryEvalNode(Expression query) {
        super(query);
    }

    @Override
    public String toString() {
        return Joiner.on(" join ").join(mChildren);
    }
}
