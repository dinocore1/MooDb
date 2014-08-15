package com.devsmart.moodb;

import org.apache.commons.jxpath.ri.compiler.CoreOperationEqual;
import org.apache.commons.jxpath.ri.compiler.Expression;
import org.apache.commons.jxpath.ri.compiler.LocationPath;

import java.util.Collection;
import java.util.List;

public class IndexChooser {

    private class ExpressionNode {
        Expression xpathExpression;
        MooDBCursor cursor;
    }


    private final LocationPath mQuery;
    private final List<LocationPath> mIndexes;

    public IndexChooser(LocationPath query, List<LocationPath> indexes) {
        mQuery = query;
        mIndexes = indexes;
    }

    public ExpressionNode generateExecutionPlan() {



    }

    private ExpressionNode generateExpressionPlan(ExpressionNode parent, Collection<Expression>) {
        if(parent.xpathExpression instanceof CoreOperationEqual){

        }
    }
}
