package com.devsmart.moodb;

import com.devsmart.moodb.query.*;
import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.ri.compiler.*;

import java.util.List;

public class IndexChooser {


    private final LocationPath mQuery;
    private final List<LocationPath> mIndexes;

    public IndexChooser(LocationPath query, List<LocationPath> indexes) {
        mQuery = query;
        mIndexes = indexes;
    }

    private LocationPath findIndex(String xpath) {
        LocationPath retval = null;
        String expression = JXPathContext.compile(xpath).toString();

        for(LocationPath index : mIndexes){
            String indexStr = index.toString();
            if(expression.equals(indexStr)){
                retval = index;
                break;
            }
        }
        return retval;
    }

    public QueryEvalNode generateExecutionPlan() {
        JoinQueryEvalNode root = new JoinQueryEvalNode(mQuery);
        for(int step=0;step<mQuery.getSteps().length;step++) {
            Step pathStep = mQuery.getSteps()[step];
            for(Expression pred : pathStep.getPredicates()){
                generateExecutionPlan(root, pred, step);
            }


        }

        return root;
    }

    private void generateExecutionPlan(CombineQueryEvalNode node, Expression pred, int step) {

        if(pred instanceof CoreOperationEqual){
            CoreOperationEqual equalOperation = (CoreOperationEqual) pred;
            Expression[] args = equalOperation.getArguments();

            String key = args[0].toString();
            Object value = args[1].compute(null);

            StringBuilder possibleIndex = new StringBuilder();
            for(int i=0;i<step;i++){
                possibleIndex.append("./");
            }
            possibleIndex.append(key);
            LocationPath index = findIndex(possibleIndex.toString());
            if(index != null){
                node.add(new EqualQueryEvalNode(index, value));
            } else {
                node.add(new IterateAllEvalNode());
            }

        } else if(pred instanceof CoreOperationAnd){
            CoreOperationAnd andOperation = (CoreOperationAnd) pred;

            JoinQueryEvalNode newNode = new JoinQueryEvalNode(pred);
            node.add(newNode);
            generateExecutionPlan(newNode, andOperation.getArguments()[0], step);
            generateExecutionPlan(newNode, andOperation.getArguments()[1], step);
        } else if(pred instanceof CoreOperationOr) {
            CoreOperationOr orOperation = (CoreOperationOr) pred;

            OrQueryEvalNode newNode = new OrQueryEvalNode(pred);
            node.add(newNode);
            generateExecutionPlan(newNode, orOperation.getArguments()[0], step);
            generateExecutionPlan(newNode, orOperation.getArguments()[1], step);
        }
    }

}
