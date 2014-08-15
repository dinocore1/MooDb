package com.devsmart.moodb;

import com.devsmart.moodb.query.*;
import org.apache.commons.jxpath.ri.compiler.*;

import java.util.List;

public class IndexChooser {


    private final LocationPath mQuery;
    private final List<LocationPath> mIndexes;

    public IndexChooser(LocationPath query, List<LocationPath> indexes) {
        mQuery = query;
        mIndexes = indexes;
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


            boolean foundIndex = false;
            for(LocationPath index : mIndexes){
                if(index.getSteps().length >= step){
                    Step indexStep = index.getSteps()[step];

                    if(indexStep.toString().equals(equalOperation)){
                        node.add(new IterateIndexQueryEvalNode(index));
                        foundIndex = true;

                    } else if(args[0].toString().equals(indexStep.toString())){
                        EqualQueryEvalNode equalNode = new EqualQueryEvalNode(index, args[1].toString());
                        node.add(equalNode);
                        foundIndex = true;
                    }
                }
            }

            if(!foundIndex){
                node.add(new IterateAllEvalNode());
            }

        } else if(pred instanceof CoreOperationAnd){
            CoreOperationAnd andOperation = (CoreOperationAnd) pred;

            JoinQueryEvalNode newNode = new JoinQueryEvalNode(pred);
            node.add(newNode);
            generateExecutionPlan(newNode, andOperation.getArguments()[0], step);
            generateExecutionPlan(newNode, andOperation.getArguments()[1], step);
        }
    }

    /*
    private void generateExpressionPlan(ExpressionNode queryPredicate, int step) {
        if(queryPredicate.xpathExpression instanceof CoreOperationEqual){
            CoreOperationEqual equalOperation = (CoreOperationEqual) queryPredicate.xpathExpression;
            for(Expression arg : equalOperation.getArguments()){
                if(arg instanceof LocationPath) {
                    final String predicateNode = arg.toString();
                    for(LocationPath index : mIndexes){
                        if(index.getSteps().length >= step) {
                            Step indexStep = index.getSteps()[step];
                            if (predicateNode.equals(indexStep.toString())) {
                                queryPredicate.andPossibleIndexes.add(index);
                            }
                        }
                    }
                }
            }

        } else if(queryPredicate.xpathExpression instanceof CoreOperationAnd){
            CoreOperationAnd andOperation = (CoreOperationAnd) queryPredicate.xpathExpression;
            for(Expression e : andOperation.getArguments()){
                ExpressionNode child = new ExpressionNode();
                child.xpathExpression = e;
                queryPredicate.addChild(child);
                generateExpressionPlan(child, step);
            }

        }
    }
    */
}
