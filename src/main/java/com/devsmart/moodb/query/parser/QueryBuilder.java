package com.devsmart.moodb.query.parser;

import com.devsmart.moodb.MooDBBaseVisitor;
import com.devsmart.moodb.MooDBParser;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.antlr.v4.runtime.tree.TerminalNode;


public class QueryBuilder extends MooDBBaseVisitor<Void> {

    ParseTreeProperty<Object> prop = new ParseTreeProperty<Object>();
    public Step query;

    @Override
    public Void visitEvaluation(@NotNull MooDBParser.EvaluationContext ctx) {

        ObjectOperation op = null;

        TerminalNode field = ctx.ID();
        if(field != null) {
            op = new ExtractField(field.getText());
        } else {
            op = new NoOp();
        }

        Predicate predicate = null;
        MooDBParser.PredicateContext predicateCtx = ctx.predicate();
        if(predicateCtx != null) {
            visit(predicateCtx);
            predicate = (Predicate) prop.get(predicateCtx);
        }

        query = new Step(op, predicate);

        MooDBParser.StepContext nextStepCtx = ctx.step();
        if(nextStepCtx != null) {
            visit(nextStepCtx);
            query.setNextStep((Step) prop.get(nextStepCtx));
        }

        prop.put(ctx, query);
        return null;
    }

    @Override
    public Void visitStep(@NotNull MooDBParser.StepContext ctx) {
        ObjectOperation op = new ExtractField(ctx.ID().getText());
        Predicate predicate = null;
        MooDBParser.PredicateContext predicateCtx = ctx.predicate();
        if(predicateCtx != null) {
            visit(predicateCtx);
            predicate = (Predicate) prop.get(predicateCtx);
        }

        Step step = new Step(op, predicate);
        MooDBParser.StepContext nextStepCtx = ctx.step();
        if(nextStepCtx != null) {
            visit(nextStepCtx);
            step.setNextStep((Step) prop.get(nextStepCtx));
        }
        prop.put(ctx, step);
        return null;
    }

    @Override
    public Void visitPredicate(@NotNull MooDBParser.PredicateContext ctx) {
        visit(ctx.expr());
        prop.put(ctx, prop.get(ctx.expr()));
        return null;
    }

    @Override
    public Void visitEvalExpr(@NotNull MooDBParser.EvalExprContext ctx) {
        visit(ctx.l);
        String leftStr = (String) prop.get(ctx.l);
        ObjectOperation leftOp = new ExtractField(leftStr);

        visit(ctx.r);
        String rightStr = (String) prop.get(ctx.r);

        Predicate predicate = new ComparatorPredicate(leftOp, rightStr, ctx.o.getText());
        prop.put(ctx, predicate);
        return null;
    }

    @Override
    public Void visitExprId(@NotNull MooDBParser.ExprIdContext ctx) {
        prop.put(ctx, ctx.getText());
        return null;
    }

    @Override
    public Void visitExprStrLit(@NotNull MooDBParser.ExprStrLitContext ctx) {
        String str = ctx.getText();
        str = str.substring(1, str.length()-1);
        prop.put(ctx, str);
        return null;
    }
}
