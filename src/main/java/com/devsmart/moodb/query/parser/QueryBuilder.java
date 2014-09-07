package com.devsmart.moodb.query.parser;

import com.devsmart.moodb.MooDBBaseVisitor;
import com.devsmart.moodb.MooDBParser;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.antlr.v4.runtime.tree.TerminalNode;


public class QueryBuilder extends MooDBBaseVisitor<Void> {

    ParseTreeProperty<Object> prop = new ParseTreeProperty<Object>();
    public ObjectQuery query;

    @Override
    public Void visitEvaluation(@NotNull MooDBParser.EvaluationContext ctx) {

        ExtractField getField = null;
        TerminalNode field = ctx.ID();
        if(field != null) {
            getField = new ExtractField(field.getText());
        }

        Predicate predicate = null;
        MooDBParser.PredicateContext predicateCtx = ctx.predicate();
        if(predicateCtx != null) {
            visit(predicateCtx);
            predicate = (Predicate) prop.get(predicateCtx);
        }

        query = new ObjectQuery(getField, predicate);
        prop.put(ctx, query);
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
        if("=".equals(ctx.o.getText())){
            visit(ctx.l);
            visit(ctx.r);

            prop.put(ctx, new EqualsPredicate(new ExtractField((String)prop.get(ctx.l)), (String)prop.get(ctx.r)));
        }

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
