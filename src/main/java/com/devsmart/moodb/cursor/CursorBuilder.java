package com.devsmart.moodb.cursor;


import com.devsmart.moodb.Index;
import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBBaseVisitor;
import com.devsmart.moodb.MooDBCursor;
import com.devsmart.moodb.MooDBLexer;
import com.devsmart.moodb.MooDBParser;
import com.devsmart.moodb.Utils;
import com.devsmart.moodb.objectquery.Predicate;
import com.devsmart.moodb.objectquery.QueryBuilder;
import com.google.common.base.Joiner;
import com.sleepycat.bind.tuple.SortedDoubleBinding;
import com.sleepycat.je.DatabaseEntry;

import com.sleepycat.je.DiskOrderedCursorConfig;
import com.sleepycat.util.keyrange.RangeCursor;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.LinkedList;
import java.util.Stack;
import java.util.regex.Pattern;

public class CursorBuilder extends MooDBBaseVisitor<Void> {



    public static MooDBCursor query(MooDB db, String query) {
        MooDBLexer lexer = new MooDBLexer(new ANTLRInputStream(query));
        MooDBParser parser = new MooDBParser(new CommonTokenStream(lexer));
        MooDBParser.EvaluationContext tree = parser.evaluation();
        CursorBuilder builder = new CursorBuilder(db);
        builder.visit(tree);

        return (MooDBCursor) builder.prop.get(tree);
    }


    private final MooDB mDb;
    private Stack<String> mSteps = new Stack<String>();
    ParseTreeProperty<Object> prop = new ParseTreeProperty<Object>();

    public CursorBuilder(MooDB db) {
        mDb = db;
    }

    private byte[] guessDataValue(String value) {
        try {
            double numValue = Double.parseDouble(value);
            DatabaseEntry entry = new DatabaseEntry();
            SortedDoubleBinding.doubleToEntry(numValue, entry);
            return entry.getData();
        } catch (NumberFormatException e) {}

        return Utils.toBytes(value);
    }



    @Override
    public Void visitEvaluation(@NotNull MooDBParser.EvaluationContext ctx) {

        MooDBCursor cursor = null;
        MooDBParser.PredicateContext predicate = ctx.predicate();
        if(predicate != null){
            visit(predicate);
            cursor = (MooDBCursor) prop.get(predicate);
        }


        prop.put(ctx, cursor);

        return null;
    }

    @Override
    public Void visitPredicate(@NotNull MooDBParser.PredicateContext ctx) {
        MooDBParser.ExprContext u = ctx.expr();
        visit(u);
        prop.put(ctx, prop.get(u));
        return null;
    }

    @Override
    public Void visitEvalExpr(@NotNull MooDBParser.EvalExprContext ctx) {

        visit(ctx.l);
        final String nodeName = (String) prop.get(ctx.l);

        visit(ctx.r);
        final String value = (String) prop.get(ctx.r);

        mSteps.push(nodeName);
        String indexQuery = Joiner.on("/").join(mSteps);
        mSteps.pop();
        Index index = mDb.getIndex(indexQuery);

        MooDBCursor retval = null;

        final String op = ctx.o.getText();
        if("=".equals(op)) {
            if(index != null){
                retval = new IndexEqualCursor(index.getIndexDB().openCursor(null, null), guessDataValue(value));
            }
        } else if(">=".equals(op) || ">".equals(op)) {
            if(index != null){
                retval = new IndexCursor(index.getIndexDB().openCursor(null, null), guessDataValue(value), IndexCursor.Direction.ASC);
            }
        } else if("<=".equals(op) || "<".equals(op)){
            if(index != null){
                retval = new IndexCursor(index.getIndexDB().openCursor(null, null), guessDataValue(value), IndexCursor.Direction.ASC);
            }
        }

        if(retval == null) {
            retval = new AllObjectsCursor(mDb.openObjectsCursor(null, null));
        }

        prop.put(ctx, retval);
        return null;
    }

    @Override
    public Void visitAndExpr(@NotNull MooDBParser.AndExprContext ctx) {
        visit(ctx.l);
        MooDBCursor cursor = (MooDBCursor)prop.get(ctx.l);

        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.visit(ctx.r);
        Predicate predicate = (Predicate) queryBuilder.prop.get(ctx.r);

        MooDBCursor retval = new PredicateAndCursor(cursor, predicate);
        prop.put(ctx, retval);

        return null;
    }

    @Override
    public Void visitOrExpr(@NotNull MooDBParser.OrExprContext ctx) {
        /*
        visit(ctx.l);
        MooDBCursor cursor = (MooDBCursor)prop.get(ctx.l);

        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.visit(ctx.r);
        Predicate predicate = (Predicate) queryBuilder.prop.get(ctx.r);

        MooDBCursor retval = new PredicateOrCursor(cursor, predicate);
        prop.put(ctx, retval);
        */

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