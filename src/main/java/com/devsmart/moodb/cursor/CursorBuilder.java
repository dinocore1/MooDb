package com.devsmart.moodb.cursor;


import com.devsmart.moodb.Index;
import com.devsmart.moodb.MooDB;
import com.devsmart.moodb.MooDBBaseVisitor;
import com.devsmart.moodb.MooDBCursor;
import com.devsmart.moodb.MooDBLexer;
import com.devsmart.moodb.MooDBParser;
import com.devsmart.moodb.Utils;
import com.google.common.base.Joiner;
import com.sleepycat.bind.tuple.SortedDoubleBinding;
import com.sleepycat.je.DatabaseEntry;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.LinkedList;
import java.util.Stack;
import java.util.regex.Pattern;

public class CursorBuilder extends MooDBBaseVisitor<MooDBCursor> {



    public static MooDBCursor query(MooDB db, String query) {
        MooDBLexer lexer = new MooDBLexer(new ANTLRInputStream(query));
        MooDBParser parser = new MooDBParser(new CommonTokenStream(lexer));
        MooDBParser.EvaluationContext tree = parser.evaluation();
        CursorBuilder builder = new CursorBuilder(db);
        return builder.visit(tree);
    }


    private final MooDB mDb;
    private Stack<String> mSteps = new Stack<String>();
    ParseTreeProperty<Object> prop = new ParseTreeProperty<Object>();

    public CursorBuilder(MooDB db) {
        mDb = db;
    }

    private DatabaseEntry guessDataValue(String value) {
        DatabaseEntry retval = null;
        try {
            double numValue = Double.parseDouble(value);
            retval = new DatabaseEntry();
            SortedDoubleBinding.doubleToEntry(numValue, retval);
        } catch (NumberFormatException e) {}

        if(retval == null) {
            retval = new DatabaseEntry(Utils.toBytes(value));
        }

        return retval;
    }



    @Override
    public MooDBCursor visitEvaluation(@NotNull MooDBParser.EvaluationContext ctx) {

        MooDBParser.PredicateContext predicate = ctx.predicate();
        if(predicate != null){
            visit(predicate);
        }

        TerminalNode nodeName = ctx.ID();
        if(nodeName != null){
            mSteps.push(nodeName.getText());
        } else {
            mSteps.push(".");
        }


        return null;
    }

    @Override
    public MooDBCursor visitPredicate(@NotNull MooDBParser.PredicateContext ctx) {
        return visit(ctx.expr());
    }

    @Override
    public MooDBCursor visitEvalExpr(@NotNull MooDBParser.EvalExprContext ctx) {

        visit(ctx.l);
        final String nodeName = (String) prop.get(ctx.l);

        visit(ctx.r);
        final String value = (String) prop.get(ctx.r);

        MooDBCursor retval = null;

        final String op = ctx.o.getText();
        if("=".equals(op)) {
            mSteps.push(nodeName);
            String indexQuery = Joiner.on("/").join(mSteps);
            mSteps.pop();
            Index index = mDb.getIndex(indexQuery);
            if(index != null){
                retval = new IndexEqualCursor(index.getIndexDB().openCursor(null, null), guessDataValue(value));
            }
        }





        return retval;
    }



    @Override
    public MooDBCursor visitExprId(@NotNull MooDBParser.ExprIdContext ctx) {
        prop.put(ctx, ctx.getText());
        return null;
    }

    @Override
    public MooDBCursor visitExprStrLit(@NotNull MooDBParser.ExprStrLitContext ctx) {
        String str = ctx.getText();
        str = str.substring(1, str.length()-1);
        prop.put(ctx, str);
        return null;
    }
}
