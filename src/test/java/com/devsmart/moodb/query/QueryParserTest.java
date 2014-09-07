package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDBLexer;
import com.devsmart.moodb.MooDBParser;
import com.devsmart.moodb.query.parser.QueryBuilder;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;
import static org.junit.Assert.*;

public class QueryParserTest {

    public static class Test1 {
        String type;
    }

    @Test
    public void testParser() {

        String input = "[type='train']";
        MooDBLexer lexer = new MooDBLexer(new ANTLRInputStream(input));
        MooDBParser parser = new MooDBParser(new CommonTokenStream(lexer));
        MooDBParser.EvaluationContext tree = parser.evaluation();
        QueryBuilder queryBuilder = new QueryBuilder();
        queryBuilder.visit(tree);


        Test1 obj = new Test1();
        obj.type = "plane";

        Object returnobj = queryBuilder.query.eval(obj);
        assertNull(returnobj);

        obj = new Test1();
        obj.type = "train";
        returnobj = queryBuilder.query.eval(obj);
        assertNotNull(returnobj);



    }
}
