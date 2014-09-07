package com.devsmart.moodb.query;


import com.devsmart.moodb.MooDBLexer;
import com.devsmart.moodb.MooDBParser;
import com.devsmart.moodb.query.parser.ObjectOperation;
import com.devsmart.moodb.query.parser.QueryBuilder;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import java.util.LinkedList;

import static org.junit.Assert.*;

public class QueryParserTest {



    @Test
    public void testSimplePredicate() {
        ObjectOperation compileExpression = QueryBuilder.compile("[type='train']");

        class Test1 {
            String type;
        }

        Test1 obj = new Test1();
        obj.type = "plane";

        Object returnobj = compileExpression.eval(obj);
        assertNull(returnobj);

        obj = new Test1();
        obj.type = "train";
        returnobj = compileExpression.eval(obj);
        assertNotNull(returnobj);
    }

    @Test
    public void testExtractNode() {
        ObjectOperation compiledExpression = QueryBuilder.compile("type[type='type3']");

        class MyObject {
            String type;
        }

        LinkedList<MyObject> list = new LinkedList<MyObject>();
        for(int i=0;i<10;i++){
            MyObject obj = new MyObject();
            obj.type = String.format("type%d", i);
            list.add(obj);
        }

        Object output = compiledExpression.eval(list);
        assertNotNull(output);
        assertEquals("type3", output);

        compiledExpression = QueryBuilder.compile("[type='type2']");
        output = compiledExpression.eval(list);
        assertNotNull(output);
        assertEquals("type2", ((MyObject)output).type);
    }

    @Test
    public void testExtractNestedNodes() {

        class MyObject {
            String type;
            MyObject nested;
        }

        MyObject input = new MyObject();
        input.type = "type1";
        input.nested = new MyObject();
        input.nested.type = "oh yea";

        ObjectOperation compiledExpression = QueryBuilder.compile("nested[type='type1']/type");
        Object output = compiledExpression.eval(input);
        assertEquals(output, "oh yea");
    }
}
