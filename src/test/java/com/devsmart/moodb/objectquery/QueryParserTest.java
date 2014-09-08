package com.devsmart.moodb.objectquery;


import com.devsmart.moodb.objects.DBElement;
import com.devsmart.moodb.objects.POJODBElement;
import com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class QueryParserTest {



    @Test
    public void testSimplePredicate() {
        ObjectOperation compileExpression = QueryBuilder.compile("[type='train']");

        class Test1 {
            String type;
        }

        Test1 obj = new Test1();
        obj.type = "plane";

        DBElement returnobj = compileExpression.eval(POJODBElement.wrap(obj));
        assertNull(returnobj);

        obj = new Test1();
        obj.type = "train";
        returnobj = compileExpression.eval(POJODBElement.wrap(obj));
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

        DBElement output = compiledExpression.eval(POJODBElement.wrap(list));
        assertNotNull(output);
        assertEquals("type3", output.getAsPrimitive().getAsString());

        compiledExpression = QueryBuilder.compile("[type='type2']");
        output = compiledExpression.eval(POJODBElement.wrap(list));
        assertNotNull(output);
        assertEquals("type2", output.getAsObject().get("type").getAsPrimitive().getAsString());
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
        DBElement output = compiledExpression.eval(POJODBElement.wrap(input));
        assertEquals("oh yea", output.getAsPrimitive().getAsString());
    }

    @Test
    public void testLogicalOrExpression() {
        ObjectOperation compiledExpression = QueryBuilder.compile("type[type='type1' or type='type2']");

        class MyObject {
            String type;
        }

        LinkedList<MyObject> list = new LinkedList<MyObject>();
        for(int i=0;i<10;i++){
            MyObject obj = new MyObject();
            obj.type = String.format("type%d", i);
            list.add(obj);
        }

        DBElement output = compiledExpression.eval(POJODBElement.wrap(list));
        assertNotNull(output);

        assertTrue(output.isCollection());
        ArrayList<DBElement> outputList = Lists.newArrayList(output.getAsCollection());
        assertTrue(outputList.size() == 2);
        assertTrue(outputList.contains(POJODBElement.wrap("type1")));
        assertTrue(outputList.contains(POJODBElement.wrap("type2")));
    }

}
