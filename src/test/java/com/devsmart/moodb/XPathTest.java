package com.devsmart.moodb;


import org.apache.commons.jxpath.CompiledExpression;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.apache.commons.jxpath.ri.Parser;
import org.apache.commons.jxpath.ri.compiler.LocationPath;
import org.apache.commons.jxpath.ri.compiler.Step;
import org.apache.commons.jxpath.ri.compiler.TreeCompiler;
import org.junit.Test;

import java.util.*;

public class XPathTest {

    private Map<String, Object> createWidget(String type, int value) {
        HashMap<String, Object> retval = new HashMap<String, Object>();
        retval.put("type", type);
        retval.put("value", value);
        retval.put("id", UUID.randomUUID());

        return retval;
    }


    @Test
    public void testXPath() {

        ArrayList<Map> db = new ArrayList<Map>();
        for(int i=0;i<5;i++){
            db.add(createWidget("car", i));
        }
        for(int i=0;i<5;i++){
            db.add(createWidget("plane", i));
        }


        JXPathContext ctx = JXPathContext.newContext(db);
        Iterator it = ctx.iterate(".[(type='car' or type='plane') and value='3']");
        int i = 0;
        while(it.hasNext()){
            Object value = it.next();
            System.out.println(String.format("%d: %s", i++, value));
        }

        System.out.println("done");
    }

    @Test
    public void testParseXPath() {
        String xpath = ".[(type='car' or type='plane') and value='3']";
        LocationPath compiledPath = (LocationPath) Parser.parseExpression(xpath, new TreeCompiler());
        Step[] steps = compiledPath.getSteps();
        steps[0].getPredicates();


        System.out.println("done");
    }



}
