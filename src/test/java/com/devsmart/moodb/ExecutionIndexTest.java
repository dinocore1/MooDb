package com.devsmart.moodb;


import com.devsmart.moodb.query.QueryEvalNode;
import org.apache.commons.jxpath.ri.Parser;
import org.apache.commons.jxpath.ri.compiler.LocationPath;
import org.apache.commons.jxpath.ri.compiler.TreeCompiler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Parameterized.class)
public class ExecutionIndexTest {

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParams() {
        ArrayList<Object[]> retval = new ArrayList<Object[]>();

        retval.add(new Object[]{
                ".[type='car' and value='3']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane']"
                },
                new int[][]{ {0,2}, {1} }
        });

        retval.add(new Object[]{
                ".[type='car' or value='3']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane']"
                },
                new int[][]{ {0,2}, {1} }
        });

        retval.add(new Object[]{
                ".[(type='car' and value='3') or id='4']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane']"
                },
                new int[][]{ {-1} }
        });

        retval.add(new Object[]{
                "type/[id='35']",
                new String[]{
                        "type",
                        "value",
                        ".[type='car']/id",
                        ".[type='plane']"
                },
                new int[][]{ {0} }
        });

        return retval;
    }

    private LocationPath mQuery;
    private ArrayList<LocationPath> mAvailableIndexes = new ArrayList<LocationPath>();


    private static final TreeCompiler COMPILER = new TreeCompiler();
    private static LocationPath compileXPath(String xpath){
        return (LocationPath) Parser.parseExpression(xpath, COMPILER);
    }

    public ExecutionIndexTest(String query, String[] availableIndexes, int[][] requiredIndexes) {
        mQuery = compileXPath(query);
        for(String index : availableIndexes){
            mAvailableIndexes.add(compileXPath(index));
        }



    }

    @Test
    public void testCreateExecutionPlan() {
        IndexChooser chooser = new IndexChooser(mQuery, mAvailableIndexes);
        QueryEvalNode executionPlan = chooser.generateExecutionPlan();
        System.out.println(String.format("%s", executionPlan));
    }
}
